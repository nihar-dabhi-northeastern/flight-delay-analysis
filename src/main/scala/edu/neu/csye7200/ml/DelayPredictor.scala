package edu.neu.csye7200.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.{RandomForestRegressor, RandomForestRegressionModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

object DelayPredictor {

  def main(args: Array[String]): Unit = {
    val csvPath = if (args.nonEmpty) args(0) else "data/flight_data.csv"

    val spark = SparkSession.builder()
      .appName("FlightDelayPredictor")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // -------------------------------------------------------------------
    // Load and clean data
    // -------------------------------------------------------------------
    val raw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    val df = raw.select(
        col("OP_UNIQUE_CARRIER").as("carrier"),
        col("ORIGIN").as("origin"),
        col("DEST").as("dest"),
        col("DAY_OF_WEEK").cast("double").as("dayOfWeek"),
        col("MONTH").cast("double").as("month"),
        col("DEP_DELAY").cast("double").as("depDelay"),
        col("DISTANCE").cast("double").as("distance"),
        col("WEATHER_DELAY").cast("double").as("weatherDelay"),
        col("CARRIER_DELAY").cast("double").as("carrierDelay"),
        col("NAS_DELAY").cast("double").as("nasDelay"),
        col("ARR_DELAY").cast("double").as("arrDelay")
      )
      .na.fill(0.0, Seq("depDelay", "weatherDelay", "carrierDelay", "nasDelay"))
      .na.drop(Seq("arrDelay"))
    val sampled = df.sample(0.2, seed = 42)


    println(s"Records after cleaning: ${df.count()}")

    // -------------------------------------------------------------------
    // Encode categorical columns
    // -------------------------------------------------------------------
    val carrierIdx = new StringIndexer().setInputCol("carrier").setOutputCol("carrierIdx").setHandleInvalid("keep")
    val originIdx  = new StringIndexer().setInputCol("origin").setOutputCol("originIdx").setHandleInvalid("keep")
    val destIdx    = new StringIndexer().setInputCol("dest").setOutputCol("destIdx").setHandleInvalid("keep")

    // -------------------------------------------------------------------
    // Assemble feature vector
    // -------------------------------------------------------------------
    val assembler = new VectorAssembler()
      .setInputCols(Array("carrierIdx", "originIdx", "destIdx", "dayOfWeek", "month", "depDelay", "distance", "weatherDelay", "carrierDelay", "nasDelay"))
      .setOutputCol("features")
      .setHandleInvalid("skip")

    // -------------------------------------------------------------------
    // Random Forest model
    // -------------------------------------------------------------------
    val rf = new RandomForestRegressor()
      .setLabelCol("arrDelay")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(10)
      .setMaxBins(400)
      .setSeed(42)

    // -------------------------------------------------------------------
    // Pipeline
    // -------------------------------------------------------------------
    val pipeline = new Pipeline().setStages(Array(carrierIdx, originIdx, destIdx, assembler, rf))

    // -------------------------------------------------------------------
    // Train / test split
    // -------------------------------------------------------------------
    val Array(train, test) = sampled.randomSplit(Array(0.8, 0.2), seed = 42)
    println(s"Training: ${train.count()} | Test: ${test.count()}")

    // -------------------------------------------------------------------
    // Train
    // -------------------------------------------------------------------
    println("Training Random Forest...")
    val model = pipeline.fit(train)

    // -------------------------------------------------------------------
    // Evaluate
    // -------------------------------------------------------------------
    val predictions = model.transform(test)

    def eval(metric: String) = new RegressionEvaluator()
      .setLabelCol("arrDelay")
      .setPredictionCol("prediction")
      .setMetricName(metric)
      .evaluate(predictions)

    val mae  = eval("mae")
    val rmse = eval("rmse")
    val r2   = eval("r2")

    println("=" * 45)
    println("Random Forest Results:")
    println(f"  MAE  : $mae%.2f minutes")
    println(f"  RMSE : $rmse%.2f minutes")
    println(f"  R²   : $r2%.4f")
    println("=" * 45)

    // -------------------------------------------------------------------
    // Feature importance
    // -------------------------------------------------------------------
    val rfModel = model.stages.last.asInstanceOf[RandomForestRegressionModel]
    val features = Array("carrier", "origin", "dest", "dayOfWeek", "month", "depDelay", "distance", "weatherDelay", "carrierDelay", "nasDelay")

    println("\nFeature Importance:")
    features.zip(rfModel.featureImportances.toArray)
      .sortBy(-_._2)
      .foreach { case (name, imp) => println(f"  $name%-20s $imp%.4f") }

    // -------------------------------------------------------------------
    // Sample predictions
    // -------------------------------------------------------------------
    println("\nSample Predictions:")
    predictions.select("carrier", "origin", "dest", "arrDelay", "prediction").show(10, truncate = false)

    // -------------------------------------------------------------------
    // Save model
    // -------------------------------------------------------------------
    model.write.overwrite().save("model/random_forest_flight_delay")
    println("Model saved to model/random_forest_flight_delay")

    spark.stop()
  }
}