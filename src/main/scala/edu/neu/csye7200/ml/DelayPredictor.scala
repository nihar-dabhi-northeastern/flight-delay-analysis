package edu.neu.csye7200.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.{RandomForestRegressor, RandomForestRegressionModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

object DelayPredictor {

  val PG_URL  = "jdbc:postgresql://localhost:5432/flightdb"
  val PG_USER = "flightuser"
  val PG_PASS = "flightpass"

  def main(args: Array[String]): Unit = {
    val csvPath = if (args.nonEmpty) args(0) else "data/flight_data.csv"

    val spark = SparkSession.builder()
      .appName("FlightDelayPredictor")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Load and clean data
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

    // Encode categorical columns
    val carrierIdx = new StringIndexer().setInputCol("carrier").setOutputCol("carrierIdx").setHandleInvalid("keep")
    val originIdx  = new StringIndexer().setInputCol("origin").setOutputCol("originIdx").setHandleInvalid("keep")
    val destIdx    = new StringIndexer().setInputCol("dest").setOutputCol("destIdx").setHandleInvalid("keep")

    // Feature vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("carrierIdx", "originIdx", "destIdx", "dayOfWeek", "month", "depDelay", "distance", "weatherDelay", "carrierDelay", "nasDelay"))
      .setOutputCol("features")
      .setHandleInvalid("skip")

    // Random Forest
    val rf = new RandomForestRegressor()
      .setLabelCol("arrDelay")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(10)
      .setMaxBins(400)
      .setSeed(42)

    val pipeline = new Pipeline().setStages(Array(carrierIdx, originIdx, destIdx, assembler, rf))

    // Train / test split
    val Array(train, test) = sampled.randomSplit(Array(0.8, 0.2), seed = 42)
    println(s"Training: ${train.count()} | Test: ${test.count()}")

    // Train
    println("Training Random Forest...")
    val model = pipeline.fit(train)

    // Evaluate
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

    // Feature importance
    val rfModel  = model.stages.last.asInstanceOf[RandomForestRegressionModel]
    val featNames = Array("carrier", "origin", "dest", "dayOfWeek", "month", "depDelay", "distance", "weatherDelay", "carrierDelay", "nasDelay")
    val importance = featNames.zip(rfModel.featureImportances.toArray).sortBy(-_._2)

    println("\nFeature Importance:")
    importance.foreach { case (name, imp) => println(f"  $name%-20s $imp%.4f") }

    // Sample predictions
    println("\nSample Predictions:")
    predictions.select("carrier", "origin", "dest", "arrDelay", "prediction").show(10, truncate = false)

    // Save model
    model.write.overwrite().save("model/random_forest_flight_delay")
    println("Model saved to model/random_forest_flight_delay")

    // -------------------------------------------------------------------
    // Save ML results to PostgreSQL
    // -------------------------------------------------------------------
    println("Saving ML results to PostgreSQL...")
    val conn = java.sql.DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)
    try {
      val stmt = conn.createStatement()

      // Create tables if not exist
      stmt.execute("""
        CREATE TABLE IF NOT EXISTS ml_results (
          id SERIAL PRIMARY KEY,
          r2 NUMERIC(6,4), mae NUMERIC(8,2), rmse NUMERIC(8,2),
          total_records BIGINT, top_feature VARCHAR(50),
          created_at TIMESTAMP DEFAULT NOW()
        )
      """)
      stmt.execute("""
        CREATE TABLE IF NOT EXISTS ml_feature_importance (
          feature_name VARCHAR(50) PRIMARY KEY,
          importance NUMERIC(10,6)
        )
      """)

      // Save metrics
      val topFeature = importance.head._1
      stmt.execute(s"INSERT INTO ml_results (r2, mae, rmse, total_records, top_feature) VALUES ($r2, $mae, $rmse, ${df.count()}, '$topFeature')")

      // Save feature importance
      importance.foreach { case (name, imp) =>
        stmt.execute(s"""
          INSERT INTO ml_feature_importance (feature_name, importance)
          VALUES ('$name', $imp)
          ON CONFLICT (feature_name) DO UPDATE SET importance = $imp
        """)
      }
      stmt.close()
      println("✅ ML results saved to PostgreSQL!")
    } finally {
      conn.close()
    }

    spark.stop()
  }
}