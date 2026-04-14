package edu.neu.csye7200.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.PipelineModel

object FlightStreamProcessor {

  val TOPIC         = "flight-events"
  val BROKER        = "localhost:9092"
  val POSTGRES_URL  = "jdbc:postgresql://localhost:5432/flightdb"
  val POSTGRES_USER = "flightuser"
  val POSTGRES_PASS = "flightpass"
  val MODEL_PATH    = "model/random_forest_flight_delay"

  val schema: StructType = StructType(Seq(
    StructField("flightDate",        StringType,  nullable = true),
    StructField("month",             IntegerType, nullable = true),
    StructField("dayOfWeek",         IntegerType, nullable = true),
    StructField("carrier",           StringType,  nullable = true),
    StructField("flightNumber",      StringType,  nullable = true),
    StructField("origin",            StringType,  nullable = true),
    StructField("originCity",        StringType,  nullable = true),
    StructField("originState",       StringType,  nullable = true),
    StructField("dest",              StringType,  nullable = true),
    StructField("destCity",          StringType,  nullable = true),
    StructField("destState",         StringType,  nullable = true),
    StructField("depDelay",          DoubleType,  nullable = true),
    StructField("arrDelay",          DoubleType,  nullable = true),
    StructField("cancelled",         BooleanType, nullable = true),
    StructField("distance",          DoubleType,  nullable = true),
    StructField("carrierDelay",      DoubleType,  nullable = true),
    StructField("weatherDelay",      DoubleType,  nullable = true),
    StructField("nasDelay",          DoubleType,  nullable = true),
    StructField("securityDelay",     DoubleType,  nullable = true),
    StructField("lateAircraftDelay", DoubleType,  nullable = true)
  ))

  def writeToPostgres(df: org.apache.spark.sql.DataFrame, table: String, conflictCols: String): Unit =
    if (!df.isEmpty) {
      val tmpTable = s"${table}_tmp"
      df.write
        .format("jdbc")
        .option("url",      POSTGRES_URL)
        .option("dbtable",  tmpTable)
        .option("user",     POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver",   "org.postgresql.Driver")
        .mode("overwrite")
        .save()

      val cols = df.columns.mkString(", ")
      val updateCols = df.columns
        .filterNot(conflictCols.split(",").map(_.trim).contains)
        .map(c => s"$c = EXCLUDED.$c")
        .mkString(", ")

      val upsertSQL =
        s"""INSERT INTO $table ($cols)
           |SELECT $cols FROM $tmpTable
           |ON CONFLICT ($conflictCols) DO UPDATE SET $updateCols;
           |DROP TABLE IF EXISTS $tmpTable;""".stripMargin

      val conn = java.sql.DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASS)
      try {
        val stmt = conn.createStatement()
        upsertSQL.split(";").map(_.trim).filter(_.nonEmpty).foreach(stmt.execute)
        stmt.close()
      } finally {
        conn.close()
      }
    }

  def writeAppend(df: org.apache.spark.sql.DataFrame, table: String): Unit =
    if (!df.isEmpty) {
      df.write
        .format("jdbc")
        .option("url",      POSTGRES_URL)
        .option("dbtable",  table)
        .option("user",     POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver",   "org.postgresql.Driver")
        .mode("append")
        .save()
    }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FlightDelayStreamProcessor")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // Load trained ML model if available
    val modelOpt = try {
      val m = PipelineModel.load(MODEL_PATH)
      println("✅ ML model loaded successfully!")
      Some(m)
    } catch {
      case _: Exception =>
        println("⚠️  ML model not found. Run DelayPredictor first for predictions.")
        None
    }

    // Create predictions table if model loaded
    modelOpt.foreach { _ =>
      val conn = java.sql.DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASS)
      try {
        conn.createStatement().execute("""
          CREATE TABLE IF NOT EXISTS flight_predictions (
            id              SERIAL PRIMARY KEY,
            carrier         VARCHAR(10),
            flight_number   VARCHAR(10),
            origin          VARCHAR(10),
            dest            VARCHAR(10),
            dep_delay       NUMERIC(8,2),
            actual_delay    NUMERIC(8,2),
            predicted_delay NUMERIC(8,2),
            processed_at    TIMESTAMP DEFAULT NOW()
          )
        """)
      } finally { conn.close() }
    }

    // Read stream from Kafka
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BROKER)
      .option("subscribe", TOPIC)
      .option("startingOffsets", "earliest")
      .load()

    // Parse JSON and add processing time
    val flightStream = rawStream
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")
      .withColumn("processingTime", current_timestamp())

    // -------------------------------------------------------------------
    // Aggregation 1: Avg delay per carrier — 5 min tumbling window
    // -------------------------------------------------------------------
    val carrierAgg = flightStream
      .groupBy(window(col("processingTime"), "5 minutes"), col("carrier"))
      .agg(
        round(avg("arrDelay"), 2).as("avg_arr_delay"),
        round(avg("depDelay"), 2).as("avg_dep_delay"),
        count("*").as("total_flights"),
        sum(when(col("cancelled"), 1).otherwise(0)).as("cancellations"),
        round(avg("weatherDelay"), 2).as("avg_weather_delay"),
        round(avg("carrierDelay"), 2).as("avg_carrier_delay")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("carrier"),
        col("avg_arr_delay"),
        col("avg_dep_delay"),
        col("total_flights"),
        col("cancellations"),
        col("avg_weather_delay"),
        col("avg_carrier_delay")
      )

    // -------------------------------------------------------------------
    // Aggregation 2: Avg delay per airport — 15 min sliding window
    // -------------------------------------------------------------------
    val airportAgg = flightStream
      .groupBy(window(col("processingTime"), "15 minutes", "5 minutes"), col("origin"))
      .agg(
        round(avg("arrDelay"), 2).as("avg_arr_delay"),
        count("*").as("total_flights"),
        sum(when(col("cancelled"), 1).otherwise(0)).as("cancellations")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("origin"),
        col("avg_arr_delay"),
        col("total_flights"),
        col("cancellations")
      )

    // -------------------------------------------------------------------
    // Aggregation 3: Delay cause breakdown — 5 min tumbling window
    // -------------------------------------------------------------------
    val causeAgg = flightStream
      .groupBy(window(col("processingTime"), "5 minutes"))
      .agg(
        round(avg("carrierDelay"), 2).as("avg_carrier_delay"),
        round(avg("weatherDelay"), 2).as("avg_weather_delay"),
        round(avg("nasDelay"), 2).as("avg_nas_delay"),
        round(avg("securityDelay"), 2).as("avg_security_delay"),
        round(avg("lateAircraftDelay"), 2).as("avg_late_aircraft_delay")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("avg_carrier_delay"),
        col("avg_weather_delay"),
        col("avg_nas_delay"),
        col("avg_security_delay"),
        col("avg_late_aircraft_delay")
      )

    // -------------------------------------------------------------------
    // Write aggregations to PostgreSQL
    // -------------------------------------------------------------------
    val q1 = carrierAgg.writeStream
      .outputMode("update")
      .foreachBatch { (df: org.apache.spark.sql.DataFrame, id: Long) =>
        println(s"[Carrier Agg] Batch $id")
        df.show(5, truncate = false)
        writeToPostgres(df, "carrier_delay_agg", "window_start, carrier")
      }
      .option("checkpointLocation", "checkpoints/carrier")
      .queryName("carrier_agg")
      .start()

    val q2 = airportAgg.writeStream
      .outputMode("update")
      .foreachBatch { (df: org.apache.spark.sql.DataFrame, id: Long) =>
        println(s"[Airport Agg] Batch $id")
        df.show(5, truncate = false)
        writeToPostgres(df, "airport_delay_agg", "window_start, origin")
      }
      .option("checkpointLocation", "checkpoints/airport")
      .queryName("airport_agg")
      .start()

    val q3 = causeAgg.writeStream
      .outputMode("update")
      .foreachBatch { (df: org.apache.spark.sql.DataFrame, id: Long) =>
        println(s"[Cause Agg] Batch $id")
        df.show(3, truncate = false)
        writeToPostgres(df, "delay_cause_agg", "window_start")
      }
      .option("checkpointLocation", "checkpoints/delay_cause")
      .queryName("cause_agg")
      .start()

    // -------------------------------------------------------------------
    // Real-time ML predictions
    // -------------------------------------------------------------------
    val q4 = modelOpt.map { mlModel =>
      flightStream
        .na.fill(0.0, Seq("depDelay", "weatherDelay", "carrierDelay", "nasDelay"))
        .writeStream
        .outputMode("append")
        .foreachBatch { (df: org.apache.spark.sql.DataFrame, id: Long) =>
          if (!df.isEmpty) {
            val predicted = mlModel.transform(df)
              .select(
                col("carrier"),
                col("flightNumber").as("flight_number"),
                col("origin"),
                col("dest"),
                round(col("depDelay"), 2).as("dep_delay"),
                round(col("arrDelay"), 2).as("actual_delay"),
                round(col("prediction"), 2).as("predicted_delay")
              )
              .limit(50) // save top 50 predictions per batch
            println(s"[ML Predictions] Batch $id — ${predicted.count()} predictions")
            predicted.show(5, truncate = false)
            writeAppend(predicted, "flight_predictions")
          }
        }
        .option("checkpointLocation", "checkpoints/predictions")
        .queryName("ml_predictions")
        .start()
    }

    println("Streaming started — writing to console + PostgreSQL...")
    if (q4.isDefined) println("✅ Real-time ML predictions active!")
    else println("⚠️  ML predictions disabled — run DelayPredictor first")

    spark.streams.awaitAnyTermination()
  }
}