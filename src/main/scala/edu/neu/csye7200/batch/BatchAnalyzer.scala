package edu.neu.csye7200.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BatchAnalyzer {

  def main(args: Array[String]): Unit = {
    val csvPath = if (args.nonEmpty) args(0) else "data/flight_data.csv"

    val spark = SparkSession.builder()
      .appName("FlightDelayBatchAnalyzer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // -------------------------------------------------------------------
    // Load CSV
    // -------------------------------------------------------------------
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    println(s"Total records loaded: ${df.count()}")

    // -------------------------------------------------------------------
    // Write as Parquet partitioned by year / month / carrier
    // -------------------------------------------------------------------
    df.withColumn("year",  lit(2024))
      .withColumn("month", col("Month"))
      .write
      .partitionBy("year", "month", "Reporting_Airline")
      .mode("overwrite")
      .parquet("output/parquet/")

    println("Parquet files written to output/parquet/")

    // Register as temp view for Spark SQL
    val parquet = spark.read.parquet("output/parquet/")
    parquet.createOrReplaceTempView("flights")

    // -------------------------------------------------------------------
    // Query 1: Top 10 most delayed carriers
    // -------------------------------------------------------------------
    println("\n=== Top 10 Most Delayed Carriers ===")
    spark.sql(
      """
        SELECT Reporting_Airline as carrier,
               ROUND(AVG(ArrDelay), 2) as avg_arr_delay,
               COUNT(*) as total_flights,
               SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) as cancellations
        FROM flights
        WHERE ArrDelay IS NOT NULL
        GROUP BY Reporting_Airline
        ORDER BY avg_arr_delay DESC
        LIMIT 10
      """
    ).show(truncate = false)

    // -------------------------------------------------------------------
    // Query 2: Top 10 most delayed airports
    // -------------------------------------------------------------------
    println("\n=== Top 10 Most Delayed Origin Airports ===")
    spark.sql(
      """
        SELECT Origin as airport,
               ROUND(AVG(ArrDelay), 2) as avg_arr_delay,
               COUNT(*) as total_flights,
               SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) as cancellations
        FROM flights
        WHERE ArrDelay IS NOT NULL
        GROUP BY Origin
        ORDER BY avg_arr_delay DESC
        LIMIT 10
      """
    ).show(truncate = false)

    // -------------------------------------------------------------------
    // Query 3: Delay cause breakdown
    // -------------------------------------------------------------------
    println("\n=== Delay Cause Breakdown (avg minutes) ===")
    spark.sql(
      """
        SELECT ROUND(AVG(CarrierDelay), 2)      as avg_carrier_delay,
               ROUND(AVG(WeatherDelay), 2)      as avg_weather_delay,
               ROUND(AVG(NASDelay), 2)          as avg_nas_delay,
               ROUND(AVG(SecurityDelay), 2)     as avg_security_delay,
               ROUND(AVG(LateAircraftDelay), 2) as avg_late_aircraft_delay
        FROM flights
        WHERE ArrDelay > 0
      """
    ).show(truncate = false)

    // -------------------------------------------------------------------
    // Query 4: Delay by day of week
    // -------------------------------------------------------------------
    println("\n=== Avg Delay by Day of Week ===")
    spark.sql(
      """
        SELECT DayOfWeek as day_of_week,
               ROUND(AVG(ArrDelay), 2) as avg_arr_delay,
               COUNT(*) as total_flights
        FROM flights
        WHERE ArrDelay IS NOT NULL
        GROUP BY DayOfWeek
        ORDER BY DayOfWeek
      """
    ).show(truncate = false)

    spark.stop()
    println("Batch analysis complete!")
  }
}