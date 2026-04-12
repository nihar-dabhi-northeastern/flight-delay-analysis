package edu.neu.csye7200.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlightRecordSpec extends AnyFlatSpec with Matchers {

  // BTS format: MONTH,DAY_OF_WEEK,FL_DATE,OP_UNIQUE_CARRIER,OP_CARRIER_FL_NUM,
  // ORIGIN,ORIGIN_CITY_NAME,ORIGIN_STATE_ABR,DEST,DEST_CITY_NAME,DEST_STATE_ABR,
  // CRS_DEP_TIME,DEP_TIME,DEP_DELAY,DEP_DEL15,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,
  // ARR_DEL15,CANCELLED,CANCELLATION_CODE,DISTANCE,CARRIER_DELAY,WEATHER_DELAY,
  // NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY

  val validRow: Array[String] = Array(
    "12", "1", "12/2/2024 12:00:00 AM", "AA", "1234",
    "JFK", "New York, NY", "NY",
    "LAX", "Los Angeles, CA", "CA",
    "0800", "0815", "15.00", "1.00",
    "1100", "1125", "25.00", "1.00",
    "0.00", "", "2475.00",
    "0.00", "25.00", "0.00", "0.00", "0.00"
  )

  val cancelledRow: Array[String] = Array(
    "12", "1", "12/2/2024 12:00:00 AM", "DL", "5678",
    "ORD", "Chicago, IL", "IL",
    "ATL", "Atlanta, GA", "GA",
    "0900", "", "", "",
    "1200", "", "", "",
    "1.00", "B", "600.00",
    "", "", "", "", ""
  )

  val severeRow: Array[String] = Array(
    "12", "3", "12/3/2024 12:00:00 AM", "UA", "9999",
    "BOS", "Boston, MA", "MA",
    "SFO", "San Francisco, CA", "CA",
    "0700", "0710", "10.00", "0.00",
    "1000", "1250", "170.00", "1.00",
    "0.00", "", "2704.00",
    "0.00", "170.00", "0.00", "0.00", "0.00"
  )

  // --- Parsing tests ---

  "fromCSV" should "parse a valid row successfully" in {
    FlightRecord.fromCSV(validRow) shouldBe defined
  }

  it should "return None for malformed row" in {
    FlightRecord.fromCSV(Array("bad", "data")) shouldBe None
  }

  it should "parse carrier correctly" in {
    FlightRecord.fromCSV(validRow).map(_.carrier) shouldBe Some("AA")
  }

  it should "parse origin correctly" in {
    FlightRecord.fromCSV(validRow).map(_.origin) shouldBe Some("JFK")
  }

  it should "parse dest correctly" in {
    FlightRecord.fromCSV(validRow).map(_.dest) shouldBe Some("LAX")
  }

  it should "parse arrDelay as Some(25.0)" in {
    FlightRecord.fromCSV(validRow).flatMap(_.arrDelay) shouldBe Some(25.0)
  }

  it should "parse cancelled = false for normal flight" in {
    FlightRecord.fromCSV(validRow).map(_.cancelled) shouldBe Some(false)
  }

  it should "parse cancelled = true for cancelled flight" in {
    FlightRecord.fromCSV(cancelledRow).map(_.cancelled) shouldBe Some(true)
  }

  it should "parse cancellationCode as Some(B)" in {
    FlightRecord.fromCSV(cancelledRow).flatMap(_.cancellationCode) shouldBe Some("B")
  }

  it should "parse depTime as None for cancelled flight" in {
    FlightRecord.fromCSV(cancelledRow).map(_.depTime) shouldBe Some(None)
  }

  it should "parse distance correctly" in {
    FlightRecord.fromCSV(validRow).map(_.distance) shouldBe Some(2475.0)
  }

  it should "parse weatherDelay as Some(25.0)" in {
    FlightRecord.fromCSV(validRow).flatMap(_.weatherDelay) shouldBe Some(25.0)
  }

  it should "parse month correctly" in {
    FlightRecord.fromCSV(validRow).map(_.month) shouldBe Some(12)
  }

  it should "parse dayOfWeek correctly" in {
    FlightRecord.fromCSV(validRow).map(_.dayOfWeek) shouldBe Some(1)
  }

  // --- delayCategory boundary tests ---

  "delayCategory" should "return OnTime for negative delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(-5.0)).delayCategory shouldBe OnTime
  }

  it should "return OnTime for zero delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(0.0)).delayCategory shouldBe OnTime
  }

  it should "return Minor for 1 min delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(1.0)).delayCategory shouldBe Minor
  }

  it should "return Minor for 25 min delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(25.0)).delayCategory shouldBe Minor
  }

  it should "return Minor for exactly 29 min delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(29.0)).delayCategory shouldBe Minor
  }

  it should "return Major for exactly 30 min delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(30.0)).delayCategory shouldBe Major
  }

  it should "return Major for 45 min delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(45.0)).delayCategory shouldBe Major
  }

  it should "return Major for exactly 120 min delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(120.0)).delayCategory shouldBe Major
  }

  it should "return Severe for exactly 121 min delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(121.0)).delayCategory shouldBe Severe
  }

  it should "return Severe for 170 min delay" in {
    FlightRecord.fromCSV(severeRow).get.delayCategory shouldBe Severe
  }

  it should "return OnTime when arrDelay is None" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = None).delayCategory shouldBe OnTime
  }

  // --- isDelayed tests ---

  "isDelayed" should "return true when arrDel15 = 1" in {
    val r = FlightRecord.fromCSV(validRow).get.copy(arrDel15 = Some(1))
    r.isDelayed shouldBe true
  }

  it should "return false when arrDel15 = 0" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDel15 = Some(0)).isDelayed shouldBe false
  }

  // --- totalCauseDelay tests ---

  "totalCauseDelay" should "sum all delay causes correctly" in {
    val r = FlightRecord.fromCSV(validRow).get.copy(
      carrierDelay      = Some(10.0),
      weatherDelay      = Some(15.0),
      nasDelay          = Some(5.0),
      securityDelay     = Some(0.0),
      lateAircraftDelay = Some(0.0)
    )
    r.totalCauseDelay shouldBe 30.0
  }

  it should "return 0.0 when all delays are None" in {
    FlightRecord.fromCSV(cancelledRow).get.totalCauseDelay shouldBe 0.0
  }
}