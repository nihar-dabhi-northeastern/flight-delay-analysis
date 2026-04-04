package edu.neu.csye7200.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlightRecordSpec extends AnyFlatSpec with Matchers {

  // Valid CSV row
  val validRow: Array[String] = Array(
    "20241201", "12", "7", "AA", "1234",
    "JFK", "New York, NY", "NY",
    "LAX", "Los Angeles, CA", "CA",
    "0800", "0815", "15.0", "1",
    "1100", "1125", "25.0", "1",
    "0", "", "2475.0",
    "0.0", "25.0", "0.0", "0.0", "0.0"
  )

  // Cancelled flight row
  val cancelledRow: Array[String] = Array(
    "20241202", "12", "1", "DL", "5678",
    "ORD", "Chicago, IL", "IL",
    "ATL", "Atlanta, GA", "GA",
    "0900", "", "", "",
    "1200", "", "", "",
    "1", "B", "600.0",
    "", "", "", "", ""
  )

  // Severe delay row
  val severeRow: Array[String] = Array(
    "20241203", "12", "3", "UA", "9999",
    "BOS", "Boston, MA", "MA",
    "SFO", "San Francisco, CA", "CA",
    "0700", "0710", "10.0", "0",
    "1000", "1250", "170.0", "1",
    "0", "", "2704.0",
    "0.0", "170.0", "0.0", "0.0", "0.0"
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

  // --- delayCategory tests ---

  "delayCategory" should "return OnTime for negative delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(-5.0)).delayCategory shouldBe OnTime
  }

  it should "return OnTime for zero delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(0.0)).delayCategory shouldBe OnTime
  }

  it should "return Minor for 25 min delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(25.0)).delayCategory shouldBe Minor
  }

  it should "return Major for 45 min delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(45.0)).delayCategory shouldBe Major
  }

  it should "return Major for exactly 120 min delay" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = Some(120.0)).delayCategory shouldBe Major
  }

  it should "return Severe for 170 min delay" in {
    FlightRecord.fromCSV(severeRow).get.delayCategory shouldBe Severe
  }

  it should "return OnTime when arrDelay is None" in {
    FlightRecord.fromCSV(validRow).get.copy(arrDelay = None).delayCategory shouldBe OnTime
  }

  // --- isDelayed tests ---

  "isDelayed" should "return true when arrDel15 = 1" in {
    FlightRecord.fromCSV(validRow).map(_.isDelayed) shouldBe Some(true)
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