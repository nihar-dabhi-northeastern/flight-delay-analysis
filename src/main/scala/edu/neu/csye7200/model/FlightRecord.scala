package edu.neu.csye7200.model

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try

// Delay category ADT
sealed trait DelayCategory
case object OnTime extends DelayCategory  // <= 0 min
case object Minor  extends DelayCategory  // 1 - 29 min
case object Major  extends DelayCategory  // 30 - 120 min
case object Severe extends DelayCategory  // > 120 min

// Flight domain model - Option used for all nullable fields
case class FlightRecord(
                         flightDate:        LocalDate,
                         month:             Int,
                         dayOfWeek:         Int,
                         carrier:           String,
                         flightNumber:      String,
                         origin:            String,
                         originCity:        String,
                         originState:       String,
                         dest:              String,
                         destCity:          String,
                         destState:         String,
                         crsDepTime:        Int,
                         depTime:           Option[Int],
                         depDelay:          Option[Double],
                         depDel15:          Option[Int],
                         crsArrTime:        Int,
                         arrTime:           Option[Int],
                         arrDelay:          Option[Double],
                         arrDel15:          Option[Int],
                         cancelled:         Boolean,
                         cancellationCode:  Option[String],
                         distance:          Double,
                         carrierDelay:      Option[Double],
                         weatherDelay:      Option[Double],
                         nasDelay:          Option[Double],
                         securityDelay:     Option[Double],
                         lateAircraftDelay: Option[Double]
                       ) {
  // Categorize delay using pattern matching
  def delayCategory: DelayCategory = arrDelay match {
    case None                 => OnTime
    case Some(d) if d <= 0   => OnTime
    case Some(d) if d < 30   => Minor
    case Some(d) if d <= 120 => Major
    case _                    => Severe
  }

  // True if flight was delayed 15+ minutes
  def isDelayed: Boolean = arrDel15.contains(1)

  // Sum of all delay causes
  def totalCauseDelay: Double =
    carrierDelay.getOrElse(0.0) +
      weatherDelay.getOrElse(0.0) +
      nasDelay.getOrElse(0.0) +
      securityDelay.getOrElse(0.0) +
      lateAircraftDelay.getOrElse(0.0)
}

object FlightRecord {
  private val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")

  private def toDouble(s: String): Option[Double] =
    s.trim match {
      case "" => None
      case v  => Try(v.toDouble).toOption
    }

  private def toInt(s: String): Option[Int] =
    s.trim match {
      case "" => None
      case v  => Try(v.toInt).toOption
    }

  // Parse one CSV row into FlightRecord — returns None if row is malformed
  def fromCSV(cols: Array[String]): Option[FlightRecord] =
    Try {
      FlightRecord(
        flightDate        = LocalDate.parse(cols(0).trim, fmt),
        month             = cols(1).trim.toInt,
        dayOfWeek         = cols(2).trim.toInt,
        carrier           = cols(3).trim,
        flightNumber      = cols(4).trim,
        origin            = cols(5).trim,
        originCity        = cols(6).trim,
        originState       = cols(7).trim,
        dest              = cols(8).trim,
        destCity          = cols(9).trim,
        destState         = cols(10).trim,
        crsDepTime        = cols(11).trim.toInt,
        depTime           = toInt(cols(12)),
        depDelay          = toDouble(cols(13)),
        depDel15          = toInt(cols(14)),
        crsArrTime        = cols(15).trim.toInt,
        arrTime           = toInt(cols(16)),
        arrDelay          = toDouble(cols(17)),
        arrDel15          = toInt(cols(18)),
        cancelled         = cols(19).trim == "1",
        cancellationCode  = cols(20).trim match { case "" => None; case c => Some(c) },
        distance          = cols(21).trim.toDouble,
        carrierDelay      = toDouble(cols(22)),
        weatherDelay      = toDouble(cols(23)),
        nasDelay          = toDouble(cols(24)),
        securityDelay     = toDouble(cols(25)),
        lateAircraftDelay = toDouble(cols(26))
      )
    }.toOption
}