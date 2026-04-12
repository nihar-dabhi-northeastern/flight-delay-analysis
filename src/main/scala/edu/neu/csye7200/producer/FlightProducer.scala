package edu.neu.csye7200.producer

import edu.neu.csye7200.model.FlightRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import scala.io.Source
import scala.util.Using

object FlightProducer {

  val TOPIC    = "flight-events"
  val BROKER   = "localhost:9092"
  val DELAY_MS = 10L  // 10ms between messages = ~100 msg/sec

  // Kafka producer config
  private def kafkaProps: Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      BROKER)
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   classOf[StringSerializer].getName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.ACKS_CONFIG,                   "1")
    p.put(ProducerConfig.RETRIES_CONFIG,                "3")
    p
  }

  // Convert FlightRecord to JSON string
  def toJson(f: FlightRecord): String =
    s"""{
       |  "flightDate": "${f.flightDate}",
       |  "month": ${f.month},
       |  "dayOfWeek": ${f.dayOfWeek},
       |  "carrier": "${f.carrier}",
       |  "flightNumber": "${f.flightNumber}",
       |  "origin": "${f.origin}",
       |  "originCity": "${f.originCity}",
       |  "originState": "${f.originState}",
       |  "dest": "${f.dest}",
       |  "destCity": "${f.destCity}",
       |  "destState": "${f.destState}",
       |  "depDelay": ${f.depDelay.getOrElse(0.0)},
       |  "arrDelay": ${f.arrDelay.getOrElse(0.0)},
       |  "cancelled": ${f.cancelled},
       |  "distance": ${f.distance},
       |  "carrierDelay": ${f.carrierDelay.getOrElse(0.0)},
       |  "weatherDelay": ${f.weatherDelay.getOrElse(0.0)},
       |  "nasDelay": ${f.nasDelay.getOrElse(0.0)},
       |  "securityDelay": ${f.securityDelay.getOrElse(0.0)},
       |  "lateAircraftDelay": ${f.lateAircraftDelay.getOrElse(0.0)}
       |}""".stripMargin

  def main(args: Array[String]): Unit = {
    val csvPath = if (args.nonEmpty) args(0) else "data/flight_data.csv"

    println(s"Starting Flight Producer")
    println(s"Reading from: $csvPath")
    println(s"Publishing to Kafka topic: $TOPIC")

    val producer = new KafkaProducer[String, String](kafkaProps)
    var count    = 0
    var skipped  = 0

    try {
      Using(Source.fromFile(csvPath)) { src =>
        val lines = src.getLines()
        if (lines.hasNext) lines.next() // skip header

        for (line <- lines) {
          val cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
            .map(_.replaceAll("\"", "").trim)
          FlightRecord.fromCSV(cols) match {
            case Some(flight) =>
              val key    = s"${flight.carrier}-${flight.flightNumber}-${flight.flightDate}"
              val value  = toJson(flight)
              val record = new ProducerRecord[String, String](TOPIC, key, value)
              producer.send(record, (_, ex) => if (ex != null) println(s"Error: ${ex.getMessage}"))
              count += 1
              if (count % 10000 == 0) println(s"Published $count records...")
              Thread.sleep(DELAY_MS)
            case None =>
              skipped += 1
          }
        }
      }
    } finally {
      producer.flush()
      producer.close()
      println(s"Done! Published: $count records | Skipped: $skipped malformed rows")
    }
  }
}