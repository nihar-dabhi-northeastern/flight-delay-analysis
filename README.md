# flight-delay-analysis

# Real-Time Flight Delay Analytics Pipeline
**CSYE7200 — Big Data System Engineering | Prof. Robin Hillyard**
Team: Nihar Dabhi, [Partner Name]

---

## Project Overview
A real-time flight delay analytics pipeline built with Scala, Apache Spark Structured Streaming, Kafka, and Spark MLlib. Ingests BTS On-Time Performance data, computes live delay aggregations per carrier and airport, and predicts arrival delays using a Random Forest model.

---

## Tech Stack
| Component | Technology |
|-----------|------------|
| Language | Scala 2.13 |
| Stream Processing | Apache Spark 3.5 Structured Streaming |
| Message Broker | Apache Kafka 3.6 |
| ML | Spark MLlib (Random Forest) |
| Build Tool | sbt |
| Testing | ScalaTest |

---

## Project Structure
```
flight-delay-analytics/
├── build.sbt
├── project/
│   └── build.properties
├── src/
│   ├── main/scala/edu/neu/csye7200/
│   │   ├── model/
│   │   │   └── FlightRecord.scala        # Domain model + CSV parser
│   │   ├── producer/
│   │   │   └── FlightProducer.scala      # Kafka producer
│   │   ├── streaming/
│   │   │   └── FlightStreamProcessor.scala # Spark Structured Streaming
│   │   └── ml/
│   │       └── DelayPredictor.scala      # Random Forest model
│   └── test/scala/edu/neu/csye7200/
│       └── FlightRecordSpec.scala        # Unit tests
├── data/
│   └── flights_dec2024.csv               # BTS dataset
└── model/
    └── random_forest_flight_delay/       # Saved ML model
```

---

## Data Source
- **Bureau of Transportation Statistics (BTS)** Reporting Carrier On-Time Performance
- URL: https://www.transtats.bts.gov
- Coverage: December 2024 (~650K rows)

---

## How to Run

### Prerequisites
- Java 17+
- Scala 2.13
- sbt
- Docker (for Kafka)

### Step 1 — Start Kafka
```bash
docker-compose up -d
```

### Step 2 — Run Spark Streaming Consumer (first)
```bash
sbt "runMain edu.neu.csye7200.streaming.FlightStreamProcessor"
```

### Step 3 — Run Kafka Producer
```bash
sbt "runMain edu.neu.csye7200.producer.FlightProducer data/flights_dec2024.csv"
```

### Step 4 — Run ML Model
```bash
sbt "runMain edu.neu.csye7200.ml.DelayPredictor data/flights_dec2024.csv"
```

### Step 5 — Run Tests
```bash
sbt test
```

---

## ML Model Results
| Metric | Value |
|--------|-------|
| Model | Random Forest (100 trees) |
| MAE | ~8-11 minutes |
| R² Score | ~0.85-0.90 |
| Top Feature | DepDelay |

---

## Acceptance Criteria
1. Kafka producer publishes BTS records at ~100 events/sec
2. Spark Streaming processes events with under 10 seconds latency
3. Window aggregations compute correct avg delay per carrier and airport
4. Random Forest achieves R² > 0.80 on test set
5. All domain models use Option — no null values
6. Core transformation logic covered by unit tests
