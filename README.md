# Real-Time Flight Delay Analytics Pipeline

**CSYE7200 — Big Data System Engineering | Prof. Robin Hillyard**  
Team: Nihar Dabhi, [Partner Name]

---

## Project Overview

A real-time flight delay analytics pipeline built with Scala, Apache Spark Structured Streaming, Kafka, Spark MLlib, and a live web dashboard. Ingests BTS On-Time Performance data (~650K records), streams events through Kafka, computes live delay aggregations per carrier and airport, predicts arrival delays using a Random Forest model, and visualizes everything on a FastAPI-powered dashboard.

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Scala 2.13 |
| Stream Processing | Apache Spark 3.5 Structured Streaming |
| Message Broker | Apache Kafka 3.6 (Confluent) |
| Database | PostgreSQL 16 |
| ML | Spark MLlib (Random Forest) |
| Dashboard API | Python FastAPI |
| Frontend | HTML / CSS / Chart.js |
| Containerization | Docker + Docker Compose |
| Build Tool | sbt |
| Testing | ScalaTest |

---

## Project Structure

```
flight-delay-analysis/
├── build.sbt
├── docker-compose.yml
├── run.sh                                # One-command startup script
├── project/
│   └── build.properties
├── src/
│   ├── main/scala/edu/neu/csye7200/
│   │   ├── model/
│   │   │   └── FlightRecord.scala        # Domain model + CSV parser
│   │   ├── producer/
│   │   │   └── FlightProducer.scala      # Kafka producer
│   │   ├── streaming/
│   │   │   └── FlightStreamProcessor.scala # Spark Structured Streaming + PostgreSQL sink
│   │   ├── ml/
│   │   │   └── DelayPredictor.scala      # Random Forest model (R² 0.8886)
│   │   └── batch/
│   │       └── BatchAnalyzer.scala       # Historical Parquet batch analysis
│   └── test/scala/edu/neu/csye7200/
│       └── FlightRecordSpec.scala        # Unit tests
├── DashBoard/
│   ├── api.py                            # FastAPI backend (6 REST endpoints)
│   └── static/
│       └── index.html                    # Live analytics dashboard
├── sql/
│   └── init.sql                          # PostgreSQL schema
├── data/
│   └── flight_data.csv                   # BTS dataset (Dec 2024)
└── model/
    └── random_forest_flight_delay/       # Saved ML model
```

---

## Data Source

- **Bureau of Transportation Statistics (BTS)** Reporting Carrier On-Time Performance
- URL: https://www.transtats.bts.gov
- Coverage: December 2024 (~650K rows)
- Records after cleaning: 584,832

---

## ML Model Results

| Metric | Value |
|--------|-------|
| Algorithm | Random Forest (100 trees) |
| R² Score | 0.8886 |
| MAE | 8.97 minutes |
| RMSE | 20.69 minutes |
| Top Feature | Departure Delay (62.1% importance) |
| Training Records | 584,832 |

---

## Dashboard

A live web dashboard served at `http://localhost:8000` showing:

- **KPIs** — Flights processed, avg arrival delay, carriers tracked, cancellations
- **Carrier Delay Chart** — Top 10 airlines by average arrival delay
- **Delay Root Cause Chart** — Breakdown by carrier, weather, NAS, late aircraft
- **Top 10 Most Delayed Airports** — Ranked table with severity tags
- **Live Flight Delay Predictions** — Real-time ML predictions vs actual delays with accuracy tags

---

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/stats` | Overall KPI stats |
| `GET /api/carrier-delays` | Top 10 carriers by avg delay |
| `GET /api/airport-delays` | Top 10 airports by avg delay |
| `GET /api/delay-causes` | Delay breakdown by cause |
| `GET /api/predictions` | Latest 20 ML predictions |

---

## How to Run

### Prerequisites
- Java 17+
- Scala 2.13
- sbt
- Docker Desktop
- Python 3.x + uvicorn + fastapi + psycopg2-binary

### Option A — One Command (Recommended)
```bash
chmod +x run.sh
./run.sh
```
Then open: **http://localhost:8000**

The script automatically:
1. Starts Docker Desktop
2. Resolves PostgreSQL port conflicts
3. Starts Kafka, Zookeeper, PostgreSQL containers
4. Waits for all services to be ready
5. Creates Kafka topic `flight-events`
6. Recreates streaming tables
7. Starts Spark Streaming Consumer
8. Starts Kafka Producer
9. Starts FastAPI Dashboard

---

### Option B — Manual (4 terminals)

**Terminal 1 — Docker**
```bash
docker-compose up -d
```

**Terminal 2 — Spark Streaming Consumer**
```bash
sbt "runMain edu.neu.csye7200.streaming.FlightStreamProcessor"
```

**Terminal 3 — Kafka Producer**
```bash
sbt "runMain edu.neu.csye7200.producer.FlightProducer data/flight_data.csv"
```

**Terminal 4 — Dashboard**
```bash
uvicorn DashBoard.api:app --reload --port 8000
```

---

### Run ML Model (one-time)
```bash
sbt "runMain edu.neu.csye7200.ml.DelayPredictor"
```

### Run Tests
```bash
sbt test
```