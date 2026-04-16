#!/bin/bash

echo "🚀 Starting Flight Delay Analytics Pipeline"
echo "============================================"

# Step 1 - Restart Docker Desktop cleanly
echo "🐳 Restarting Docker Desktop..."
osascript -e 'quit app "Docker"' 2>/dev/null
sleep 3
open -a Docker
echo "⏳ Waiting for Docker to be ready..."
until docker info > /dev/null 2>&1; do
  sleep 2
  echo "   Still waiting..."
done
echo "✅ Docker is ready!"

# Step 2 - Kill local PostgreSQL if running on port 5432
echo ""
echo "⏳ Checking for local PostgreSQL conflicts on port 5432..."
LOCAL_PG=$(lsof -t -i:5432 2>/dev/null | head -1)
if [ ! -z "$LOCAL_PG" ]; then
  echo "⚠️  Killing local PostgreSQL (PID $LOCAL_PG)..."
  sudo kill -9 $LOCAL_PG
  sleep 2
  echo "✅ Local PostgreSQL stopped!"
else
  echo "✅ No local PostgreSQL conflict!"
fi

# Step 3 - Start Docker containers
echo ""
echo "🐳 Starting Docker containers..."
docker-compose up -d

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
until docker exec postgres pg_isready -U flightuser -d flightdb > /dev/null 2>&1; do
  sleep 2
  echo "   PostgreSQL not ready yet..."
done
echo "✅ PostgreSQL is ready!"

# Step 4 - Verify containers
echo ""
echo "📦 Running containers:"
docker ps --format "  ✅ {{.Names}} ({{.Status}})"

# Step 5 - Wait for Kafka to be fully ready
echo ""
echo "⏳ Waiting for Kafka to be ready..."
until docker exec kafka bash -c "echo > /dev/tcp/localhost/9092" > /dev/null 2>&1; do
  sleep 2
  echo "   Kafka not ready yet..."
done
echo "✅ Kafka is ready!"

# Step 6 - Delete and recreate Kafka topic for fresh start
echo ""
echo "📨 Resetting Kafka topic 'flight-events'..."
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic flight-events 2>/dev/null
sleep 3
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic flight-events \
  --partitions 3 \
  --replication-factor 1
echo "✅ Kafka topic reset — fresh start!"

# Step 7 - Clear checkpoints for fresh start
echo ""
echo "🗑️  Clearing old checkpoints..."
rm -rf checkpoints/
echo "✅ Checkpoints cleared!"

# Step 8 - Recreate ALL tables from scratch
echo ""
echo "🗑️  Recreating all tables from scratch..."
docker exec -i postgres psql -U flightuser -d flightdb -c "
DROP TABLE IF EXISTS carrier_delay_agg;
DROP TABLE IF EXISTS airport_delay_agg;
DROP TABLE IF EXISTS delay_cause_agg;
DROP TABLE IF EXISTS flight_predictions;
CREATE TABLE carrier_delay_agg (
    window_start TIMESTAMP, window_end TIMESTAMP, carrier VARCHAR(10),
    avg_arr_delay NUMERIC(8,2), avg_dep_delay NUMERIC(8,2),
    total_flights BIGINT, cancellations BIGINT,
    avg_weather_delay NUMERIC(8,2), avg_carrier_delay NUMERIC(8,2),
    PRIMARY KEY (window_start, carrier));
CREATE TABLE airport_delay_agg (
    window_start TIMESTAMP, window_end TIMESTAMP, origin VARCHAR(10),
    avg_arr_delay NUMERIC(8,2), total_flights BIGINT, cancellations BIGINT,
    PRIMARY KEY (window_start, origin));
CREATE TABLE delay_cause_agg (
    window_start TIMESTAMP PRIMARY KEY, window_end TIMESTAMP,
    avg_carrier_delay NUMERIC(8,2), avg_weather_delay NUMERIC(8,2),
    avg_nas_delay NUMERIC(8,2), avg_security_delay NUMERIC(8,2),
    avg_late_aircraft_delay NUMERIC(8,2));
CREATE TABLE IF NOT EXISTS ml_results (
    id SERIAL PRIMARY KEY, r2 NUMERIC(6,4), mae NUMERIC(8,2),
    rmse NUMERIC(8,2), total_records BIGINT, top_feature VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW());
CREATE TABLE IF NOT EXISTS ml_feature_importance (
    feature_name VARCHAR(50) PRIMARY KEY, importance NUMERIC(10,6));
CREATE TABLE flight_predictions (
    id SERIAL PRIMARY KEY, carrier VARCHAR(10), flight_number VARCHAR(10),
    origin VARCHAR(10), dest VARCHAR(10), dep_delay NUMERIC(8,2),
    actual_delay NUMERIC(8,2), predicted_delay NUMERIC(8,2),
    processed_at TIMESTAMP DEFAULT NOW());" 2>/dev/null
echo "✅ All tables ready — fresh start!"

# Step 9 - Start Spark Streaming Consumer in background
echo ""
echo "🔥 Starting Spark Streaming Consumer..."
sbt "runMain edu.neu.csye7200.streaming.FlightStreamProcessor" > /dev/null 2>&1 &
SPARK_PID=$!
echo "✅ Spark started (PID $SPARK_PID)"
sleep 15

# Step 10 - Start Kafka Producer in background
echo ""
echo "📨 Starting Kafka Producer..."
sbt "runMain edu.neu.csye7200.producer.FlightProducer data/flight_data.csv" > /dev/null 2>&1 &
PRODUCER_PID=$!
echo "✅ Producer started (PID $PRODUCER_PID)"
sleep 3

# Step 11 - Start FastAPI Dashboard
echo ""
echo "🌐 Starting Dashboard API..."
uvicorn DashBoard.api:app --reload --port 8000 > /dev/null 2>&1 &
API_PID=$!
echo "✅ API started (PID $API_PID)"
sleep 2

echo ""
echo "============================================"
echo "✅ Everything is running!"
echo "============================================"
echo ""
echo "  🌐 Dashboard : http://localhost:8000"
echo ""
echo "  To stop everything:"
echo "  kill $SPARK_PID $PRODUCER_PID $API_PID && docker-compose down"
echo ""