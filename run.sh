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
sleep 5

# Step 4 - Verify containers
echo ""
echo "📦 Running containers:"
docker ps --format "  ✅ {{.Names}} ({{.Status}})"

# Step 5 - Clear checkpoints
echo ""
echo "🗑️  Clearing old checkpoints..."
rm -rf checkpoints/
echo "✅ Checkpoints cleared!"

# Step 6 - Recreate DB tables with proper primary keys
echo ""
echo "🗑️  Recreating database tables..."
sleep 3
docker exec -i postgres psql -U flightuser -d flightdb -c "
DROP TABLE IF EXISTS carrier_delay_agg;
DROP TABLE IF EXISTS airport_delay_agg;
DROP TABLE IF EXISTS delay_cause_agg;
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
    avg_late_aircraft_delay NUMERIC(8,2));" 2>/dev/null
echo "✅ Database tables recreated with primary keys!"

# Done
echo ""
echo "============================================"
echo "✅ Setup complete! Now run in 3 terminals:"
echo "============================================"
echo ""
echo "  Terminal 1 — Spark Streaming:"
echo "  sbt \"runMain edu.neu.csye7200.streaming.FlightStreamProcessor\""
echo ""
echo "  Terminal 2 — Kafka Producer:"
echo "  sbt \"runMain edu.neu.csye7200.producer.FlightProducer\""
echo ""
echo "  Terminal 3 — Dashboard API:"
echo "  uvicorn DashBoard.api:app --reload --port 8000"
echo ""
echo "  Browser: http://localhost:8000"
echo ""