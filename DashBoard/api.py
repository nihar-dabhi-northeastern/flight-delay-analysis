from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import psycopg2
import psycopg2.extras

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="DashBoard/static"), name="static")

DB = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "flightdb",
    "user":     "flightuser",
    "password": "flightpass"
}

def get_conn():
    return psycopg2.connect(**DB)

def query(sql):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]

@app.get("/")
def index():
    return FileResponse("DashBoard/static/index.html")

@app.get("/api/stats")
def stats():
    rows = query("""
        SELECT
            ROUND(AVG(avg_arr_delay)::numeric, 2)  as overall_avg_delay,
            SUM(total_flights)                      as total_flights,
            SUM(cancellations)                      as total_cancellations,
            COUNT(DISTINCT carrier)                 as carriers_tracked
        FROM carrier_delay_agg
    """)
    r = rows[0] if rows else {}
    return {
        "overall_avg_delay":   float(r.get("overall_avg_delay",   0) or 0),
        "total_flights":       int(r.get("total_flights",          0) or 0),
        "total_cancellations": int(r.get("total_cancellations",    0) or 0),
        "carriers_tracked":    int(r.get("carriers_tracked",       0) or 0),
    }

@app.get("/api/carrier-delays")
def carrier_delays():
    return query("""
        SELECT carrier,
               ROUND(AVG(avg_arr_delay)::numeric, 2) as avg_delay,
               SUM(total_flights) as total_flights,
               SUM(cancellations) as cancellations
        FROM carrier_delay_agg
        GROUP BY carrier
        ORDER BY avg_delay DESC
        LIMIT 10
    """)

@app.get("/api/airport-delays")
def airport_delays():
    return query("""
        SELECT origin,
               ROUND(AVG(avg_arr_delay)::numeric, 2) as avg_delay,
               SUM(total_flights) as total_flights
        FROM airport_delay_agg
        GROUP BY origin
        ORDER BY avg_delay DESC
        LIMIT 10
    """)

@app.get("/api/delay-causes")
def delay_causes():
    rows = query("""
        SELECT
            ROUND(AVG(avg_carrier_delay)::numeric, 2)       as carrier,
            ROUND(AVG(avg_weather_delay)::numeric, 2)       as weather,
            ROUND(AVG(avg_nas_delay)::numeric, 2)           as nas,
            ROUND(AVG(avg_late_aircraft_delay)::numeric, 2) as late_aircraft
        FROM delay_cause_agg
    """)
    r = rows[0] if rows else {}
    return [
        {"cause": "Carrier",       "minutes": float(r.get("carrier",       0) or 0)},
        {"cause": "Weather",       "minutes": float(r.get("weather",       0) or 0)},
        {"cause": "NAS",           "minutes": float(r.get("nas",           0) or 0)},
        {"cause": "Late Aircraft", "minutes": float(r.get("late_aircraft", 0) or 0)},
    ]

@app.get("/api/predictions")
def predictions():
    try:
        return query("""
            SELECT carrier, flight_number, origin, dest,
                   dep_delay, actual_delay, predicted_delay,
                   processed_at
            FROM flight_predictions
            ORDER BY processed_at DESC
            LIMIT 20
        """)
    except:
        return []