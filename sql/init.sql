-- Auto-runs when PostgreSQL container starts for the first time

CREATE TABLE IF NOT EXISTS carrier_delay_agg (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    carrier             VARCHAR(10),
    avg_arr_delay       NUMERIC(8,2),
    avg_dep_delay       NUMERIC(8,2),
    total_flights       BIGINT,
    cancellations       BIGINT,
    avg_weather_delay   NUMERIC(8,2),
    avg_carrier_delay   NUMERIC(8,2),
    PRIMARY KEY (window_start, carrier)
);

CREATE TABLE IF NOT EXISTS airport_delay_agg (
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    origin          VARCHAR(10),
    avg_arr_delay   NUMERIC(8,2),
    total_flights   BIGINT,
    cancellations   BIGINT,
    PRIMARY KEY (window_start, origin)
);

CREATE TABLE IF NOT EXISTS delay_cause_agg (
    window_start            TIMESTAMP PRIMARY KEY,
    window_end              TIMESTAMP,
    avg_carrier_delay       NUMERIC(8,2),
    avg_weather_delay       NUMERIC(8,2),
    avg_nas_delay           NUMERIC(8,2),
    avg_security_delay      NUMERIC(8,2),
    avg_late_aircraft_delay NUMERIC(8,2)
);