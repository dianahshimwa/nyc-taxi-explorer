import sqlite3

SCHEMA = """
-- ================================================
-- NYC TAXI EXPLORER - DATABASE SCHEMA
-- ================================================

-- Drop existing tables if starting fresh
DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS zones;

-- ================================================
-- TABLE 1: zones
-- Stores all 265 NYC taxi zone reference data
-- This is our dimension table - it barely changes
-- ================================================
CREATE TABLE zones (
    LocationID   INTEGER  NOT NULL,
    Borough      TEXT     NOT NULL,
    Zone         TEXT     NOT NULL,
    service_zone TEXT,

    CONSTRAINT pk_zones PRIMARY KEY (LocationID)
);

-- ================================================
-- TABLE 2: trips
-- Stores every cleaned taxi trip record
-- This is our fact table - it grows constantly
-- ================================================
CREATE TABLE trips (
    trip_id          INTEGER  NOT NULL,
    pickup_datetime  TEXT     NOT NULL,
    dropoff_datetime TEXT     NOT NULL,
    passenger_count  INTEGER,
    trip_distance    REAL     NOT NULL,
    PULocationID     INTEGER  NOT NULL,
    DOLocationID     INTEGER  NOT NULL,
    payment_type     INTEGER,
    payment_label    TEXT,
    fare_amount      REAL     NOT NULL,
    tip_amount       REAL,
    total_amount     REAL     NOT NULL,
    duration_minutes REAL     NOT NULL,
    speed_mph        REAL,
    revenue_per_mile REAL,
    is_rush_hour     INTEGER  DEFAULT 0,
    pickup_hour      INTEGER,
    pickup_day_num   INTEGER,
    pickup_borough   TEXT,
    pickup_zone      TEXT,
    dropoff_borough  TEXT,
    dropoff_zone     TEXT,

    CONSTRAINT pk_trips
        PRIMARY KEY (trip_id AUTOINCREMENT),

    CONSTRAINT fk_pickup
        FOREIGN KEY (PULocationID)
        REFERENCES zones (LocationID),

    CONSTRAINT fk_dropoff
        FOREIGN KEY (DOLocationID)
        REFERENCES zones (LocationID),

    CONSTRAINT chk_distance
        CHECK (trip_distance > 0),

    CONSTRAINT chk_fare
        CHECK (fare_amount > 0),

    CONSTRAINT chk_duration
        CHECK (duration_minutes > 0),

    CONSTRAINT chk_rush_hour
        CHECK (is_rush_hour IN (0, 1))
);

-- ================================================
-- INDEXES
-- These make queries faster on large datasets
-- ================================================

CREATE INDEX idx_pickup_zone
    ON trips (PULocationID);

CREATE INDEX idx_dropoff_zone
    ON trips (DOLocationID);

CREATE INDEX idx_pickup_datetime
    ON trips (pickup_datetime);

CREATE INDEX idx_pickup_hour
    ON trips (pickup_hour);

CREATE INDEX idx_rush_hour
    ON trips (is_rush_hour);

CREATE INDEX idx_total_amount
    ON trips (total_amount);
"""

def init_database(db_path='nyc_taxi.db'):
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_database()
