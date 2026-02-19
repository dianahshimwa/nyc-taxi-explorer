import sqlite3
import csv

DB_PATH = 'nyc_taxi.db'

# Database design and triple quotations to make it neat

SCHEMA = """   
DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS zones;

CREATE TABLE zones (
    LocationID   INTEGER  NOT NULL,
    Borough      TEXT     NOT NULL,
    Zone         TEXT     NOT NULL,
    service_zone TEXT,

    CONSTRAINT pk_zones PRIMARY KEY (LocationID)
);

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

def init_database():
    print("Creating tables...")
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    print("Tables created.")

def load_zones():
    print("Loading zones...")
    conn = sqlite3.connect(DB_PATH)
    with open('../data/taxi_zone_lookup.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            conn.execute('INSERT OR IGNORE INTO zones VALUES (?,?,?,?)', 
                         (row['LocationID'], row['Borough'], row['Zone'], row.get('service_zone', '')))
    conn.commit()
    conn.close()
    print("Zones loaded.")

def load_trips():
    print("Loading trips (this will take a few minutes)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    batch = []
    count = 0

    with open('processed_trips.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            batch.append((
                row['pickup_datetime'], row['dropoff_datetime'], row['passenger_count'],
                row['trip_distance'], row['PULocationID'], row['DOLocationID'],
                row['payment_type'], row['payment_label'], row['fare_amount'],
                row['tip_amount'], row['total_amount'], row['duration_minutes'],
                row['speed_mph'], row['revenue_per_mile'], row['is_rush_hour'],
                row['pickup_hour'], row['pickup_day_num'], row['pickup_borough'],
                row['pickup_zone'], row['dropoff_borough'], row['dropoff_zone']
            ))
            if len(batch) == 10000:
                conn.executemany('''INSERT INTO trips
                    (pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
                     PULocationID, DOLocationID, payment_type, payment_label, fare_amount,
                     tip_amount, total_amount, duration_minutes, speed_mph, revenue_per_mile,
                     is_rush_hour, pickup_hour, pickup_day_num, pickup_borough, pickup_zone,
                     dropoff_borough, dropoff_zone)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', batch)
                conn.commit()
                count += len(batch)
                batch = []
                print(f"  {count:,} rows inserted...")

    if batch:
        conn.executemany('''INSERT INTO trips
            (pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
             PULocationID, DOLocationID, payment_type, payment_label, fare_amount,
             tip_amount, total_amount, duration_minutes, speed_mph, revenue_per_mile,
             is_rush_hour, pickup_hour, pickup_day_num, pickup_borough, pickup_zone,
             dropoff_borough, dropoff_zone)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', batch)
        conn.commit()
        count += len(batch)

    conn.close()
    print(f"Done! {count:,} trips loaded.")

if __name__ == '__main__':
    init_database()
    load_zones()
    load_trips()
