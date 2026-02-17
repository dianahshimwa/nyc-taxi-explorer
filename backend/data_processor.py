import pandas as pd
import json

# This class handles everything related to cleaning the data
class DataProcessor:
    
    def __init__(self):
        self.trips = None
        self.zones = None
        self.geojson = None
        # This list keeps track of every cleaning decision we make
        self.cleaning_log = []
    
    # -------------------------------------------------------
    # STEP 1: LOAD ALL THREE DATA FILES
    # -------------------------------------------------------
    def load_data(self):
        print("Loading parquet trip data...")
        self.trips = pd.read_parquet('../data/yellow_tripdata_2024-01.parquet')
        
        print("Loading zone lookup CSV...")
        self.zones = pd.read_csv('../data/taxi_zone_lookup.csv')
        
        print("Loading GeoJSON spatial data...")
        with open('../data/taxi_zones.geojson', 'r') as f:
            self.geojson = json.load(f)
        
        print(f"Trips loaded: {len(self.trips)}")
        print(f"Zones loaded: {len(self.zones)}")
        print(f"GeoJSON features: {len(self.geojson['features'])}")
    
    # -------------------------------------------------------
    # STEP 2: JOIN TRIPS WITH ZONE DATA
    # This connects the location IDs to actual zone names
    # -------------------------------------------------------
    def integrate_data(self):
        print("\nJoining trip data with zone metadata...")
        
        # Join pickup zone info
        self.trips = self.trips.merge(
            self.zones[['LocationID', 'Borough', 'Zone']],
            left_on='PULocationID',
            right_on='LocationID',
            how='left'
        )
        # Rename so we know these are pickup columns
        self.trips.rename(columns={
            'Borough': 'pickup_borough',
            'Zone': 'pickup_zone'
        }, inplace=True)
        self.trips.drop('LocationID', axis=1, inplace=True)
        
        # Join dropoff zone info
        self.trips = self.trips.merge(
            self.zones[['LocationID', 'Borough', 'Zone']],
            left_on='DOLocationID',
            right_on='LocationID',
            how='left'
        )
        self.trips.rename(columns={
            'Borough': 'dropoff_borough',
            'Zone': 'dropoff_zone'
        }, inplace=True)
        self.trips.drop('LocationID', axis=1, inplace=True)
        
        print("Zone data joined successfully")
    
    # -------------------------------------------------------
    # STEP 3: CLEAN THE DATA
    # Remove anything that doesnt make sense in real world
    # -------------------------------------------------------
    def clean_data(self):
        print("\nCleaning data...")
        original = len(self.trips)
        
        # Remove exact duplicate rows
        before = len(self.trips)
        self.trips = self.trips.drop_duplicates()
        self.cleaning_log.append(
            f"Duplicates removed: {before - len(self.trips)}"
        )
        
        # Remove rows where location IDs are missing
        before = len(self.trips)
        self.trips = self.trips.dropna(
            subset=['PULocationID', 'DOLocationID']
        )
        self.cleaning_log.append(
            f"Missing location IDs removed: {before - len(self.trips)}"
        )
        
        # Remove trips where fare is zero or negative
        # A real trip always costs money
        before = len(self.trips)
        self.trips = self.trips[self.trips['fare_amount'] > 0]
        self.cleaning_log.append(
            f"Invalid fares (zero or negative) removed: {before - len(self.trips)}"
        )
        
        # Remove trips with zero or negative distance
        before = len(self.trips)
        self.trips = self.trips[self.trips['trip_distance'] > 0]
        self.cleaning_log.append(
            f"Zero distance trips removed: {before - len(self.trips)}"
        )
        
        # Remove trips over 100 miles (likely GPS errors in NYC)
        before = len(self.trips)
        self.trips = self.trips[self.trips['trip_distance'] < 100]
        self.cleaning_log.append(
            f"Trips over 100 miles removed: {before - len(self.trips)}"
        )
        
        # Calculate duration so we can filter bad time records
        self.trips['duration_minutes'] = (
            pd.to_datetime(self.trips['tpep_dropoff_datetime']) -
            pd.to_datetime(self.trips['tpep_pickup_datetime'])
        ).dt.total_seconds() / 60
        
        # Remove trips with negative or zero duration
        before = len(self.trips)
        self.trips = self.trips[self.trips['duration_minutes'] > 0]
        self.cleaning_log.append(
            f"Negative/zero duration trips removed: {before - len(self.trips)}"
        )
        
        # Remove trips longer than 3 hours (very unusual for NYC taxi)
        before = len(self.trips)
        self.trips = self.trips[self.trips['duration_minutes'] < 180]
        self.cleaning_log.append(
            f"Trips over 3 hours removed: {before - len(self.trips)}"
        )
        
        # Remove trips where passenger count is impossible
        before = len(self.trips)
        self.trips = self.trips[
            (self.trips['passenger_count'] > 0) &
            (self.trips['passenger_count'] <= 6)
        ]
        self.cleaning_log.append(
            f"Invalid passenger count removed: {before - len(self.trips)}"
        )
        
        print(f"Original records: {original}")
        print(f"After cleaning: {len(self.trips)}")
        print(f"Total removed: {original - len(self.trips)}")
    
    # -------------------------------------------------------
    # STEP 4: NORMALIZE
    # Standardize formats so database storage is clean
    # -------------------------------------------------------
    def normalize_data(self):
        print("\nNormalizing data...")
        
        # Standardize timestamps to same format
        self.trips['pickup_datetime'] = pd.to_datetime(
            self.trips['tpep_pickup_datetime']
        ).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        self.trips['dropoff_datetime'] = pd.to_datetime(
            self.trips['tpep_dropoff_datetime']
        ).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Round money to 2 decimal places like real currency
        self.trips['fare_amount']  = self.trips['fare_amount'].round(2)
        self.trips['tip_amount']   = self.trips['tip_amount'].round(2)
        self.trips['total_amount'] = self.trips['total_amount'].round(2)
        self.trips['trip_distance'] = self.trips['trip_distance'].round(2)
        
        # Map payment type numbers to readable labels
        payment_map = {
            1: 'Credit Card',
            2: 'Cash',
            3: 'No Charge',
            4: 'Dispute',
            5: 'Unknown',
            6: 'Voided'
        }
        self.trips['payment_label'] = self.trips['payment_type'].map(payment_map)
        
        # Make sure location IDs are integers not floats
        self.trips['PULocationID'] = self.trips['PULocationID'].astype(int)
        self.trips['DOLocationID'] = self.trips['DOLocationID'].astype(int)
        
        print("Normalization done")
    
    # -------------------------------------------------------
    # STEP 5: FEATURE ENGINEERING
    # Create new columns from existing ones that give
    # us better insight into the data
    # -------------------------------------------------------
    def create_features(self):
        print("\nCreating derived features...")
        
        # Feature 1: Trip speed in MPH
        # Why: Helps identify traffic congestion by zone and time
        # How: distance divided by duration (converted to hours)
        self.trips['speed_mph'] = (
            self.trips['trip_distance'] /
            (self.trips['duration_minutes'] / 60)
        ).round(2)
        
        # Feature 2: Revenue per mile
        # Why: Shows which trips are most profitable per distance
        # Short airport pickups vs long outer borough rides
        # How: total fare divided by miles traveled
        self.trips['revenue_per_mile'] = (
            self.trips['total_amount'] /
            self.trips['trip_distance']
        ).round(2)
        
        # Feature 3: Rush hour flag
        # Why: Separates peak demand from normal traffic
        # Helps understand how time of day affects taxi usage
        # How: Weekdays (Mon-Fri) between 7-9am or 5-7pm
        hour = pd.to_datetime(self.trips['pickup_datetime']).dt.hour
        day  = pd.to_datetime(self.trips['pickup_datetime']).dt.dayofweek
        
        self.trips['is_rush_hour'] = (
            ((hour >= 7) & (hour < 9)) |
            ((hour >= 17) & (hour < 19))
        ) & (day < 5)
        
        self.trips['is_rush_hour'] = self.trips['is_rush_hour'].astype(int)
        
        # Also extract hour and day of week for charts
        self.trips['pickup_hour']    = hour
        self.trips['pickup_day_num'] = day
        
        print("Features created: speed_mph, revenue_per_mile, is_rush_hour")
    
    # -------------------------------------------------------
    # STEP 6: SAVE OUTPUTS
    # -------------------------------------------------------
    def save_outputs(self):
        print("\nSaving outputs...")
        
        # Save the cleaning log
        with open('cleaning_log.txt', 'w') as f:
            f.write("DATA CLEANING LOG\n")
            f.write("=" * 40 + "\n\n")
            for entry in self.cleaning_log:
                f.write(f"- {entry}\n")
        print("Cleaning log saved: cleaning_log.txt")
        
        # Keep only the columns we actually need for the database
        columns_to_keep = [
            'pickup_datetime',
            'dropoff_datetime',
            'passenger_count',
            'trip_distance',
            'PULocationID',
            'DOLocationID',
            'payment_type',
            'payment_label',
            'fare_amount',
            'tip_amount',
            'total_amount',
            'duration_minutes',
            'speed_mph',
            'revenue_per_mile',
            'is_rush_hour',
            'pickup_hour',
            'pickup_day_num',
            'pickup_borough',
            'pickup_zone',
            'dropoff_borough',
            'dropoff_zone'
        ]
        
        self.trips[columns_to_keep].to_csv(
            'processed_trips.csv',
            index=False
        )
        print("Processed trips saved: processed_trips.csv")


# Run everything
if __name__ == "__main__":
    processor = DataProcessor()
    processor.load_data()
    processor.integrate_data()
    processor.clean_data()
    processor.normalize_data()
    processor.create_features()
    processor.save_outputs()
    print("\nData processing complete!")