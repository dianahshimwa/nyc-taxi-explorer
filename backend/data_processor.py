import pandas as pd
import json

class DataProcessor:
    
    def __init__(self):
        self.trips = None
        self.zones = None
        self.geojson = None
        self.cleaning_log = []
    
    # Load data files

    def load_data(self):
        print("Loading csv trip data...")
        self.trips = pd.read_csv('../data/yellow_tripdata_2019-01.csv')
        
        print("Loading zone lookup CSV...")
        self.zones = pd.read_csv('../data/taxi_zone_lookup.csv')
        
        print("Loading GeoJSON spatial data...")
        try:
            with open('../data/taxi_zones.geojson', 'r') as f:
                self.geojson = json.load(f)
            print(f"GeoJSON features: {len(self.geojson['features'])}")
        except FileNotFoundError:
            print("GeoJSON file not found (optional)")
            self.geojson = None
        
        print(f"Trips loaded: {len(self.trips)}")
        print(f"Zones loaded: {len(self.zones)}")
    
    
    # Connect locationIDs to zone names

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
    
    # Data cleaning (remove anything the doesn't make sense)

    def clean_data(self):
        print("\nCleaning data...")
        original = len(self.trips)

        before = len(self.trips)
        self.trips = self.trips.drop_duplicates()
        self.cleaning_log.append(
            f"Duplicates removed: {before - len(self.trips)}"
        )

        before = len(self.trips)
        self.trips = self.trips.dropna(
            subset=['PULocationID', 'DOLocationID']
        )
        self.cleaning_log.append(
            f"Missing location IDs removed: {before - len(self.trips)}"
        )
    
        before = len(self.trips)
        self.trips = self.trips[self.trips['fare_amount'] > 0]
        self.cleaning_log.append(
            f"Invalid fares (zero or negative) removed: {before - len(self.trips)}"
        )

        before = len(self.trips)
        self.trips = self.trips[self.trips['trip_distance'] > 0]
        self.cleaning_log.append(
            f"Zero distance trips removed: {before - len(self.trips)}"
        )

        before = len(self.trips)
        self.trips = self.trips[self.trips['trip_distance'] < 100]
        self.cleaning_log.append(
            f"Trips over 100 miles removed: {before - len(self.trips)}"
        )

        self.trips['duration_minutes'] = (
            pd.to_datetime(self.trips['tpep_dropoff_datetime']) -
            pd.to_datetime(self.trips['tpep_pickup_datetime'])
        ).dt.total_seconds() / 60

        before = len(self.trips)
        self.trips = self.trips[self.trips['duration_minutes'] > 0]
        self.cleaning_log.append(
            f"Negative/zero duration trips removed: {before - len(self.trips)}"
        )

        before = len(self.trips)
        self.trips = self.trips[self.trips['duration_minutes'] < 180]
        self.cleaning_log.append(
            f"Trips over 3 hours removed: {before - len(self.trips)}"
        )

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
    
    
    # Normalization

    def normalize_data(self):
        print("\nNormalizing data...")

        self.trips['pickup_datetime'] = pd.to_datetime(
            self.trips['tpep_pickup_datetime']
        ).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        self.trips['dropoff_datetime'] = pd.to_datetime(
            self.trips['tpep_dropoff_datetime']
        ).dt.strftime('%Y-%m-%d %H:%M:%S')

        self.trips['fare_amount']  = self.trips['fare_amount'].round(2)
        self.trips['tip_amount']   = self.trips['tip_amount'].round(2)
        self.trips['total_amount'] = self.trips['total_amount'].round(2)
        self.trips['trip_distance'] = self.trips['trip_distance'].round(2)

        payment_map = {
            1: 'Credit Card',
            2: 'Cash',
            3: 'No Charge',
            4: 'Dispute',
            5: 'Unknown',
            6: 'Voided'
        }
        self.trips['payment_label'] = self.trips['payment_type'].map(payment_map)

        self.trips['PULocationID'] = self.trips['PULocationID'].astype(int)
        self.trips['DOLocationID'] = self.trips['DOLocationID'].astype(int)
        
        print("Normalization done")

    def create_features(self):
        print("\nCreating derived features...")
        
        self.trips['speed_mph'] = (
            self.trips['trip_distance'] /
            (self.trips['duration_minutes'] / 60)
        ).round(2)
        
        self.trips['revenue_per_mile'] = (
            self.trips['total_amount'] /
            self.trips['trip_distance']
        ).round(2)
        
        hour = pd.to_datetime(self.trips['pickup_datetime']).dt.hour
        day  = pd.to_datetime(self.trips['pickup_datetime']).dt.dayofweek
        
        self.trips['is_rush_hour'] = (
            ((hour >= 7) & (hour < 9)) |
            ((hour >= 17) & (hour < 19))
        ) & (day < 5)
        
        self.trips['is_rush_hour'] = self.trips['is_rush_hour'].astype(int)
        
        self.trips['pickup_hour']    = hour
        self.trips['pickup_day_num'] = day
        
        print("Features created: speed_mph, revenue_per_mile, is_rush_hour")
    
    def save_outputs(self):
        print("\nSaving outputs...")
        
        with open('cleaning_log.txt', 'w') as f:
            f.write("DATA CLEANING LOG\n")
            f.write("=" * 40 + "\n\n")
            for entry in self.cleaning_log:
                f.write(f"- {entry}\n")
        print("Cleaning log saved: cleaning_log.txt")
        
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

if __name__ == "__main__":
    processor = DataProcessor()
    processor.load_data()
    processor.integrate_data()
    processor.clean_data()
    processor.normalize_data()
    processor.create_features()
    processor.save_outputs()
    print("\nData processing complete!")