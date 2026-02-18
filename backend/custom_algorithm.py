class TaxiZoneRanker:
    def __init__(self):
        self.comparisons = 0
    
    def quicksort(self, arr, key_func=None):
        if key_func is None:
            key_func = lambda x: x
        
        if len(arr) <= 1:
            return arr
        
        # Choose pivot (middle element)
        pivot_idx = len(arr) // 2
        pivot = arr[pivot_idx]
        pivot_value = key_func(pivot)
        
        # Partition into three lists
        left = []
        middle = []
        right = []
        
        for item in arr:
            self.comparisons += 1
            item_value = key_func(item)
            
            if item_value < pivot_value:
                left.append(item)
            elif item_value > pivot_value:
                right.append(item)
            else:
                middle.append(item)
        
        # Recursively sort and combine
        return (self.quicksort(left, key_func) + 
                middle + 
                self.quicksort(right, key_func))
    
    def rank_zones_by_revenue(self, trips_data):
    
        # Manually calculate revenue per zone (no groupby!)
        zone_revenue = {}
        
        for trip in trips_data:
            zone_id = trip['PULocationID']
            revenue = trip['total_amount']
            
            if zone_id in zone_revenue:
                zone_revenue[zone_id] += revenue
            else:
                zone_revenue[zone_id] = revenue
        
        # Convert to list of tuples
        zone_list = [(zone_id, revenue) for zone_id, revenue in zone_revenue.items()]
        
        # Sort using our custom quicksort (by revenue, descending)
        sorted_zones = self.quicksort(
            zone_list,
            key_func=lambda x: -x[1]  # Negative for descending order
        )
        
        return sorted_zones
    
    def get_stats(self):
        return {
            'comparisons': self.comparisons,
            'algorithm': 'QuickSort',
            'time_complexity_avg': 'O(n log n)',
            'time_complexity_worst': 'O(n²)',
            'space_complexity': 'O(log n)'
        }


# Test the algorithm
if __name__ == "__main__":
    # Sample test data
    test_trips = [
        {'PULocationID': 1, 'total_amount': 15.50},
        {'PULocationID': 2, 'total_amount': 22.00},
        {'PULocationID': 1, 'total_amount': 18.00},
        {'PULocationID': 3, 'total_amount': 45.50},
        {'PULocationID': 2, 'total_amount': 12.50},
        {'PULocationID': 1, 'total_amount': 9.00},
    ]
    
    ranker = TaxiZoneRanker()
    results = ranker.rank_zones_by_revenue(test_trips)
    
    print("Zone Revenue Rankings:")
    for zone_id, revenue in results:
        print(f"  Zone {zone_id}: ${revenue:.2f}")
    
    print(f"\nAlgorithm Stats:")
    stats = ranker.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")