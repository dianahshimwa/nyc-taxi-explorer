from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
from custom_algorithm import TaxiZoneRanker

app = Flask(__name__)
CORS(app, origins="*", supports_credentials=False)  # Allow frontend to call this API

DATABASE = 'nyc_taxi.db'

def get_db():
    """Create a database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/', methods=['GET'])
def home():
    return jsonify({'message': 'NYC Taxi API is running'})

@app.route('/api/stats', methods=['GET'])
def get_stats():
    conn = get_db()
    cursor = conn.cursor()
    
# Total trips

    cursor.execute('SELECT COUNT(*) as count FROM trips')
    total_trips = cursor.fetchone()['count']
    
# Average fare

    cursor.execute('SELECT ROUND(AVG(total_amount), 2) as avg FROM trips')
    avg_fare = cursor.fetchone()['avg']
    
# Total revenue

    cursor.execute('SELECT ROUND(SUM(total_amount), 2) as total FROM trips')
    total_revenue = cursor.fetchone()['total']
    
# Rush hour percentage

    cursor.execute('SELECT ROUND(AVG(is_rush_hour) * 100, 1) as pct FROM trips')
    rush_hour_pct = cursor.fetchone()['pct']
    
# Average distance

    cursor.execute('SELECT ROUND(AVG(trip_distance), 2) as avg FROM trips')
    avg_distance = cursor.fetchone()['avg']
    
    conn.close()
    
    return jsonify({
        'total_trips': total_trips,
        'average_fare': avg_fare,
        'total_revenue': total_revenue,
        'rush_hour_pct': rush_hour_pct,
        'average_distance': avg_distance
    })

# Hourly trips

@app.route('/api/hourly', methods=['GET'])
def get_hourly():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT
            pickup_hour as hour,
            COUNT(*) as trip_count,
            ROUND(AVG(total_amount), 2) as avg_fare,
            ROUND(AVG(speed_mph), 2) as avg_speed
        FROM trips
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    ''')
    
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(rows)

# Use custom algorithms

@app.route('/api/top-zones', methods=['GET'])
def get_top_zones():
    limit = request.args.get('limit', 10, type=int)
    
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('SELECT PULocationID, total_amount FROM trips')
    trips = [dict(row) for row in cursor.fetchall()]
    
# Use our custom quicksort algorithm to rank zones

    ranker = TaxiZoneRanker()
    ranked_zones = ranker.rank_zones_by_revenue(trips)[:limit]
    
# Add zone names by looking up each zone

    results = []
    for zone_id, revenue in ranked_zones[:limit]:
        cursor.execute(
            'SELECT Borough, Zone FROM zones WHERE LocationID = ?',
            (zone_id,)
        )
        zone = cursor.fetchone()
        
        results.append({
            'zone_id': zone_id,
            'borough': zone['Borough'] if zone else 'Unknown',
            'zone': zone['Zone'] if zone else 'Unknown',
            'revenue': round(revenue, 2)
        })
    
    conn.close()
    return jsonify(results)

# Donut chart

@app.route('/api/distance-distribution', methods=['GET'])
def get_distance_distribution():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT
            CASE
                WHEN trip_distance < 1  THEN '0-1 miles'
                WHEN trip_distance < 3  THEN '1-3 miles'
                WHEN trip_distance < 5  THEN '3-5 miles'
                WHEN trip_distance < 10 THEN '5-10 miles'
                ELSE '10+ miles'
            END as range,
            COUNT(*) as count
        FROM trips
        GROUP BY range
        ORDER BY MIN(trip_distance)
    ''')
    
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(rows)

# Bar chart

@app.route('/api/boroughs', methods=['GET'])
def get_boroughs():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT
            pickup_borough as borough,
            COUNT(*) as trip_count,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_fare
        FROM trips
        WHERE pickup_borough IS NOT NULL
        GROUP BY pickup_borough
        ORDER BY trip_count DESC
    ''')
    
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(rows)

@app.route('/api/trips', methods=['GET'])
def get_trips():
    limit = request.args.get('limit', 100, type=int)
    borough = request.args.get('borough', '')
    min_fare = request.args.get('min_fare', 0, type=float)
    max_fare = request.args.get('max_fare', 9999, type=float)
    rush_hour = request.args.get('rush_hour', '')
    sort_by = request.args.get('sort_by', 'pickup_datetime')
    order = request.args.get('order', 'DESC')
    
# Prevent sql injections

    allowed_sorts = [
        'pickup_datetime',
        'total_amount',
        'trip_distance',
        'duration_minutes',
        'speed_mph'
    ]
    safe_sort = sort_by if sort_by in allowed_sorts else 'pickup_datetime'
    safe_order = 'ASC' if order == 'ASC' else 'DESC'
    
# Build query based on filters

    query = '''
        SELECT
            trip_id,
            pickup_datetime,
            pickup_borough,
            pickup_zone,
            dropoff_borough,
            dropoff_zone,
            trip_distance,
            duration_minutes,
            speed_mph,
            total_amount,
            payment_label,
            is_rush_hour
        FROM trips
        WHERE total_amount BETWEEN ? AND ?
        AND trip_id % 50 = 0
    '''
    params = [min_fare, max_fare]
    
    if borough:
        query += ' AND pickup_borough = ?'
        params.append(borough)
    
    if rush_hour != '':
        query += ' AND is_rush_hour = ?'
        params.append(int(rush_hour))
    
    query += f' ORDER BY {safe_sort} {safe_order} LIMIT ?'
    params.append(limit)
    
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(query, params)
    
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(rows)

@app.route('/api/payment-types', methods=['GET'])
def get_payment_types():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT
            payment_label as payment_type,
            COUNT(*) as count,
            ROUND(AVG(tip_amount), 2) as avg_tip
        FROM trips
        WHERE payment_label IS NOT NULL
        GROUP BY payment_label
        ORDER BY count DESC
    ''')
    
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(rows)

# Start server

if __name__ == '__main__':
    print('Starting Flask server...')
    print('API running at http://localhost:5000')
    app.run(debug=True, port=5000)