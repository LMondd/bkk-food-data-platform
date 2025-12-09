import pandas as pd
import random
import time
from faker import Faker
from datetime import datetime, timedelta
import os

# 1. SETUP
fake = Faker('th_TH') # Thai locale

# Constants for Bangkok Geospatial Bounds
BKK_LAT_MIN, BKK_LAT_MAX = 13.6000, 13.9500
BKK_LON_MIN, BKK_LON_MAX = 100.3500, 100.7000

RESTAURANT_CATEGORIES = ['Thai Street Food', 'Bubble Tea', 'Som Tum', 'Japanese', 'Burgers']
PAYMENT_METHODS = ['Credit Card', 'Cash', 'QR PromptPay']

# OUTPUT PATH (Crucial for Airflow to find the file later)
OUTPUT_PATH = "/opt/airflow/dags/"

def generate_random_coordinates():
    lat = random.uniform(BKK_LAT_MIN, BKK_LAT_MAX)
    lon = random.uniform(BKK_LON_MIN, BKK_LON_MAX)
    return lat, lon

def generate_mock_data(num_orders=500):
    data = []
    print(f"🚀 Generating {num_orders} mock orders for Bangkok area...")

    for _ in range(num_orders):
        order_time = fake.date_time_between(start_date='-1d', end_date='now')
        user_lat, user_lon = generate_random_coordinates()
        restaurant_lat, restaurant_lon = generate_random_coordinates()
        
        order_item = {
            'order_id': fake.uuid4(),
            'customer_name': fake.name(),
            'restaurant_name': f"{fake.company()} {random.choice(['Kitchen', 'Bistro', 'Cafe'])}",
            'category': random.choice(RESTAURANT_CATEGORIES),
            'payment_method': random.choice(PAYMENT_METHODS),
            'total_amount': round(random.uniform(40, 1200), 2),
            'delivery_fee': round(random.uniform(10, 50), 2),
            'order_timestamp': order_time,
            'customer_lat': user_lat,
            'customer_long': user_lon,
            'restaurant_lat': restaurant_lat,
            'restaurant_long': restaurant_lon,
            'status': 'COMPLETED'
        }
        data.append(order_item)
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    df = generate_mock_data(500)
    
    # 1. Save timestamped version (for backup/history)
    timestamp_filename = f"{OUTPUT_PATH}lmwn_mock_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(timestamp_filename, index=False, encoding='utf-8')
    
    # 2. Save "Latest" version (This is what the Pipeline will look for)
    latest_filename = f"{OUTPUT_PATH}latest_orders.csv"
    df.to_csv(latest_filename, index=False, encoding='utf-8')

    print(f"✅ Backup saved: {timestamp_filename}")
    print(f"✅ Pipeline file saved: {latest_filename}")