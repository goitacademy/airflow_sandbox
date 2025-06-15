#!/usr/bin/env python3
"""
Streaming Progress Monitor
Check if your streaming pipeline has written data to MySQL yet
"""

import time
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Your database configuration
DB_CONFIG = {
    'host': '217.61.57.46',
    'port': 3306,
    'database': 'olympic_dataset',
    'user': 'neo_data_admin',
    'password': 'Proyahaxuqithab9oplp'
}

def check_streaming_progress():
    """Monitor streaming pipeline progress"""
    table_name = "fefelov_athlete_enriched_avg"
    
    print("🔍 Monitoring Streaming Pipeline Progress...")
    print("=" * 60)
    print(f"📅 Started monitoring at: {datetime.now()}")
    print(f"🎯 Looking for table: {table_name}")
    print("⏳ Checking every 30 seconds...")
    print("\n💡 Your streaming DAG logs show:")
    print("   ✅ Pipeline started at 17:01:50 UTC")
    print("   ✅ 316,844 records written to Kafka")
    print("   ✅ Processing batches every 30 seconds")
    print("   ⏳ Waiting for aggregated data to appear in MySQL...")
    print("\n" + "=" * 60)
    
    attempt = 1
    max_attempts = 10  # Check for 5 minutes max
    
    while attempt <= max_attempts:
        try:
            connection = mysql.connector.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            print(f"\n🔍 Attempt {attempt}/{max_attempts} at {datetime.now().strftime('%H:%M:%S')}")
            
            # Check if table exists
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            table_exists = cursor.fetchone()
            
            if not table_exists:
                print(f"   ❌ Table '{table_name}' not created yet")
                print("   💭 Streaming pipeline still aggregating data...")
            else:
                print(f"   ✅ Table '{table_name}' exists!")
                
                # Check row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    print(f"   📊 Table exists but empty (0 rows)")
                    print("   💭 Waiting for first aggregated batch...")
                else:
                    print(f"   🎉 SUCCESS! Found {count} rows of aggregated data!")
                    print(f"   📸 Ready for screenshot!")
                    
                    # Show sample data
                    print("\n📊 Sample data for screenshot:")
                    print("-" * 80)
                    cursor.execute(f"""
                        SELECT sport, medal, sex, country_noc, 
                               ROUND(avg_height, 2) as avg_height,
                               ROUND(avg_weight, 2) as avg_weight, 
                               timestamp
                        FROM {table_name} 
                        ORDER BY timestamp DESC 
                        LIMIT 5
                    """)
                    
                    results = cursor.fetchall()
                    print(f"{'Sport':<15} {'Medal':<10} {'Sex':<5} {'Country':<8} {'Height':<8} {'Weight':<8} {'Timestamp':<20}")
                    print("-" * 80)
                    for row in results:
                        sport, medal, sex, country, height, weight, timestamp = row
                        print(f"{sport:<15} {medal:<10} {sex:<5} {country:<8} {height:<8} {weight:<8} {timestamp}")
                    
                    print("\n🎯 SCREENSHOT INSTRUCTIONS:")
                    print("1. Take a screenshot of the data above")
                    print("2. This is your Part 1 database screenshot!")
                    print("3. Make sure table name and timestamp are visible")
                    
                    connection.close()
                    return True
            
            connection.close()
            
            if attempt < max_attempts:
                print("   ⏳ Waiting 30 seconds for next check...")
                time.sleep(30)
            
            attempt += 1
            
        except Error as e:
            print(f"   ❌ Database error: {e}")
            time.sleep(30)
            attempt += 1
    
    print(f"\n⏰ Monitoring completed after {max_attempts} attempts")
    print("💡 If no data appeared, the streaming pipeline may need more time")
    print("💡 You can run this script again later or check Airflow logs")
    return False

if __name__ == "__main__":
    success = check_streaming_progress()
    if success:
        print("\n🎉 Streaming data found! Ready for screenshots!")
    else:
        print("\n⏳ No data yet, check again in a few minutes")
