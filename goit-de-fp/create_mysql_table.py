#!/usr/bin/env python3
"""
Create MySQL Table Script
This script manually creates the MySQL table needed for the streaming pipeline
"""

import mysql.connector
import sys
from datetime import datetime

def create_mysql_table():
    """Create the MySQL table for streaming pipeline output"""
    print("🔧 Creating MySQL Table for Streaming Pipeline")
    print("=" * 60)
    
    # Database configuration - adjust as needed
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'neo4j',
        'password': 'admin',
        'database': 'neo4j'
    }
    
    table_name = 'fefelov_athlete_enriched_avg'
    
    try:
        print(f"🔌 Connecting to MySQL: {config['host']}:{config['port']}")
        print(f"   Database: {config['database']}")
        print(f"   User: {config['user']}")
        
        # Connect to MySQL
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        print("✅ MySQL connection successful!")
        
        # Check if table exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{config['database']}' 
            AND table_name = '{table_name}'
        """)
        
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            print(f"⚠️ Table '{table_name}' already exists!")
            drop_table = input("Do you want to drop and recreate the table? (y/n): ")
            if drop_table.lower() == 'y':
                cursor.execute(f"DROP TABLE {table_name}")
                connection.commit()
                print(f"✅ Table '{table_name}' dropped")
            else:
                print("❌ Table operation cancelled")
                return
        
        # Create table with correct schema
        print(f"📝 Creating table '{table_name}'...")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            sport VARCHAR(255),
            medal VARCHAR(255),
            sex VARCHAR(10),
            country_noc VARCHAR(10),
            avg_height DOUBLE,
            avg_weight DOUBLE,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (sport, medal, sex, country_noc, timestamp)
        )
        """
        
        cursor.execute(create_table_sql)
        connection.commit()
        print(f"✅ Table '{table_name}' created successfully!")
        
        # Insert sample data for testing
        insert_data = input("Do you want to insert sample data? (y/n): ")
        if insert_data.lower() == 'y':
            print("📊 Inserting sample data...")
            sample_data = [
                ('Swimming', 'Gold', 'M', 'USA', 185.7, 82.3, datetime.now()),
                ('Swimming', 'Silver', 'M', 'AUS', 183.2, 80.1, datetime.now()),
                ('Athletics', 'Gold', 'F', 'JAM', 172.5, 65.8, datetime.now()),
                ('Gymnastics', 'Bronze', 'F', 'RUS', 155.0, 48.5, datetime.now()),
                ('Basketball', 'Gold', 'M', 'USA', 198.3, 95.7, datetime.now()),
            ]
            
            insert_sql = f"""
            INSERT INTO {table_name} 
            (sport, medal, sex, country_noc, avg_height, avg_weight, timestamp) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(insert_sql, sample_data)
            connection.commit()
            print(f"✅ Inserted {len(sample_data)} sample records")
            
            # Verify data
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            print("\n📋 Table Contents:")
            for row in rows:
                print(f"  {row}")
        
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        print("\nPossible solutions:")
        print("1. Check if MySQL server is running")
        print("2. Verify database credentials")
        print("3. Ensure user has CREATE TABLE permissions")
        return False
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
        
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print(f"\n🔌 MySQL connection closed")
    
    print("\n✅ MySQL table setup complete!")
    print("🚀 The streaming pipeline should now be able to write to this table.")
    print("📸 You can now capture screenshots of the table for your project submission.")
    return True

if __name__ == "__main__":
    create_mysql_table()
