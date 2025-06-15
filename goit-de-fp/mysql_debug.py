#!/usr/bin/env python3
"""
MySQL Connection Debug Script
Test MySQL connection and table creation for streaming pipeline
"""

import mysql.connector
import sys
from datetime import datetime

def test_mysql_connection():
    """Test MySQL connection and table creation"""
    print("🔍 MySQL Connection Debug")
    print("=" * 50)
    
    # Database configuration - match your config
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'neo4j',
        'password': 'admin',
        'database': 'neo4j'
    }
    
    try:
        print(f"🔌 Connecting to MySQL: {config['host']}:{config['port']}")
        print(f"   Database: {config['database']}")
        print(f"   User: {config['user']}")
        
        # Test connection
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        print("✅ MySQL connection successful!")
        
        # Test database access
        cursor.execute("SELECT DATABASE()")
        current_db = cursor.fetchone()[0]
        print(f"✅ Current database: {current_db}")
        
        # Test table creation permissions
        table_name = 'fefelov_athlete_enriched_avg'
        
        # Check if table exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{config['database']}' 
            AND table_name = '{table_name}'
        """)
        
        table_exists = cursor.fetchone()[0] > 0
        print(f"📊 Table '{table_name}' exists: {table_exists}")
        
        if table_exists:
            # Check table structure
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            print(f"📋 Table structure:")
            for col in columns:
                print(f"   {col[0]}: {col[1]}")
                
            # Check row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            print(f"📈 Row count: {row_count}")
            
            if row_count > 0:
                # Show sample data
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
                rows = cursor.fetchall()
                print(f"📄 Sample data:")
                for row in rows:
                    print(f"   {row}")
        
        else:
            print("⚠️  Table does not exist. Testing table creation...")
            
            # Test table creation
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
            
            try:
                cursor.execute(create_table_sql)
                connection.commit()
                print("✅ Table creation test successful!")
                
                # Insert test data
                insert_sql = f"""
                INSERT INTO {table_name} 
                (sport, medal, sex, country_noc, avg_height, avg_weight, timestamp) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                test_data = (
                    'Swimming',
                    'Gold', 
                    'M',
                    'USA',
                    180.5,
                    75.2,
                    datetime.now()
                )
                
                cursor.execute(insert_sql, test_data)
                connection.commit()
                print("✅ Test data insertion successful!")
                
                # Verify the insert
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"✅ Table now has {count} rows")
                
            except mysql.connector.Error as e:
                print(f"❌ Table creation failed: {e}")
                return False
                
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        print("\n🔧 Possible solutions:")
        print("1. Check if MySQL server is running")
        print("2. Verify database credentials")
        print("3. Check network connectivity")
        print("4. Verify database permissions")
        return False
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
        
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("\n🔌 MySQL connection closed")
    
    return True

def check_spark_mysql_config():
    """Check Spark-MySQL configuration"""
    print("\n🔧 Spark-MySQL Configuration Check")
    print("=" * 50)
    
    # Check if running in correct environment
    import os
    airflow_ctx = os.getenv("AIRFLOW_CTX_DAG_ID")
    if airflow_ctx:
        print(f"🎯 Running in Airflow context: {airflow_ctx}")
    else:
        print("🏠 Running in local environment")
    
    # Check required packages
    try:
        import pyspark
        print(f"✅ PySpark available: {pyspark.__version__}")
    except ImportError:
        print("❌ PySpark not available")
    
    try:
        import mysql.connector
        print(f"✅ MySQL connector available")
    except ImportError:
        print("❌ MySQL connector not available")

if __name__ == "__main__":
    print("🚀 MySQL Debug Script for GoIT Final Project")
    print("=" * 60)
    
    check_spark_mysql_config()
    success = test_mysql_connection()
    
    if success:
        print("\n🎉 All MySQL tests passed!")
        print("✅ Your streaming pipeline should be able to write to MySQL")
    else:
        print("\n⚠️  MySQL connection issues detected")
        print("❌ Fix these issues before running streaming pipeline")
