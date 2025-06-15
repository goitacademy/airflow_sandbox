#!/usr/bin/env python3
"""
MySQL Checker and Monitor for GoIT Final Project
Checks the status of the MySQL table for the streaming pipeline
and helps diagnose and fix common issues
"""

import mysql.connector
from mysql.connector import Error
import time
import sys
import os
from datetime import datetime
import pandas as pd

# Database configuration - Use the correct host and credentials for your environment
# For remote olympis dataset
REMOTE_DB_CONFIG = {
    'host': '217.61.57.46',
    'port': 3306,
    'database': 'olympic_dataset',
    'user': 'neo_data_admin',
    'password': 'Proyahaxuqithab9oplp'
}

# For local MySQL instance where streaming writes to
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'neo4j',
    'password': 'admin',
    'database': 'neo4j'
}

# Table name 
TABLE_NAME = 'fefelov_athlete_enriched_avg'

def connect_and_show_results():
    """Connect to MySQL and show your streaming results"""
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        print("🔗 Connected to MySQL database successfully!")
        print("=" * 60)
        
        # Your streaming results table
        table_name = "fefelov_athlete_enriched_avg"
        
        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if not cursor.fetchone():
            print(f"❌ Table '{table_name}' does not exist yet.")
            print("💡 You need to run your streaming DAG first!")
            return
        
        # Show table structure
        print(f"📋 Table Structure: {table_name}")
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  - {col[0]} ({col[1]})")
        
        print("\n" + "=" * 60)
        
        # Show recent data (for screenshot)
        print(f"📊 Recent Data from {table_name}:")
        print("(This is what you need for your screenshot)")
        print("-" * 60)
        
        query = f"""
        SELECT sport, medal, sex, country_noc, 
               ROUND(avg_height, 2) as avg_height,
               ROUND(avg_weight, 2) as avg_weight, 
               timestamp
        FROM {table_name} 
        ORDER BY timestamp DESC 
        LIMIT 20
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        if not results:
            print("❌ No data found in table.")
            print("💡 Run your streaming DAG to populate data!")
        else:
            # Print header
            print(f"{'Sport':<15} {'Medal':<10} {'Sex':<5} {'Country':<8} {'Avg Height':<12} {'Avg Weight':<12} {'Timestamp':<20}")
            print("-" * 100)
            
            # Print data
            for row in results:
                sport, medal, sex, country, height, weight, timestamp = row
                print(f"{sport:<15} {medal:<10} {sex:<5} {country:<8} {height:<12} {weight:<12} {timestamp}")
        
        # Show total count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cursor.fetchone()[0]
        print(f"\n📈 Total records in table: {total_count}")
        
        print("\n" + "=" * 60)
        print("📸 SCREENSHOT INSTRUCTIONS:")
        print("1. Take a screenshot of the data above")
        print("2. Make sure the table name and timestamp are visible")
        print("3. This shows your streaming pipeline final results!")
        
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        print("💡 Make sure your VPN is connected if required")
    
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("\n🔒 Database connection closed.")

if __name__ == "__main__":
    print("🎯 GoIT Final Project - Streaming Results Checker")
    print(f"📅 Run time: {datetime.now()}")
    print()
    connect_and_show_results()
