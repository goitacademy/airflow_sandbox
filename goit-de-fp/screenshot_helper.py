#!/usr/bin/env python3
"""
Screenshot Helper Script
This script helps you prepare and verify your DAGs for screenshot capture
"""

import os
import sys
import mysql.connector
from datetime import datetime
import pandas as pd

def show_mysql_results():
    """Display the results from the MySQL table for screenshot"""
    print("\n🗄️ MYSQL TABLE RESULTS")
    print("=" * 60)
    
    try:
        # Database connection configuration
        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'neo4j',
            'password': 'admin',
            'database': 'neo4j'
        }
        
        print(f"🔌 Connecting to MySQL: {config['host']}:{config['port']}")
        
        # Connect to MySQL
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Check if table exists
        table_name = 'fefelov_athlete_enriched_avg'
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'neo4j' 
            AND table_name = '{table_name}'
        """)
        
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            print(f"❌ Table '{table_name}' does not exist yet")
            print("   The streaming pipeline may still be processing data")
            print("   Wait for more data to accumulate and try again")
            return
        
        # Get table row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        print(f"✅ Table '{table_name}' found with {row_count} rows")
        
        if row_count == 0:
            print("   Table exists but is empty")
            print("   Wait for streaming pipeline to process more data")
            return
        
        # Fetch and display data
        print(f"\n📊 DISPLAYING DATA FROM {table_name}:")
        print("-" * 40)
        
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
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Display as formatted table
        df = pd.DataFrame(results, columns=columns)
        print(df.to_string(index=False))
        
        print(f"\n📈 SUMMARY:")
        print(f"   Total records: {row_count}")
        print(f"   Last updated: {df['timestamp'].max() if not df.empty else 'N/A'}")
        print(f"   Sports covered: {df['sport'].nunique() if not df.empty else 0}")
        print(f"   Countries: {df['country_noc'].nunique() if not df.empty else 0}")
        
        print(f"\n✅ This data is ready for screenshot capture!")
        print(f"   Use any MySQL client to view and screenshot this table:")
        print(f"   - phpMyAdmin")
        print(f"   - MySQL Workbench") 
        print(f"   - DataGrip")
        print(f"   - Command line: mysql -u neo4j -p -h localhost")
        
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        print("   Make sure MySQL server is running and credentials are correct")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print(f"\n🔌 MySQL connection closed")

def check_dag_files():
    """Check if DAG files are ready for deployment"""
    print("🔍 Checking DAG Files for Screenshot Readiness")
    print("=" * 60)
    
    # Part 1 - Streaming DAG
    streaming_dag = "part1_streaming/streaming_solution.py"
    if os.path.exists(streaming_dag):
        print("✅ Part 1 - Streaming DAG: READY")
        print(f"   📁 File: {streaming_dag}")
        print("   🎯 DAG Name: fefelov_streaming_pipeline_v7")
        print("   📋 Key Task: fefelov_kafka_spark_streaming")
    else:
        print("❌ Part 1 - Streaming DAG: MISSING")
    
    print()
    
    # Part 2 - Batch DAG  
    batch_dag = "part2_batch/project_solution.py"
    if os.path.exists(batch_dag):
        print("✅ Part 2 - Batch DAG: READY")
        print(f"   📁 File: {batch_dag}")
        print("   🎯 DAG Name: fefelov_olympic_medals_dag_v2_robust")
        print("   📋 Key Tasks:")
        print("      - landing_to_bronze_athlete_bio")
        print("      - landing_to_bronze_athlete_event_results") 
        print("      - bronze_to_silver_athlete_bio")
        print("      - bronze_to_silver_athlete_event_results")
        print("      - silver_to_gold")
    else:
        print("❌ Part 2 - Batch DAG: MISSING")

def generate_screenshot_plan():
    """Generate a specific screenshot capture plan"""
    print("\n📸 SCREENSHOT CAPTURE PLAN")
    print("=" * 60)
    
    print("\n🎯 PART 1: STREAMING PIPELINE (2 screenshots needed)")
    print("-" * 40)
    print("Screenshot 1: DATABASE RESULTS")
    print("  📍 Where: MySQL database")
    print("  📋 What: Final aggregated table")
    print("  📊 Columns: sport, medal, sex, country_noc, avg_height, avg_weight, timestamp")
    print("  ⏰ When: After streaming DAG completes successfully")
    
    print("\nScreenshot 2: KAFKA TOPIC OUTPUT")
    print("  📍 Where: Airflow UI → fefelov_kafka_spark_streaming task → Logs")
    print("  📋 What: Log output showing data written to Kafka")
    print("  📊 Content: JSON records being streamed")
    print("  ⏰ When: During/after streaming task execution")
    
    print("\n🎯 PART 2: BATCH PIPELINE (4 screenshots needed)")
    print("-" * 40)
    print("Screenshot 3: LANDING TO BRONZE")
    print("  📍 Where: Airflow UI → landing_to_bronze task → Logs")
    print("  📋 What: df.show() output from CSV to Parquet conversion")
    print("  📊 Content: Raw data from FTP CSV files")
    print("  ⏰ When: After landing_to_bronze task completes")
    
    print("\nScreenshot 4: BRONZE TO SILVER")
    print("  📍 Where: Airflow UI → bronze_to_silver task → Logs")
    print("  📋 What: df.show() output after cleaning")
    print("  📊 Content: Cleaned and deduplicated data")
    print("  ⏰ When: After bronze_to_silver task completes")
    
    print("\nScreenshot 5: SILVER TO GOLD")
    print("  📍 Where: Airflow UI → silver_to_gold task → Logs")
    print("  📋 What: df.show() output of final aggregation")
    print("  📊 Content: Average height/weight by sport/medal/sex/country")
    print("  ⏰ When: After silver_to_gold task completes")
    
    print("\nScreenshot 6: DAG GRAPH")
    print("  📍 Where: Airflow UI → DAG Graph view")
    print("  📋 What: Complete DAG execution graph")
    print("  📊 Content: All tasks in green (successful)")
    print("  ⏰ When: After entire DAG run completes")

def provide_access_instructions():
    """Provide instructions for accessing Airflow UI"""
    print("\n🌐 AIRFLOW UI ACCESS")
    print("=" * 60)
    print("1. Open your web browser")
    print("2. Navigate to your Airflow UI (typically: http://your-server:8080)")
    print("3. Log in with your credentials")
    print("4. Look for these DAGs:")
    print("   • fefelov_streaming_pipeline_v7")
    print("   • fefelov_olympic_medals_dag_v2_robust")
    print("\n5. For each DAG:")
    print("   a) Click on DAG name")
    print("   b) Click 'Trigger DAG' button (▶️)")
    print("   c) Wait for completion")
    print("   d) Click on individual tasks")
    print("   e) Click 'Logs' tab")
    print("   f) Scroll to find df.show() output")
    print("   g) Take screenshot")

def main():
    print("📸 GoIT Final Project - Screenshot Helper")
    print("=" * 60)
    
    # Check if user wants to see MySQL results
    if len(sys.argv) > 1 and sys.argv[1] == "mysql":
        show_mysql_results()
        return
    
    check_dag_files()
    generate_screenshot_plan()
    provide_access_instructions()
    
    print("\n✨ NEXT STEPS:")
    print("1. Ensure your DAGs are deployed to Airflow")
    print("2. Access Airflow UI") 
    print("3. Trigger both DAGs")
    print("4. Follow the screenshot plan above")
    print("5. Update SCREENSHOTS.md with your images")
    print("\n🎯 To check MySQL table results, run:")
    print("   python screenshot_helper.py mysql")
    print("\n🎯 Good luck capturing your screenshots!")

if __name__ == "__main__":
    main()
