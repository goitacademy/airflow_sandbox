#!/usr/bin/env python3
"""
MySQL Connector Download Helper
Downloads the MySQL Connector JAR file needed for Spark
"""

import os
import sys
import requests
import shutil
from pathlib import Path

def download_mysql_connector():
    """Download MySQL Connector JAR file"""
    print("📥 Downloading MySQL Connector JAR")
    print("=" * 60)
    
    # Define directories and file path
    jar_dir = Path("d:/School/airflow_sandbox/jars")
    jar_file = jar_dir / "mysql-connector-j-8.0.32.jar"
    
    # Create directory if it doesn't exist
    jar_dir.mkdir(exist_ok=True, parents=True)
    
    # Check if file already exists
    if jar_file.exists():
        print(f"✅ MySQL Connector JAR already exists at: {jar_file}")
        overwrite = input("Download and overwrite anyway? (y/n): ")
        if overwrite.lower() != 'y':
            print("⏭️ Skipping download")
            return jar_file
    
    # Define download URL
    download_url = "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar"
    
    print(f"🔗 Downloading from: {download_url}")
    print(f"📁 Saving to: {jar_file}")
    
    try:
        # Download the file
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        
        # Save the file
        with open(jar_file, 'wb') as f:
            shutil.copyfileobj(response.raw, f)
            
        print(f"✅ Download completed! File size: {jar_file.stat().st_size / (1024*1024):.2f} MB")
        
        # Verify file existence
        if jar_file.exists():
            print("✅ JAR file verification successful")
            return jar_file
        else:
            print("❌ JAR file download failed - file not found after download")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Download error: {e}")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def update_config_paths():
    """Update config.py to point to the correct JAR path"""
    config_file = Path("d:/School/airflow_sandbox/fefelov/src/common/config.py")
    
    if not config_file.exists():
        print(f"❌ Config file not found at: {config_file}")
        return False
    
    jar_path = "d:/School/airflow_sandbox/jars/mysql-connector-j-8.0.32.jar"
    
    print(f"🔧 Updating MySQL JAR path in config to: {jar_path}")
    
    try:
        # Read the config file
        with open(config_file, 'r') as f:
            content = f.read()
        
        # Replace MySQL JAR path if found
        if "mysql_jar_path=" in content:
            new_content = content.replace(
                'mysql_jar_path=os.getenv("MYSQL_JAR_PATH", "./jars/mysql-connector-j-8.0.32.jar")',
                f'mysql_jar_path=os.getenv("MYSQL_JAR_PATH", "{jar_path}")'
            )
            
            # Write updated content
            with open(config_file, 'w') as f:
                f.write(new_content)
                
            print("✅ Config file updated successfully")
            return True
        else:
            print("❓ Could not find mysql_jar_path in config - no changes made")
            return False
            
    except Exception as e:
        print(f"❌ Error updating config: {e}")
        return False

if __name__ == "__main__":
    print("🚀 MySQL Connector Setup for GoIT Final Project")
    print("=" * 60)
    
    jar_file = download_mysql_connector()
    
    if jar_file:
        print(f"\n✅ MySQL Connector JAR is ready at: {jar_file}")
        print("🔧 Next steps:")
        print("1. Update your Spark configuration to use this JAR file")
        print("2. Restart your streaming pipeline")
        print("3. Check the MySQL table for data")
        
        # Update config file
        update_config_paths()
    else:
        print("\n❌ Failed to download MySQL Connector JAR")
        print("🔧 Please download it manually from:")
        print("https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar")
