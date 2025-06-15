#!/usr/bin/env python3
"""
Final Project Verification Script
Run this to verify all files are ready for submission
"""

import os
import ast
import sys

def check_syntax(filepath):
    """Check if Python file has valid syntax"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            source = f.read()
        ast.parse(source)
        return True, "✅ Syntax OK"
    except SyntaxError as e:
        return False, f"❌ Syntax Error: {e}"
    except Exception as e:
        return False, f"❌ Error: {e}"

def main():
    print("🔍 GoIT Final Project Verification")
    print("=" * 50)
    
    # Check Part 1 files
    print("\n📋 Part 1: Streaming Pipeline")
    part1_files = [
        "part1_streaming/streaming_solution.py",
        "part1_streaming/src/streaming/kafka_spark_streaming.py",
        "part1_streaming/src/common/config.py",
        "part1_streaming/src/common/spark_manager.py"
    ]
    
    for file in part1_files:
        if os.path.exists(file):
            valid, msg = check_syntax(file)
            print(f"  {file}: {msg}")
        else:
            print(f"  {file}: ❌ File missing")
    
    # Check Part 2 files  
    print("\n📋 Part 2: Batch Pipeline")
    part2_files = [
        "part2_batch/project_solution.py",
        "part2_batch/src/batch/landing_to_bronze.py",
        "part2_batch/src/batch/bronze_to_silver.py", 
        "part2_batch/src/batch/silver_to_gold.py"
    ]
    
    for file in part2_files:
        if os.path.exists(file):
            valid, msg = check_syntax(file)
            print(f"  {file}: {msg}")
        else:
            print(f"  {file}: ❌ File missing")
    
    # Check documentation
    print("\n📋 Documentation")
    doc_files = ["README.md", "SCREENSHOTS.md", "VERIFICATION_CHECKLIST.md", "requirements.txt"]
    
    for file in doc_files:
        if os.path.exists(file):
            print(f"  {file}: ✅ Present")
        else:
            print(f"  {file}: ❌ Missing")
    
    print("\n" + "=" * 50)
    print("🎯 Next Steps:")
    print("1. Test both DAGs in Airflow UI")
    print("2. Capture required screenshots")
    print("3. Create public repository 'goit-de-fp'")
    print("4. Upload all files from this directory")
    print("5. Create archive ФП_ПІБ.zip for submission")
    print("✨ Good luck with your final project!")

if __name__ == "__main__":
    main()
