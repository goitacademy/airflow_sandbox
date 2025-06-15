# GoIT Data Engineering Final Project

## Project Overview
This repository contains the complete implementation of the GoIT Data Engineering final project, consisting of two parts:

### Part 1: End-to-End Streaming Pipeline (50 points)
- **Location**: `part1_streaming/`
- **Main DAG**: `streaming_solution.py`
- **Core Logic**: `src/streaming/kafka_spark_streaming.py`
- **Description**: Kafka-Spark streaming pipeline for real-time processing of Olympic athlete data

**Requirements Implemented:**
1. ✅ Read athlete bio data from MySQL `olympic_dataset.athlete_bio`
2. ✅ Filter records with empty/non-numeric height/weight data
3. ✅ Read from MySQL and write to Kafka topic `athlete_event_results`
4. ✅ Read from Kafka stream and parse JSON to DataFrame
5. ✅ Join Kafka stream with bio data using `athlete_id`
6. ✅ Calculate average height/weight by sport, medal, sex, country_noc with timestamp
7. ✅ Stream results using forEachBatch to both Kafka topic and MySQL database

### Part 2: End-to-End Batch Data Lake (50 points)
- **Location**: `part2_batch/`
- **Main DAG**: `project_solution.py`
- **Batch Scripts**: `src/batch/`
- **Description**: Multi-hop data lake with landing → bronze → silver → gold architecture

**Requirements Implemented:**
1. ✅ `landing_to_bronze.py` - Download CSV from FTP, save as Parquet in bronze/
2. ✅ `bronze_to_silver.py` - Clean text, deduplicate, save to silver/
3. ✅ `silver_to_gold.py` - Join tables, calculate avg stats, save to gold/
4. ✅ `project_solution.py` - Airflow DAG orchestrating all three stages

## Technical Stack
- **Apache Airflow** - Workflow orchestration
- **Apache Spark** - Data processing (both batch and streaming)
- **Apache Kafka** - Stream processing
- **MySQL** - Data storage
- **Python** - Implementation language

## Project Structure
```
goit-de-fp/
├── part1_streaming/
│   ├── streaming_solution.py          # Main streaming DAG
│   └── src/
│       ├── streaming/
│       │   └── kafka_spark_streaming.py  # Core streaming logic
│       └── common/
│           ├── config.py              # Configuration management
│           ├── spark_manager.py       # Spark session management
│           └── utils.py               # Utility functions
├── part2_batch/
│   ├── project_solution.py           # Main batch DAG
│   └── src/
│       └── batch/
│           ├── landing_to_bronze.py   # Stage 1: FTP → Bronze
│           ├── bronze_to_silver.py    # Stage 2: Bronze → Silver
│           └── silver_to_gold.py      # Stage 3: Silver → Gold
├── requirements.txt                   # Python dependencies
└── README.md                         # This file
```

## Key Features
- **Robust Error Handling** - Comprehensive try-catch blocks with logging
- **Path Resolution** - Dynamic path handling for Airflow environment
- **Scalable Architecture** - Modular design with reusable components
- **Production Ready** - Proper logging, configuration management, and documentation

## Author
**Student**: Fefelov
**Course**: GoIT Data Engineering
**Date**: June 2025

---
*This project demonstrates proficiency in modern data engineering tools and practices, including real-time streaming, batch processing, and workflow orchestration.*
