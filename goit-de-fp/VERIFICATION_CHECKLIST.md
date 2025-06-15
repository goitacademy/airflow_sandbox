# Final Project Verification Checklist

## ✅ Pre-Submission Verification

### Part 1: Streaming Pipeline Verification
**DAG Name**: `fefelov_streaming_pipeline_v7`
**File**: `fefelov/streaming_solution.py`

**Required Screenshots:**
- [ ] **Database Results**: Final aggregated data in MySQL
  - Location: Check MySQL table with aggregated results
  - Should show: sport, medal, sex, country_noc, avg_height, avg_weight, timestamp
  
- [ ] **Kafka Topic Output**: Data from output Kafka topic
  - Location: Kafka consumer output or Airflow task logs
  - Should show: JSON records being written to output topic

**Verification Steps:**
1. Go to Airflow UI
2. Find DAG: `fefelov_streaming_pipeline_v7`
3. Trigger manual run
4. Check task: `fefelov_kafka_spark_streaming`
5. Check logs for successful execution
6. Verify data in MySQL and Kafka

---

### Part 2: Batch Pipeline Verification  
**DAG Name**: `fefelov_olympic_medals_dag_v2_robust`
**File**: `fefelov/project_solution.py`

**Required Screenshots:**
- [ ] **Landing to Bronze**: DataFrame from `landing_to_bronze_athlete_bio` task
- [ ] **Bronze to Silver**: DataFrame from `bronze_to_silver_athlete_bio` task  
- [ ] **Silver to Gold**: DataFrame from `silver_to_gold` task
- [ ] **DAG Graph**: Completed DAG execution graph

**Verification Steps:**
1. Go to Airflow UI
2. Find DAG: `fefelov_olympic_medals_dag_v2_robust`
3. Trigger manual run
4. For each task, click → Logs → Find `df.show()` output
5. Take screenshots ensuring timestamp is visible
6. Take screenshot of DAG graph showing successful run

---

## 🔧 Current Status

### Files Ready:
✅ Both main DAG files syntax validated
✅ All supporting files in place
✅ Encrypted archives created
✅ Repository structure prepared
✅ README and documentation created

### Next Steps:
1. **Test both DAGs** in Airflow UI
2. **Capture screenshots** as specified above
3. **Create public repository** `goit-de-fp`
4. **Upload files** to repository
5. **Create archive** with format `ФП_ПІБ.zip`
6. **Submit** repository link and archive

---

## 📁 Files for Repository Upload

Copy these directories to your `goit-de-fp` repository:
- `part1_streaming/` - Complete streaming solution
- `part2_batch/` - Complete batch solution  
- `requirements.txt` - Dependencies
- `README.md` - Project documentation
- `SCREENSHOTS.md` - Screenshots document (with your screenshots)

---

## 🎯 Success Criteria

**Part 1 (50 points):**
- [x] Stage 1: Read athlete bio from MySQL (10 pts)
- [x] Stage 2: Filter invalid height/weight (5 pts)  
- [x] Stage 3: Read MySQL → write to Kafka (10 pts)
- [x] Stage 4: Join streams by athlete_id (5 pts)
- [x] Stage 5: Calculate avg stats (5 pts)
- [x] Stage 6: Stream to Kafka + DB (15 pts)

**Part 2 (50 points):**
- [x] Stage 1: FTP → Bronze parquet (15 pts)
- [x] Stage 2: Bronze → Silver clean (15 pts)
- [x] Stage 3: Silver → Gold aggregation (10 pts)
- [x] Stage 4: Airflow DAG orchestration (10 pts)
