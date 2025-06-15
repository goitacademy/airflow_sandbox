# GoIT Data Engineering Final Project - Screenshots

## Part 1: End-to-End Streaming Pipeline

### Your Streaming Database Table:
- **Table Name**: `fefelov_athlete_enriched_avg`
- **Database**: `olympic_dataset`
- **Host**: `217.61.57.46:3306`
- **Contents**: Final aggregated results with avg height/weight by sport, medal, sex, country_noc

### DAG to Test: `fefelov_streaming_pipeline_v7`

### Step-by-Step Instructions:

1. **Access Airflow UI**
   - Open your browser and go to the Airflow UI
   - Look for DAG: `fefelov_streaming_pipeline_v7`

2. **Trigger the Streaming DAG**
   - Click on the DAG name
   - Click "Trigger DAG" (play button)
   - Wait for execution to complete

3. **Screenshot 1: Database Results**
   - After successful run, access your MySQL database
   - Query table: `fefelov_athlete_enriched_avg`
   - Or run: `python mysql_checker.py` in this directory
   - **Should show columns**: sport, medal, sex, country_noc, avg_height, avg_weight, timestamp
   - **Screenshot requirement**: Table with data showing the final aggregated results
   - [ ] Database results: [Insert screenshot here]

4. **Screenshot 2: Kafka Topic Output** ✅ **FOUND IN LOGS!**
   - **From your logs** (2025-06-15 17:01:44): Successfully wrote to Kafka topic `fefelov_athlete_event_results`
   - **Data written**: 316,844 athlete event results
   - **Log excerpt to screenshot**:
     ```
     Writing to Kafka topic: fefelov_athlete_event_results
     Successfully wrote athlete event results to Kafka topic
     ```
   - [ ] Kafka topic output: [Insert screenshot here]

### 🎉 CURRENT STATUS - STREAMING DAG RUNNING!
✅ **Your streaming pipeline is working!**
- **Started**: 2025-06-15 17:01:50 UTC
- **Status**: Processing batches every 30 seconds  
- **Kafka**: ✅ 316,844 records written successfully
- **Database**: ⏳ Waiting for aggregated results (check in 2-3 minutes)

### Next Steps:
1. **Screenshot the Kafka logs** (you already have this data!)
2. **Wait 2-3 minutes** for more batches to process
3. **Run `python mysql_checker.py`** again to check for database results  
4. **Screenshot the database** once data appears in `fefelov_athlete_enriched_avg`

---

## Part 2: End-to-End Batch Data Lake

### DAG to Test: `fefelov_olympic_medals_dag_v2_robust`

### Step-by-Step Instructions:

1. **Access Airflow UI**
   - Look for DAG: `fefelov_olympic_medals_dag_v2_robust`

2. **Trigger the Batch DAG**
   - Click on the DAG name
   - Click "Trigger DAG" (play button)  
   - Wait for all tasks to complete successfully (should see green boxes)

3. **Screenshot 3: Landing to Bronze**
   - Click on task: `landing_to_bronze_athlete_bio` or `landing_to_bronze_athlete_event_results`
   - Click "Logs"
   - Scroll down to find `df.show()` output
   - **Should show**: DataFrame with columns from CSV file
   - **Make sure timestamp is visible** in the log entry
   - [ ] Landing to Bronze (df.show() output): [Insert screenshot here]

4. **Screenshot 4: Bronze to Silver**  
   - Click on task: `bronze_to_silver_athlete_bio` or `bronze_to_silver_athlete_event_results`
   - Click "Logs"
   - Scroll down to find `df.show()` output
   - **Should show**: Cleaned DataFrame (text cleaning, deduplication applied)
   - **Make sure timestamp is visible** in the log entry
   - [ ] Bronze to Silver (df.show() output): [Insert screenshot here]

5. **Screenshot 5: Silver to Gold**
   - Click on task: `silver_to_gold`
   - Click "Logs" 
   - Scroll down to find `df.show()` output
   - **Should show**: Final aggregated DataFrame with avg_height, avg_weight by sport/medal/sex/country
   - **Make sure timestamp is visible** in the log entry
   - [ ] Silver to Gold (df.show() output): [Insert screenshot here]

6. **Screenshot 6: DAG Graph**
   - Go back to the main DAG view for `fefelov_olympic_medals_dag_v2_robust`
   - Click on "Graph" view 
   - **Should show**: All tasks in green (successful execution)
   - Take screenshot of the entire DAG graph
   - [ ] DAG Graph (completed execution): [Insert screenshot here]

---

## 🎯 Quick Access URLs (if you know your Airflow URL):

**Assuming your Airflow is at `http://your-airflow-url:8080`:**

### Part 1 - Streaming:
- DAG: `http://your-airflow-url:8080/dags/fefelov_streaming_pipeline_v7/grid`
- Task Logs: `http://your-airflow-url:8080/dags/fefelov_streaming_pipeline_v7/grid` → Click task → Logs

### Part 2 - Batch:  
- DAG: `http://your-airflow-url:8080/dags/fefelov_olympic_medals_dag_v2_robust/grid`
- Task Logs: `http://your-airflow-url:8080/dags/fefelov_olympic_medals_dag_v2_robust/grid` → Click task → Logs

---

## 📋 Screenshot Checklist:

- [ ] ✅ Part 1: Database results (MySQL table with aggregated data)
- [ ] ✅ Part 1: Kafka topic output (from task logs)  
- [ ] ✅ Part 2: Landing to Bronze df.show() (with timestamp)
- [ ] ✅ Part 2: Bronze to Silver df.show() (with timestamp)
- [ ] ✅ Part 2: Silver to Gold df.show() (with timestamp)
- [ ] ✅ Part 2: DAG Graph (successful execution, all green)

## 💡 Tips for Better Screenshots:

1. **Include timestamps** - Very important for batch processing screenshots
2. **Full context** - Show enough of the log to understand what's happening
3. **Clear data** - Make sure DataFrame outputs are readable
4. **Success indicators** - Show green status indicators in DAG graphs
5. **Table names** - In database screenshots, show table/column names clearly

---

## ⚠️ Troubleshooting:

**If DAGs don't appear:**
- Check if your encrypted files were properly deployed
- Verify DAG files are in the correct Airflow dags folder
- Check Airflow logs for any import errors

**If tasks fail:**
- Check task logs for specific error messages
- Verify database connections and Kafka configurations
- Ensure all dependencies are installed

**If no df.show() output:**
- Look for any DataFrame.show() calls in the task logs
- Check if tasks completed successfully
- Verify Spark jobs executed properly
