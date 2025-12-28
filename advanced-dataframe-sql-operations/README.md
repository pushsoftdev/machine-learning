# Session Metrics Analytics with PySpark

## 📋 Overview

This project demonstrates **user session identification and analytics** using PySpark Window Functions. It processes user event data to identify sessions based on time gaps, calculates session metrics, and stores the results in both AWS S3 (data lake) and MySQL (relational database).

---

## 🎯 What This Project Does

### **1. Session Identification**
- Uses **Window Functions** to identify user sessions based on time gaps
- A new session starts when there's a **30-minute gap** between consecutive events
- Assigns unique session IDs using cumulative sum over window partitions

### **2. Session Metrics Calculation**
- **Session Start/End Times**: First and last event timestamps
- **Total Events**: Count of events per session
- **Unique Event Types**: Distinct event types per session
- **Session Duration**: Time difference between first and last event
- **Bounce Detection**: Identifies single-event sessions (bounces)

### **3. Dual Storage**
- **AWS S3**: Stores session data in Parquet format, partitioned by date
- **MySQL**: Stores session data in relational format for querying

---

## 🏗️ Architecture

```
User Events (Sample Data)
    ↓
PySpark Window Functions
    ├─ Session Identification (lag, cumulative sum)
    ├─ Session Metrics Aggregation
    └─ Data Transformation
        ↓
    ┌───────────────┬───────────────┐
    ↓               ↓               ↓
AWS S3          MySQL         Console Output
(Parquet)    (JDBC Write)    (DataFrame.show)
```

---

## 🔑 Key Concepts Demonstrated

### **1. Window Functions**
- **`lag()`**: Access previous row's value within a partition
- **`partitionBy()`**: Group data by user_id
- **`orderBy()`**: Order events by timestamp
- **Cumulative Sum**: Running total for session ID assignment

### **2. Session Logic**
```python
# Identify new sessions based on time gap
new_session = 1 if (prev_event is NULL OR time_gap > 30 minutes) else 0

# Assign session IDs using cumulative sum
session_id = cumulative_sum(new_session) over (partition by user_id order by event_time)
```

### **3. Aggregation**
- Group by `user_id` and `session_id`
- Calculate min, max, count, countDistinct
- Derive metrics like duration and bounce status

---

## 📊 Sample Data Flow

### **Input Events:**
```
user_id | event_type | event_time
u1      | click      | 2025-12-27 10:00:00
u1      | view       | 2025-12-27 10:05:00  (5 min gap - same session)
u1      | click      | 2025-12-27 10:45:00  (40 min gap - NEW session)
u2      | view       | 2025-12-27 11:00:00
u2      | click      | 2025-12-27 11:20:00  (20 min gap - same session)
```

### **Output Sessions:**
```
user_id | session_id | session_start       | session_end         | total_events | is_bounce
u1      | 1          | 2025-12-27 10:00:00 | 2025-12-27 10:05:00 | 2            | 0
u1      | 2          | 2025-12-27 10:45:00 | 2025-12-27 10:45:00 | 1            | 1
u2      | 1          | 2025-12-27 11:00:00 | 2025-12-27 11:20:00 | 2            | 0
```

---

## 🛠️ Technology Stack

- **Apache Spark 4.0.1**: Distributed data processing
- **PySpark**: Python API for Spark
- **AWS S3**: Cloud object storage (data lake)
- **MySQL 8.0**: Relational database
- **Hadoop AWS 3.4.1**: S3 connector for Spark
- **MySQL Connector 8.0.33**: JDBC driver for MySQL

---

## 📦 Prerequisites

### **1. Software Requirements**
- Python 3.x
- Apache Spark 4.0.1
- MySQL Server 8.0+
- AWS Account (for S3 access)

### **2. Environment Variables**
```bash
export AWS_ACCESS_KEY="your-aws-access-key"
export AWS_SECRET_KEY="your-aws-secret-key"
export AWS_DEFAULT_REGION="us-east-1"  # or your region
```

### **3. MySQL Database Setup**
```sql
CREATE DATABASE kafka_demo;

USE kafka_demo;

CREATE TABLE website_sessions (
    user_id VARCHAR(50),
    session_id BIGINT,
    session_start DATETIME,
    session_end DATETIME,
    total_events BIGINT,
    unique_event_types BIGINT,
    session_duration_sec BIGINT,
    is_bounce INT,
    session_date DATE,
    session_key VARCHAR(255),
    PRIMARY KEY (session_key)
);
```

### **4. AWS S3 Bucket**
```bash
aws s3 mb s3://ml-user-events
```

---

## 🚀 Usage

### **Method 1: Using spark-submit (Recommended)**
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262,mysql:mysql-connector-java:8.0.33 \
  session_metrics.py
```

### **Method 2: Using Python directly**
```bash
python3 session_metrics.py
```

**Note**: Method 1 is preferred as it properly manages Spark dependencies.

---

## 📁 Output

### **1. Console Output**
The script displays two DataFrames:
1. **Event-level data** with session IDs
2. **Session-level metrics**

### **2. AWS S3**
- **Path**: `s3://ml-user-events/website-sessions/`
- **Format**: Parquet
- **Partitioning**: By `session_date` (e.g., `session_date=2025-12-27/`)

### **3. MySQL Table**
- **Database**: `kafka_demo`
- **Table**: `website_sessions`
- **Mode**: Append (adds new records)

---

## 🔍 Key Metrics Explained

| Metric | Description | Calculation |
|--------|-------------|-------------|
| **session_id** | Unique session identifier per user | Cumulative sum of `new_session` flag |
| **session_start** | First event timestamp in session | `MIN(event_time)` |
| **session_end** | Last event timestamp in session | `MAX(event_time)` |
| **total_events** | Number of events in session | `COUNT(*)` |
| **unique_event_types** | Distinct event types | `COUNT(DISTINCT event_type)` |
| **session_duration_sec** | Session length in seconds | `session_end - session_start` |
| **is_bounce** | Single-event session flag | `1` if `unique_event_types == 1`, else `0` |
| **session_date** | Date of session start | `DATE(session_start)` |
| **session_key** | Composite unique key | `user_id-session_id-session_date` |

---

## 🎓 Learning Objectives

This project teaches:

1. **Window Functions in PySpark**
   - Partitioning and ordering
   - Using `lag()` for time-series analysis
   - Cumulative aggregations

2. **Session Identification Logic**
   - Time-based session detection
   - Handling NULL values (first event)
   - Session boundary detection

3. **Data Aggregation**
   - GroupBy operations
   - Multiple aggregation functions
   - Derived metrics

4. **Multi-Sink Data Writing**
   - Writing to S3 (Parquet)
   - Writing to MySQL (JDBC)
   - Data format conversions (Unix timestamp ↔ DATETIME)

5. **Production Best Practices**
   - Partitioning for performance
   - Repartitioning before JDBC writes
   - Composite keys for uniqueness

---

## 🐛 Troubleshooting

### **Issue 1: ClassNotFoundException for MySQL Driver**
**Error**: `java.lang.ClassNotFoundException: com.mysql.cj.jdbc.Driver`

**Solution**: Add MySQL connector to Spark packages:
```python
.config('spark.jars.packages', "...,mysql:mysql-connector-java:8.0.33")
```

### **Issue 2: Data Truncation Error**
**Error**: `Incorrect datetime value: '1766813400' for column 'session_start'`

**Solution**: Convert Unix timestamps to DATETIME before writing to MySQL:
```python
.withColumn("session_start", from_unixtime("session_start"))
```

### **Issue 3: S3 Access Denied**
**Solution**: Verify AWS credentials are set correctly:
```bash
echo $AWS_ACCESS_KEY
echo $AWS_SECRET_KEY
```

### **Issue 4: MySQL Connection Refused**
**Solution**: Ensure MySQL is running and accessible:
```bash
mysql -u root -p -e "SHOW DATABASES;"
```

---

## 📈 Next Steps

- **Real-time Processing**: Integrate with Kafka for streaming session analytics
- **Advanced Metrics**: Add conversion rates, funnel analysis
- **ML Integration**: Use session features for user behavior prediction
- **Dashboard**: Connect MySQL to BI tools (Tableau, Grafana)

---

## 📝 Notes

- The 30-minute session timeout is configurable (line 46)
- Sample data is hardcoded for demonstration; replace with real data sources
- S3 writes use `append` mode - use `overwrite` for testing
- MySQL writes are repartitioned to 4 partitions for parallel inserts

---

## 🤝 Related Files

- **window_functions.py**: Basic window function examples and session identification logic

---

**Author**: Session Analytics Demo  
**Last Updated**: 2025-12-28  
**Spark Version**: 4.0.1

