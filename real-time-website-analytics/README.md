# Real-Time Website Analytics

A real-time data processing pipeline for website user event analytics using Apache Kafka, Apache Spark Structured Streaming, MySQL, and AWS S3.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Data Flow](#data-flow)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

This project implements a real-time analytics system that processes user events (play, pause, etc.) from a website. It demonstrates a complete streaming data pipeline that:

1. **Generates** simulated user events
2. **Streams** events through Kafka
3. **Processes** events in real-time using Spark Structured Streaming
4. **Stores** raw events in AWS S3 (data lake)
5. **Aggregates** events using time windows and watermarking
6. **Persists** aggregated metrics to MySQL database

## 🏗️ Architecture

```
┌─────────────┐      ┌─────────────┐      ┌──────────────────┐
│   Producer  │─────▶│    Kafka    │─────▶│  Spark Streaming │
│  (Events)   │      │   Topic     │      │   (Processing)   │
└─────────────┘      └─────────────┘      └──────────────────┘
                                                    │
                                          ┌─────────┴─────────┐
                                          │                   │
                                          ▼                   ▼
                                    ┌──────────┐      ┌──────────┐
                                    │  AWS S3  │      │  MySQL   │
                                    │ (Raw Data)│      │(Aggregates)│
                                    └──────────┘      └──────────┘
```

## ✨ Features

### Real-Time Processing
- **Streaming ingestion** from Kafka with configurable batch sizes
- **Watermarking** to handle late-arriving events (30-second tolerance)
- **Tumbling windows** for 1-minute aggregations
- **Parallel processing** leveraging Spark's distributed computing

### Data Storage
- **Raw event storage** in AWS S3 as Parquet files
- **Time-based partitioning** for efficient querying
- **Aggregated metrics** in MySQL for fast analytics queries

### Fault Tolerance
- **Checkpointing** for exactly-once processing semantics
- **Automatic recovery** from failures
- **Duplicate handling** with MySQL upsert operations

## 🛠️ Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Message Broker** | Apache Kafka | Latest |
| **Stream Processing** | Apache Spark | 4.0.1 |
| **Data Lake** | AWS S3 | - |
| **Database** | MySQL | 8.0+ |
| **Language** | Python | 3.x |
| **Storage Format** | Apache Parquet | - |

### Python Dependencies
- `kafka-python` - Kafka producer/consumer
- `pyspark` - Spark Structured Streaming
- `mysql-connector-python` - MySQL database connector
- `hadoop-aws` - S3 integration
- `aws-java-sdk-bundle` - AWS SDK for Hadoop

## 📦 Prerequisites

### Software Requirements
1. **Apache Kafka** (running on `localhost:9092`)
2. **Apache Spark** 4.0.1+
3. **MySQL** 8.0+ (running on `localhost:3306`)
4. **Python** 3.x
5. **AWS Account** with S3 access

### Environment Variables
Set the following environment variables:
```bash
export AWS_ACCESS_KEY="your-aws-access-key"
export AWS_SECRET_KEY="your-aws-secret-key"
export AWS_DEFAULT_REGION="your-aws-region"
```

### MySQL Database Setup
Create the database and table:
```sql
CREATE DATABASE kafka_demo;

USE kafka_demo;

CREATE TABLE user_event_aggregates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    total_value INT NOT NULL,
    event_count INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_window (user_id, event_type, window_start, window_end)
);

-- Create user for Spark
CREATE USER 'spark'@'localhost' IDENTIFIED BY '';
GRANT ALL PRIVILEGES ON kafka_demo.* TO 'spark'@'localhost';
FLUSH PRIVILEGES;
```

### Kafka Topic Setup
Create the Kafka topic:
```bash
kafka-topics --create \
  --topic user_events \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1
```

### AWS S3 Bucket
Create an S3 bucket:
```bash
aws s3 mb s3://ml-user-events
```

## 🚀 Setup

1. **Clone the repository**
   ```bash
   cd machine-learning/real-time-website-analytics
   ```

2. **Install Python dependencies**
   ```bash
   pip install kafka-python pyspark mysql-connector-python
   ```

3. **Configure environment variables**
   ```bash
   export AWS_ACCESS_KEY="your-key"
   export AWS_SECRET_KEY="your-secret"
   export AWS_DEFAULT_REGION="us-east-1"
   ```

4. **Start Kafka and MySQL services**
   ```bash
   # Start Kafka (if using Homebrew on macOS)
   brew services start kafka

   # Start MySQL
   brew services start mysql
   ```

## 💻 Usage

### 1. Start the Consumer (Spark Streaming)
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262 consumer.py
```

This will:
- Connect to Kafka topic `user_events`
- Process events in real-time
- Write raw events to S3
- Aggregate events by 1-minute windows
- Store aggregates in MySQL

### 2. Run the Producer (Event Generator)
```bash
python producer.py
```

This will:
- Generate 5 random user events
- Send events to Kafka topic `user_events`
- Display sent events in console

### 3. Read Data from S3 (Optional)
```bash
python s3reader.py
```

This will:
- Read Parquet files from S3
- Display schema and data
- Show record count

## 📊 Data Flow

### Event Schema
```json
{
  "user_id": 1,
  "event_time": "2025-12-25T19:47:56Z",
  "event_type": "play",
  "value": 3
}
```

### Processing Steps

1. **Producer** generates events with:
   - Random `user_id` (1-5)
   - Random `event_type` (play, pause)
   - Random `value` (1-10)
   - Current UTC timestamp

2. **Kafka** distributes events across partitions

3. **Spark Streaming** processes events:
   - Parses JSON payload
   - Converts timestamp to proper format
   - Applies 30-second watermark
   - Creates 1-minute tumbling windows
   - Aggregates by (window, user_id, event_type)

4. **S3 Storage**:
   - Writes raw events as Parquet files
   - Partitions by `event_date_time`
   - Format: `s3://ml-user-events/user-events/event_date_time=YYYY-MM-DD HH-mm-ss/`

5. **MySQL Storage**:
   - Stores aggregated metrics
   - Upserts on duplicate windows
   - Tracks total_value and event_count

### Aggregation Output Schema
```
window_start: timestamp
window_end: timestamp
user_id: integer
event_type: string
total_value: integer (sum of values)
events_count: integer (count of events)
```

## 📁 Project Structure

```
real-time-website-analytics/
├── producer.py          # Kafka event producer
├── consumer.py          # Spark Structured Streaming consumer
├── s3reader.py          # S3 data reader utility
└── readMe.md           # This file
```

### File Descriptions

- **producer.py**: Generates simulated user events and publishes to Kafka
- **consumer.py**: Main streaming application that processes events, writes to S3 and MySQL
- **s3reader.py**: Utility script to read and verify Parquet files from S3

## ⚙️ Configuration

### Spark Configuration
```python
spark.sql.streaming.forceDeleteTempCheckpointLocation = true
spark.hadoop.fs.s3a.access.key = <AWS_ACCESS_KEY>
spark.hadoop.fs.s3a.secret.key = <AWS_SECRET_KEY>
spark.hadoop.fs.s3a.endpoint = s3.amazonaws.com
spark.hadoop.fs.s3a.endpoint.region = <AWS_DEFAULT_REGION>
```

### Kafka Configuration
```python
bootstrap.servers = localhost:9092
topic = user_events
startingOffsets = latest
maxOffsetsPerTrigger = 500
```

### Streaming Configuration
```python
Watermark = 30 seconds
Window = 1 minute (tumbling)
Trigger = 10 seconds
Output Mode (S3) = append
Output Mode (MySQL) = update
```

## 🔧 Troubleshooting

### Common Issues

#### 1. NumberFormatException: For input string: "60s"
**Solution**: Ensure you're using compatible Hadoop and AWS SDK versions:
```python
.config('spark.jars.packages', "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026")
```

#### 2. S3 Metadata Error: `_spark_metadata/0 doesn't exist`
**Solution**: Read from partition directories instead of root:
```python
df = spark.read.parquet("s3a://ml-user-events/user-events/event_date_time=*/")
```

#### 3. MySQL Connection Error
**Solution**: Verify MySQL is running and credentials are correct:
```bash
mysql -u spark -p -h localhost
```

#### 4. Kafka Connection Refused
**Solution**: Ensure Kafka is running:
```bash
kafka-topics --list --bootstrap-server localhost:9092
```

### Performance Tuning

- **Reduce file count**: Use `.coalesce(1)` before writing to S3
- **Increase throughput**: Adjust `maxOffsetsPerTrigger`
- **Optimize windows**: Tune watermark and window duration
- **Batch size**: Modify trigger interval for larger batches

## 📈 Monitoring

### Check Kafka Topic
```bash
kafka-console-consumer --topic user_events \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

### Query MySQL Aggregates
```sql
SELECT * FROM user_event_aggregates
ORDER BY window_start DESC
LIMIT 10;
```

### Verify S3 Data
```bash
aws s3 ls s3://ml-user-events/user-events/ --recursive
```

## 🎓 Key Concepts Demonstrated

1. **Structured Streaming**: Real-time data processing with Spark
2. **Watermarking**: Handling late-arriving events
3. **Windowing**: Time-based aggregations
4. **Exactly-Once Semantics**: Checkpointing and idempotent writes
5. **Lambda Architecture**: Batch layer (S3) + Speed layer (MySQL)
6. **Data Partitioning**: Efficient storage and querying
7. **Fault Tolerance**: Automatic recovery from failures

---

**Built with ❤️ using Apache Spark, Kafka, and AWS**