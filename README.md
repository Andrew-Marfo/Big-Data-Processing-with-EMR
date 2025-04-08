# Car Rental Marketplace Data Pipeline

## Overview
This project implements a big data processing pipeline for a car rental marketplace using AWS services: EMR (Elastic MapReduce), Spark, Glue, Athena, and Step Functions. The pipeline ingests raw data from Amazon S3, transforms it using Spark to derive key business metrics, and makes it available for analysis via Athena. This documentation covers both the technical setup and the educational objectives of the project.

### Workflow
1. **Raw Data Extraction**: Data is extracted from S3 buckets.
2. **EMR Processing**: Spark jobs process the data on an EMR cluster.
3. **Data Storage**: Transformed data is loaded back to S3 in Parquet format.
4. **Glue Cataloging**: AWS Glue Crawler catalogs the processed data.
5. **Athena Analysis**: SQL queries are executed in Athena for insights.
6. **Orchestration**: AWS Step Functions orchestrates the entire workflow.

## Objectives
- Understand AWS EMR and its role in a big data ecosystem.
- Process raw data from S3 using Spark on EMR.
- Transform datasets to derive key business metrics.
- Use AWS Glue Crawlers and Athena to analyze processed data.
- Automate data workflows with AWS Step Functions.

## Prerequisites
- AWS account with appropriate permissions.
- S3 buckets for raw data, processed data, logs, and query results.
- IAM roles: `EMR_DefaultRole` and `EMR_EC2_DefaultRole`.
- AWS Glue Crawler configured.
- Athena workgroup created.

## Data
The pipeline processes four datasets stored in S3 in raw CSV format:
- **vehicles.csv**: Details of all available rental vehicles.
- **locations.csv**: Master data for all rental locations.
- **users.csv**: User sign-up information.
- **rental_transactions.csv**: Records of vehicle rentals, including:
  - Rental start and end times.
  - Pickup and drop-off locations.
  - Vehicle ID.
  - Total amount paid.

## Pipeline Components
### 1. AWS Step Functions State Machine
Orchestrates the workflow with these states:
- **CreateEMRCluster**: Creates an EMR cluster with Spark and Hive.
- **ProcessEMRJobs**: Runs parallel Spark jobs:
  - Location and Vehicle Performance Metrics.
  - User and Transaction Analysis.
- **RunGlueCrawler**: Triggers the Glue Crawler.
- **ExecuteAthenaQueries**: Runs analytical queries:
  - Highest revenue locations.
  - Most rented vehicle types.
  - Top spending users.
- **TerminateCluster**: Shuts down the EMR cluster.

#### Error Handling
- Automatic cluster termination on failure.
- Retry logic for crawler operations.
- Detailed error logging.

### 2. Spark Jobs
#### Location and Vehicle Performance Metrics (`location_vehicle_kpi.py`)
- **Inputs**:
  - `s3://<raw-data-bucket>/raw_data/vehicles.csv`
  - `s3://<raw-data-bucket>/raw_data/locations.csv`
  - `s3://<raw-data-bucket>/raw_data/rental_transactions.csv`
- **Outputs**:
  - `s3://<processed-data-bucket>/processed_data/location_performance/parquet/`
  - `s3://<processed-data-bucket>/processed_data/vehicle_type_performance/parquet/`
- **Key Metrics Calculated**:
  - Revenue per location.
  - Transactions per location.
  - Average, max, and min transaction amounts.
  - Unique vehicles per location.
  - Rental duration and revenue by vehicle type.

#### User and Transaction Analysis (`user_transaction_kpi.py`)
- **Inputs**:
  - `s3://<raw-data-bucket>/raw_data/users.csv`
  - `s3://<raw-data-bucket>/raw_data/rental_transactions.csv`
- **Outputs**:
  - `s3://<processed-data-bucket>/processed_data/daily_transaction/parquet/`
  - `s3://<processed-data-bucket>/processed_data/user_engagement/parquet/`
- **Key Metrics Calculated**:
  - Daily transactions and revenue.
  - User spending patterns.
  - Rental duration metrics.
  - Maximum and minimum transaction amounts.
  - Total rental hours per user.

### 3. AWS Services Configuration
#### EMR Cluster Configuration
- **Release Label**: `emr-6.9.0`
- **Applications**: Spark, Hive
- **Instance Types**: `m5.xlarge`
- **Nodes**: 1 Master, 2 Core
- **Spark Configuration**:
  - Driver memory: 4GB
  - Executor memory: 4GB
  - Executor cores: 2
- **Logging**: Enabled with aggregation, stored in `s3://<logs-bucket>/emr-logs/`.

#### Glue Crawler
- **Name**: `crawl_rentals_processed_data`
- **Target**: Processed data in `s3://<processed-data-bucket>/processed_data/`.
- **Purpose**: Creates/updates the Glue Data Catalog (e.g., `car_rental_db`).

#### Athena Queries
- **Workgroup**: `primary`
- **Output Location**: `s3://<query-results-bucket>/query-results/`
- **Sample Queries**:
  - Top revenue locations.
  - Most popular vehicle types.
  - Highest spending users.

## Setup Instructions
### Step 1: Upload Data to S3
- Create buckets: `raw-data-bucket`, `processed-data-bucket`, `logs-bucket`, `query-results-bucket`.
- Upload CSV files to `s3://<raw-data-bucket>/raw_data/`:
  - `vehicles.csv`
  - `locations.csv`
  - `users.csv`
  - `rental_transactions.csv`

### Step 2: Upload Spark Scripts
- Place `location_vehicle_kpi.py` and `user_transaction_kpi.py` in `s3://<scripts-bucket>/scripts/`.

### Step 3: Configure Glue Crawler
- Set up crawler to target `s3://<processed-data-bucket>/processed_data/`.
- Configure database (e.g., `car_rental_db`).

### Step 4: Deploy Step Functions
- Create state machine using the provided JSON definition.
- Update bucket names in the configuration.
- Set appropriate IAM permissions.

## Execution
1. **Start Pipeline**:
   - Execute the Step Functions state machine via the AWS Console.
   - Monitor progress in the AWS Console.
2. **Expected Outputs**:
   - Processed Parquet files in `s3://<processed-data-bucket>/processed_data/`.
   - Updated Glue Data Catalog.
   - Athena query results in `s3://<query-results-bucket>/query-results/`.
   - Cluster termination upon completion.

## Key Performance Indicators (KPIs)
### Location and Vehicle Performance
- Total revenue per location.
- Total transactions per location.
- Average, max, and min transaction amounts per location.
- Unique vehicles used per location.
- Rental duration metrics by vehicle type.

### User and Transaction Metrics
- Total daily transactions and revenue.
- Average transaction value.
- User engagement metrics (total transactions, total revenue per user).
- Max and min spending per user.
- Total rental hours per user.

## Monitoring and Troubleshooting
### Logging Locations
- **EMR Cluster Logs**: `s3://<logs-bucket>/emr-logs/`
- **Spark Application Logs**: Included in EMR logs.
- **Step Functions Execution History**: Available in AWS Console.
- **Athena Query Results**: `s3://<query-results-bucket>/query-results/`

### Common Issues and Solutions
1. **Cluster Creation Failure**:
   - Verify IAM roles exist.
   - Check EC2 instance limits.
   - Validate subnet configurations.
2. **Spark Job Failures**:
   - Check EMR step logs.
   - Verify input data exists in S3.
   - Validate file formats and schemas.
3. **Glue Crawler Issues**:
   - Verify crawler has permissions to access S3.
   - Check processed data exists before running crawler.
   - Review crawler logs for schema inference errors.
4. **Athena Query Failures**:
   - Verify tables exist in Glue Data Catalog.
   - Check table names in queries match catalog.
   - Validate output bucket permissions.

## Maintenance
### Cost Optimization
- Monitor and adjust cluster size.
- Set appropriate auto-termination policies.
- Clean up unnecessary S3 data.

### Updates
- Review and update Spark scripts as needed.
- Adjust metrics calculations based on business needs.
- Update Athena queries for new analytical requirements.

## Sample Queries for Analysis
```sql
-- Top 5 revenue-generating locations
SELECT location_name, city, state, total_revenue
FROM car_rental_db.location_performance
ORDER BY total_revenue DESC
LIMIT 5;

-- Most active vehicle types
SELECT vehicle_type, rental_count
FROM car_rental_db.vehicle_type_performance
ORDER BY rental_count DESC
LIMIT 5;

-- User engagement metrics (Top 10 spenders)
SELECT first_name, last_name, total_user_transactions, total_user_spending
FROM car_rental_db.user_engagement
ORDER BY total_user_spending DESC
LIMIT 10;