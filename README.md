# Car Rental Data Processing Pipeline Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture Components](#architecture-components)
3. [Getting Started](#getting-started)
4. [Pipeline Stages](#pipeline-stages)
5. [Troubleshooting Guide](#troubleshooting-guide)
6. [Maintenance Tasks](#maintenance-tasks)
7. [Resource Requirements](#resource-requirements)

## Overview
This pipeline processes car rental marketplace data using AWS EMR, Glue, and Athena to generate business insights. The workflow is orchestrated using AWS Step Functions.

## Architecture Components

### Data Flow
- **Source**: Raw data in S3 (`s3://{YOUR_PREFIX}-raw-data/`)
- **Processing**: EMR cluster running Spark jobs
- **Output**: Processed data in S3 (`s3://{YOUR_PREFIX}-processed-data/`)
- **Analytics**: Athena queries for business insights

### Core Components
- AWS EMR Cluster (EMR 6.9.0)
- AWS Step Functions
- AWS Glue Crawler
- Amazon Athena
- Amazon S3

## Getting Started

### Prerequisites
- AWS Account with appropriate permissions
- AWS CLI configured
- Required IAM roles:
  - EMR_DefaultRole
  - EMR_EC2_DefaultRole
  - AWSGlueServiceRole
  - StepFunctionsExecutionRole

### 1. S3 Bucket Setup

Create required S3 buckets:
```bash
# Replace {YOUR_PREFIX} with your chosen prefix
aws s3 mb s3://{YOUR_PREFIX}-raw-data
aws s3 mb s3://{YOUR_PREFIX}-processed-data
aws s3 mb s3://{YOUR_PREFIX}-scripts
aws s3 mb s3://{YOUR_PREFIX}-logs
aws s3 mb s3://{YOUR_PREFIX}-athena-results
```

Upload data files:
```bash
aws s3 cp users.csv s3://{YOUR_PREFIX}-raw-data/users.csv
aws s3 cp vehicles.csv s3://{YOUR_PREFIX}-raw-data/vehicles.csv
aws s3 cp locations.csv s3://{YOUR_PREFIX}-raw-data/locations.csv
aws s3 cp rental_transactions.csv s3://{YOUR_PREFIX}-raw-data/rental_transactions.csv
```

Upload Spark scripts:
```bash
aws s3 cp location_vehicle_kpi.py s3://{YOUR_PREFIX}-scripts/location_vehicle_kpi.py
aws s3 cp user_transaction_kpi.py s3://{YOUR_PREFIX}-scripts/user_transaction_kpi.py
```

### 2. Create AWS Glue Crawler
```bash
aws glue create-crawler \
    --name "car-rental-crawler" \
    --role "AWSGlueServiceRole" \
    --database-name "car_rental_db" \
    --targets "{"S3Targets": [{"Path": "s3://{YOUR_PREFIX}-processed-data/"}]}"
```

### 3. Create Step Functions State Machine
```bash
aws stepfunctions create-state-machine \
    --name "CarRentalDataPipeline" \
    --role-arn "arn:aws:iam::{YOUR_ACCOUNT_ID}:role/StepFunctionsExecutionRole" \
    --definition file://step_functions.json
```

### 4. Run the Pipeline
```bash
aws stepfunctions start-execution \
    --state-machine-arn "arn:aws:states:{YOUR_REGION}:{YOUR_ACCOUNT_ID}:stateMachine:CarRentalDataPipeline"
```

### 5. Cleanup
```bash
# Delete all resources when done
aws s3 rb s3://{YOUR_PREFIX}-raw-data --force
aws s3 rb s3://{YOUR_PREFIX}-processed-data --force
aws s3 rb s3://{YOUR_PREFIX}-scripts --force
aws s3 rb s3://{YOUR_PREFIX}-logs --force
aws s3 rb s3://{YOUR_PREFIX}-athena-results --force
aws glue delete-crawler --name "car-rental-crawler"
aws stepfunctions delete-state-machine \
    --state-machine-arn "arn:aws:states:{YOUR_REGION}:{YOUR_ACCOUNT_ID}:stateMachine:CarRentalDataPipeline"
```

## Pipeline Stages

### 1. EMR Cluster Creation
- **Configuration**: 
  - 1 Master node (m5.xlarge)
  - 2 Core nodes (m5.xlarge)
  - Auto-termination after 10 minutes of inactivity

### 2. Data Processing Jobs
a) **Location and Vehicle KPIs** (`location_vehicle_kpi.py`)
   - Processes location performance metrics
   - Generates vehicle type statistics
   - Output: `location_performance_metrics/` and `vehicle_type_performance_metrics/`

b) **User Transaction KPIs** (`user_transaction_kpi.py`)
   - Processes user engagement metrics
   - Generates daily transaction statistics
   - Output: `daily_transaction_metrics/` and `user_engagement_metrics/`

### 3. Data Catalog Update
- Glue Crawler: `car-rental-crawler`
- Updates Glue Data Catalog for Athena queries

### 4. Analytics Queries
Parallel execution of:
- Highest revenue location analysis
- Most rented vehicle type analysis
- Top spending users analysis

### 4 EMR Cluster Termination
- Terminates EMR cluster after all jobs are completed

## Troubleshooting Guide

### Common Issues and Solutions

1. **EMR Cluster Creation Failures**
   - Check IAM roles: `EMR_DefaultRole` and `EMR_EC2_DefaultRole`
   - Verify VPC/subnet configurations
   - Review EMR service logs in `s3://{YOUR_PREFIX}-logs/emr-logs/`

2. **Spark Job Failures**
   - Check Spark job logs in EMR cluster
   - Common causes:
     - Schema mismatches
     - Missing input data
     - S3 permission issues

3. **Glue Crawler Issues**
   - Verify crawler IAM permissions
   - Check S3 bucket permissions
   - Review crawler logs in CloudWatch

4. **Athena Query Failures**
   - Verify table schema in Glue Data Catalog
   - Check S3 output location permissions
   - Review query syntax and table existence

### Monitoring Points
1. **EMR Cluster**
   - CloudWatch metrics for cluster health
   - Spark application progress
   - Resource utilization

2. **Data Quality**
   - Built-in validation checks in Spark jobs
   - Null value monitoring
   - Data volume anomalies

3. **Pipeline State**
   - Step Functions execution status
   - Job completion notifications
   - Error handling paths

## Maintenance Tasks

### Regular Maintenance
1. Update EMR version when needed
2. Review and optimize Spark configurations
3. Monitor S3 storage usage
4. Review IAM roles and permissions

### Best Practices
1. Test changes in development environment first
2. Maintain backup of critical data
3. Regular monitoring of costs
4. Keep documentation updated

## Resource Requirements

### S3 Buckets Structure
```
{YOUR_PREFIX}-raw-data/
├── users.csv
├── vehicles.csv
├── locations.csv
└── rental_transactions.csv

{YOUR_PREFIX}-processed-data/
├── location_performance_metrics/
├── vehicle_type_performance_metrics/
├── daily_transaction_metrics/
└── user_engagement_metrics/

{YOUR_PREFIX}-scripts/
├── location_vehicle_kpi.py
└── user_transaction_kpi.py

{YOUR_PREFIX}-logs/
└── emr-logs/

{YOUR_PREFIX}-athena-results/
```

### Required IAM Roles
- EMR_DefaultRole
- EMR_EC2_DefaultRole
- AWSGlueServiceRole
- StepFunctionsExecutionRole

### Required Permissions
Each role needs specific permissions for:
- S3 bucket access
- EMR cluster management
- Glue crawler operations
- Athena query execution
- CloudWatch logging