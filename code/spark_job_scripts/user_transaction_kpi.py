#!/usr/bin/env python
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    try:
        logger.info("Initializing Spark session")
        spark = SparkSession.builder.appName("CarRentalUserTransactionKPI").getOrCreate()
        
        logger.info("Defining schemas")
        # users schema
        users_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("driver_license_number", StringType(), True),
            StructField("driver_license_expiry", StringType(),True),
            StructField("creation_date", StringType(),True),
            StructField("is_active", IntegerType(),True),
        ])

        # rental locations schema
        rental_transactions_schema = StructType([
            StructField("rental_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("vehicle_id", StringType(), True),
            StructField("rental_start_time", StringType(), True),
            StructField("rental_end_time", StringType(), True),
            StructField("pickup_location", IntegerType(), True),
            StructField("dropoff_location", IntegerType(), True),
            StructField("total_amount", DoubleType(), True)
        ])

        logger.info("Loading data from S3")
        users_df = spark.read.csv("s3://your-bucket/raw/users.csv", header=True, schema=users_schema)
        rental_transactions_df = spark.read.csv("s3://your-bucket/raw/rental_transactions.csv", header=True, schema=rental_transactions_schema)

        logger.info("Converting date strings to proper formats")
        users_df = users_df.withColumn("driver_license_expiry", to_date(col("driver_license_expiry"), "yyyy-MM-dd"))
        users_df = users_df.withColumn("creation_date", to_date(col("creation_date"), "yyyy-MM-dd"))
        rental_transactions_df = rental_transactions_df.withColumn("rental_start_time", to_timestamp(col("rental_start_time"), "yyyy-MM-dd HH:mm:ss"))
        rental_transactions_df = rental_transactions_df.withColumn("rental_end_time", to_timestamp(col("rental_end_time"), "yyyy-MM-dd HH:mm:ss"))

        logger.info("Joining transactions with users")
        user_transactions = rental_transactions_df.join(
            users_df,
            rental_transactions_df.user_id == users_df.user_id,
            "left"
        ).drop(users_df.user_id)

        logger.info("Extracting date from rental_start_time")
        user_transactions_with_date = user_transactions.withColumn(
            "rental_date",
            to_date(to_timestamp("rental_start_time", "yyyy-MM-dd HH:mm:ss"))
        )

        logger.info("Calculating rental duration in hours for user metrics")
        user_transactions_with_duration = user_transactions_with_date.withColumn(
            "rental_duration_hours",
            (
                unix_timestamp(to_timestamp("rental_end_time", "yyyy-MM-dd HH:mm:ss")) -
                unix_timestamp(to_timestamp("rental_start_time", "yyyy-MM-dd HH:mm:ss"))
            ) / 3600
        )

        logger.info("Calculating Daily Transaction Metrics")
        daily_metrics = user_transactions_with_date.groupBy("rental_date") \
            .agg(
                count("rental_id").alias("total_daily_transactions"),
                sum("total_amount").alias("total_daily_revenue"),
                round(avg("total_amount"),2).alias("avg_daily_transaction_value")
            )

        logger.info("Calculating User Engagement Metrics")
        user_metrics = user_transactions_with_duration.groupBy("user_id", "first_name", "last_name") \
            .agg(
                count("rental_id").alias("total_user_transactions"),
                sum("total_amount").alias("total_user_spending"),
                round(avg("total_amount"),2).alias("avg_user_transaction_value"),
                max("total_amount").alias("max_user_transaction"),
                min("total_amount").alias("min_user_transaction"),
                sum("rental_duration_hours").alias("total_user_rental_hours")
            )

        logger.info("Writing results to S3")
        daily_metrics.write.parquet("s3://your-bucket/processed/daily_metrics/", mode="overwrite")
        user_metrics.write.parquet("s3://your-bucket/processed/user_metrics/", mode="overwrite")

        logger.info("Job completed successfully")
        spark.stop()
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        if 'spark' in locals():
            spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()