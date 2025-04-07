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
        spark = SparkSession.builder.appName("CarRentalLocationVehicleKPI").getOrCreate()
        
        logger.info("Defining schemas")
        # vehicles schema
        vehicles_schema = StructType([
            StructField("active", IntegerType(), True),
            StructField("vehicle_license_number", IntegerType(), True),
            StructField("registration_name", StringType(), True),
            StructField("license_type", StringType(), True),
            StructField("expiration_date", StringType(), True),
            StructField("permit_license_number", StringType(), True),
            StructField("certification_date", StringType(), True),
            StructField("vehicle_year", IntegerType(), True),
            StructField("base_telephone_number", StringType(), True),
            StructField("base_address", StringType(), True),
            StructField("vehicle_id", StringType(), True),
            StructField("last_update_timestamp", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("vehicle_type", StringType(), True),
        ])

        # locations schema
        locations_schema = StructType([
            StructField("location_id", StringType(), True),
            StructField("location_name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
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
        vehicles_df = spark.read.csv("s3://your-bucket/raw/vehicles.csv", header=True, schema=vehicles_schema)
        locations_df = spark.read.csv("s3://your-bucket/raw/locations.csv", header=True, schema=locations_schema)
        rental_transactions_df = spark.read.csv("s3://your-bucket/raw/rental_transactions.csv", header=True, schema=rental_transactions_schema)

        logger.info("Converting date strings to proper formats")
        users_df = users_df.withColumn("driver_license_expiry", to_date(col("driver_license_expiry"), "yyyy-MM-dd"))
        users_df = users_df.withColumn("creation_date", to_date(col("creation_date"), "yyyy-MM-dd"))
        vehicles_df = vehicles_df.withColumn("expiration_date", to_date(col("expiration_date"), "dd-MM-yyyy"))
        vehicles_df = vehicles_df.withColumn("certification_date", to_date(col("certification_date"), "yyyy-MM-dd"))
        vehicles_df = vehicles_df.withColumn("last_update_timestamp", to_timestamp(col("last_update_timestamp"), "dd-MM-yyyy HH:mm:ss"))
        rental_transactions_df = rental_transactions_df.withColumn("rental_start_time", to_timestamp(col("rental_start_time"), "yyyy-MM-dd HH:mm:ss"))
        rental_transactions_df = rental_transactions_df.withColumn("rental_end_time", to_timestamp(col("rental_end_time"), "yyyy-MM-dd HH:mm:ss"))

        logger.info("Joining rental_transactions, vehicles and locations")
        joined_df = rental_transactions_df.join(vehicles_df, rental_transactions_df.vehicle_id == vehicles_df.vehicle_id, "left").drop(vehicles_df.vehicle_id)
        joined_df = joined_df.join(locations_df, joined_df.pickup_location == locations_df.location_id, "left")

        logger.info("Calculating the rental duration in hours")
        joined_df = joined_df.withColumn(
            "rental_duration_hours",
            (unix_timestamp(to_timestamp("rental_end_time", "yyyy-MM-dd HH:mm:ss")) -
            unix_timestamp(to_timestamp("rental_start_time", "yyyy-MM-dd HH:mm:ss"))
            ) / 3600
        )

        logger.info("Calculating Location Performance Metrics")
        location_metrics = joined_df.groupBy("pickup_location", "location_name", "city", "state") \
            .agg(
                sum("total_amount").alias("total_revenue"),
                count("rental_id").alias("total_transactions"),
                round(avg("total_amount"),2).alias("avg_transaction_amount"),
                max("total_amount").alias("max_transaction_amount"),
                min("total_amount").alias("min_transaction_amount"),
                countDistinct("vehicle_id").alias("unique_vehicles_used")
            )

        logger.info("Calculating Vehicle Type Performance Metrics")
        vehicle_type_metrics = joined_df.groupBy("vehicle_type") \
            .agg(
                count("rental_id").alias("rental_count"),
                sum("total_amount").alias("total_revenue"),
                round(avg("total_amount"),2).alias("avg_revenue"),
                sum("rental_duration_hours").alias("total_rental_hours"),
                round(avg("rental_duration_hours"),2).alias("avg_rental_duration_hours")
            )

        logger.info("Writing results to S3")
        location_metrics.write.parquet("s3://your-bucket/processed/location_metrics/", mode="overwrite")
        vehicle_type_metrics.write.parquet("s3://your-bucket/processed/vehicle_type_metrics/", mode="overwrite")

        logger.info("Job completed successfully")
        spark.stop()
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        if 'spark' in locals():
            spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()