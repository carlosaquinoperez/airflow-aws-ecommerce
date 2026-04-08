import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp

# 1. Initialization and Parameter parsing
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bronze_path = f"s3://{args['BRONZE_BUCKET']}/raw/orders/"
silver_path = f"s3://{args['SILVER_BUCKET']}/cleaned/orders/"

print(f"Reading raw data from: {bronze_path}")
print(f"Writing clean data to: {silver_path}")

# 2. Extract: Read the CSV from the Bronze layer
# In a production environment, we read all files dynamically, not just one.
raw_df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .csv(bronze_path)

# 3. Transform: Cleanse and standardize the data
# - Drop rows where critical fields (like order_id) are null
# - Cast string timestamps to actual Timestamp types for analytical querying
clean_df = raw_df.dropna(subset=["order_id", "customer_id"]) \
                 .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
                 .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))

# 4. Load: Write to the Silver layer in Parquet format
# Overwrite mode is used here to maintain idempotency in the staging layer
clean_df.write.mode("overwrite") \
              .parquet(silver_path)

print("Data transformation and load to Silver layer completed successfully.")

job.commit()