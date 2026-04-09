import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp
from delta.tables import DeltaTable

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
                 .dropDuplicates(["order_id"]) \
                 .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
                 .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))

# 4. Load: Write to the Silver layer in Delta format (Upsert/Merge)
# ------------------------------------------------------------------------------
# 4.1. If it's the first run and the table DOES NOT exist, create it in Delta format
if not DeltaTable.isDeltaTable(spark, silver_path):
    print("Creating Delta table for the first time...")
    clean_df.write.format("delta").save(silver_path)

# 4.2. If the table ALREADY exists, perform a transactional MERGE
else:
    print("Performing incremental MERGE with new data...")
    delta_table = DeltaTable.forPath(spark, silver_path)
    
    delta_table.alias("historical_table") \
        .merge(
            clean_df.alias("new_daily_data"),
            "historical_table.order_id = new_daily_data.order_id" # The Key Column
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

print("Data transformation and Delta MERGE completed successfully.")

job.commit()