import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. Initialization
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

# 2. Extract
raw_df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .csv(bronze_path)

# 3. Transform: Cleanse and Deduplicate using Window Functions
print("Starting transformation and deduplication...")

# Step 3.1: Cast timestamps and drop rows with null critical IDs
transformed_df = raw_df.dropna(subset=["order_id", "customer_id"]) \
                       .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
                       .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))

# Step 3.2: Window Function to safely remove duplicates based on order_id
# We partition by order_id and order by purchase timestamp descending to keep the latest record
windowSpec = Window.partitionBy("order_id").orderBy(col("order_purchase_timestamp").desc())

clean_df = transformed_df.withColumn("row_num", row_number().over(windowSpec)) \
                         .filter(col("row_num") == 1) \
                         .drop("row_num")

print(f"Transformation complete. Prepared data for writing.")

# 4. Load: Write to the Silver layer in Delta format (Upsert/Merge)
if not DeltaTable.isDeltaTable(spark, silver_path):
    print("Creating Delta table for the first time...")
    clean_df.write.format("delta").save(silver_path)
else:
    print("Delta table exists. Performing MERGE (Upsert)...")
    delta_table = DeltaTable.forPath(spark, silver_path)
    
    delta_table.alias("historical_table").merge(
        clean_df.alias("new_daily_data"),
        "historical_table.order_id = new_daily_data.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

print("Job completed successfully!")
job.commit()