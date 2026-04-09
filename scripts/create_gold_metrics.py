import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count

# 1. Initialization
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SILVER_BUCKET', 'GOLD_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

silver_path = f"s3://{args['SILVER_BUCKET']}/cleaned/orders/"
gold_path = f"s3://{args['GOLD_BUCKET']}/metrics/orders_by_status/"

print(f"Reading Silver data from: {silver_path}")

# 2. Extract: Read the clean Delta table from Silver
silver_df = spark.read.format("delta").load(silver_path)

# 3. Transform: Aggregate to create Business Metrics (Gold)
# Group by order_status and count total orders
gold_metrics_df = silver_df.groupBy("order_status") \
                           .agg(count("order_id").alias("total_orders"))

# 4. Load: Write to Gold Layer (Delta Lake)
gold_metrics_df.write.format("delta") \
               .mode("overwrite") \
               .save(gold_path)

print("Gold metrics successfully calculated and saved.")
job.commit()