from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("media_app_subscriptions") \
    .enableHiveSupport() \
    .getOrCreate()
    
S3_BUCKET = "s3://subscriprions-emr-etl"
S3_BUCKET_SSOT = f"{S3_BUCKET}/data/ssot"
S3_BUCKET_PROD = f"{S3_BUCKET}/prod"
price_plans_data = "price_plans"

batch_date = sys.argv[1]  # {{ ds_nodash }}
batch_date_dt = datetime.strptime(batch_date, "%Y%m%d")
latest_date_dt = batch_date_dt - timedelta(days=1)
latest_date = latest_date_dt.strftime("%Y%m%d")

price_plans_file_path_incoming =f"{S3_BUCKET_SSOT}/{price_plans_data}_{batch_date}.csv"
price_plans_file_path_latest = f"{S3_BUCKET_PROD}/{price_plans_data}/dt={batch_date}"

# Because price  plans do not change often, and business always want the latest price plan data
# SCD type 1 applied to this dataset

price_plans_schema = StructType([
    StructField("price_plan_id", IntegerType(), True),
    StructField("price_plan_name", StringType(), True),
    StructField("price", DecimalType(), True),
    StructField("payment_period", StringType(), True),
    StructField("price_plan_type", StringType(), True)
])

# Read incoming new data
price_plans_df = spark.read.csv(price_plans_file_path_incoming,
                                schema=price_plans_schema,
                                header=True)

# Write the latest data to S3 storage
price_plans_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(price_plans_file_path_latest)