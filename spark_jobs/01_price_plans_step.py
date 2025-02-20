from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
import sys
from datetime import datetime, timedelta
import config

spark = SparkSession.builder \
    .appName("media_app_subscriptions") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    batch_date = sys.argv[1] # {{ ds_nodash }}
except Exception as e:
    print(f"Error with an input execution date parameter. Error {e}")
    sys.exit(1)
    
batch_date_dt = datetime.strptime(batch_date, "%Y%m%d")
latest_date_dt = batch_date_dt - timedelta(days=1)
latest_date = latest_date_dt.strftime("%Y%m%d")

price_plans_data = "price_plans"
price_plans_file_path_incoming =f"{config.S3_BUCKET_SSOT}/{price_plans_data}_{batch_date}.csv"
price_plans_file_path_latest = f"{config.S3_BUCKET_PROD}/{price_plans_data}/dt={batch_date}"

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