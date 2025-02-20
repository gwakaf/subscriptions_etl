from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DecimalType
from pyspark.sql import functions as F
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
    
transactions_data = "transactions"
transactions_file_path_incoming =f"{config.S3_BUCKET_SSOT}/{transactions_data}_{batch_date}.csv"
transactions_file_path_existed = f"{config.S3_BUCKET_PROD}/{transactions_data}/dt={latest_date}"
transactions_file_path_latest = f"{config.S3_BUCKET_PROD}/{transactions_data}/dt={batch_date}"


transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("subs_id", IntegerType(), True),
    StructField("price_plan_id", IntegerType(), True),
    StructField("transaction_amount", DecimalType(), True),
    StructField("transaction_created_timestamp", TimestampType(), True),
    StructField("transaction_updated_timestamp", TimestampType(), True),
    StructField("transaction_status", StringType(), True)
])


# Read incoming new data
transactions_df = spark.read.csv(transactions_file_path_incoming,
                                schema=transactions_schema,
                                header=True)

# Write the data to S3 storage
transactions_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("price_plan_id") \
    .save(transactions_file_path_latest)