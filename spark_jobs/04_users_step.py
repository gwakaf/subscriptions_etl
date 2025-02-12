from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
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
users_data = "subscriptions"

batch_date = sys.argv[1]  # {{ ds_nodash }}
batch_date_dt = datetime.strptime(batch_date, "%Y%m%d")
latest_date_dt = batch_date_dt - timedelta(days=1)
latest_date = latest_date_dt.strftime("%Y%m%d")

users_file_path_incoming =f"{S3_BUCKET_SSOT}/{users_data}_{batch_date}.csv"
users_file_path_existed = f"{S3_BUCKET_PROD}/{users_data}/dt={latest_date}"
users_file_path_latest = f"{S3_BUCKET_PROD}/{users_data}/dt={batch_date}"

users_schema_incoming = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("price_plan_id", IntegerType(), True),
    StructField("subs_id", IntegerType(), True),
    StructField("subs_start_timestamp", TimestampType(), True),
    StructField("subs_end_timestamp", TimestampType(), True),
    StructField("create_timestamp", TimestampType(), True),
    StructField("update_timestamp", TimestampType(), True),
    StructField("subs_cancel_timestamp", StringType(), True)
])

users_schema_existed = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("price_plan_id", IntegerType(), True),
    StructField("subs_id", IntegerType(), True),
    StructField("subs_start_timestamp", TimestampType(), True),
    StructField("subs_end_timestamp", TimestampType(), True),
    StructField("create_timestamp", TimestampType(), True),
    StructField("update_timestamp", TimestampType(), True),
    StructField("subs_cancel_timestamp", TimestampType(), True),
    StructField("eff_start_date", DateType(), True),
    StructField("eff_end_date", DateType(), True)
])

# Read incoming new data
users_df_incoming = spark.read.csv(users_file_path_incoming,
                                schema=users_schema_incoming,
                                header=True)

# Load Existing Table from AWS Glue Catalog 
users_df_existing = spark.read.format("parquet").load(users_file_path_existed)

# Add Effective Start and End Dates to Incoming Data (For New Records)
users_df_incoming = users_df_incoming.withColumn("eff_start_date", batch_date_dt) \
    .withColumn("eff_end_date", F.lit(None).cast("date"))
    
# Union existing and incoming dataframes
df_union = users_df_existing.unionByName(users_df_incoming)

# Define Window filter changed records 
window_row_num = Window.partitionBy("user_id", "subs_id").orderBy(F.col("subs_start_timestamp").desc())

# Apply Window Function to assign row numbers
df_ranked = df_union.withColumn("row_number", F.row_number().over(window_row_num))

# Apply filter to get the latest active records
users_df_latest = df_ranked.filter((F.col("row_number") == 1) & F.col("subs_cancel_timestamp").isNull()) \
    .drop("row_number")

# Apply filter to get historical and inactive records    
users_df_historic = df_ranked.filter((F.col("row_number") > 1) | F.col("subs_cancel_timestamp").isNotNull()) \
    .drop("row_number")
    
# Update effective end date for historical records
users_df_historic = users_df_historic.withColumn("eff_end_date", batch_date_dt)


# Write the latest snapshot data to S3 storage
users_df_latest.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("price_plan_id") \
    .save(users_file_path_latest)

# Update historic snapshot data on S3 storage    
users_df_historic.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("price_plan_id") \
    .save(users_file_path_existed)


