from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType, TimestampType
from pyspark.sql import functions as F
import sys
from datetime import datetime

spark = SparkSession.builder \
    .appName("media_app_subscriptions") \
    .enableHiveSupport() \
    .getOrCreate()
    
# AWS Glue Database & Table Names
# glue_database = "your_glue_database"
# glue_table = "subscriptions"

S3_BUCKET = "s3://subscriprions-emr-etl"
S3_BUCKET_SSOT = f"{S3_BUCKET}/data/ssot"
S3_BUCKET_OUTPUT = f"{S3_BUCKET}/output"
subscriptions_data = "subscriptions"
# current_date = datetime.today()
# current_year = current_date.year
# current_month = current_date.month
# current_day = current_date.day
partition_date = sys.argv[1]  # {{ ds_nodash }}

subscriptions_file_path_incoming =f"{S3_BUCKET_SSOT}/{subscriptions_data}_{current_year}{current_month}{current_day}.csv"
subscriptions_file_path_existed = f"{S3_BUCKET_OUTPUT}/{subscriptions_data}"

subscriptions_schema_incoming = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("price_plan_id", IntegerType(), True),
    StructField("subs_id", IntegerType(), True),
    StructField("subs_start_timestamp", TimestampType(), True),
    StructField("subs_end_timestamp", TimestampType(), True),
    StructField("create_timestamp", TimestampType(), True),
    StructField("update_timestamp", TimestampType(), True),
    StructField("subs_cancel_timestamp", StringType(), True)
])

subscriptions_schema_existed = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("price_plan_id", IntegerType(), True),
    StructField("subs_id", IntegerType(), True),
    StructField("subs_start_timestamp", TimestampType(), True),
    StructField("subs_end_timestamp", TimestampType(), True),
    StructField("create_timestamp", TimestampType(), True),
    StructField("update_timestamp", TimestampType(), True),
    StructField("subs_cancel_timestamp", TimestampType(), True),
    StructField("eff_start_timestamp", TimestampType(), True),
    StructField("eff_end_timestamp", TimestampType(), True),
])

subscriptions_df_incoming = spark.read.csv(subscriptions_file_path_incoming,
                                schema=subscriptions_schema_incoming,
                                header=True)

# Load Existing Table from AWS Glue Catalog 
subscriptions_df_existing = spark.read.format("parquet").load(subscriptions_file_path_existed)


# Step 1: Identify Records that Changed (Join on user_id & subs_id)
df_joined = subscriptions_df_existing.alias("existing").join(
    subscriptions_df_incoming.alias("incoming"),
    ["user_id", "subs_id"],
    "outer"
)

# Step 2: Identify changed records (price_plan_id or subs_end_timestamp changed)
df_changed = df_joined.filter(
    (F.col("existing.price_plan_id") != F.col("incoming.price_plan_id")) |
    (F.col("existing.subs_end_timestamp") != F.col("incoming.subs_end_timestamp"))
)

# Step 3: Mark existing records as inactive (set eff_end_timestamp)
df_existing_updates = df_changed.select(
    F.col("existing.user_id"),
    F.col("existing.price_plan_id"),
    F.col("existing.subs_id"),
    F.col("existing.subs_start_timestamp"),
    F.col("existing.subs_end_timestamp"),
    F.col("existing.create_timestamp"),
    F.col("existing.update_timestamp"),
    F.col("existing.subs_cancel_timestamp"),
    F.col("existing.eff_start_timestamp"),
    F.current_timestamp().alias("eff_end_timestamp")  # Mark record as inactive
)

# Step 4: Insert a new version of the updated record
df_new_records = df_changed.select(
    F.col("incoming.user_id"),
    F.col("incoming.price_plan_id"),
    F.col("incoming.subs_id"),
    F.col("incoming.subs_start_timestamp"),
    F.col("incoming.subs_end_timestamp"),
    F.col("incoming.create_timestamp"),
    F.col("incoming.update_timestamp"),
    F.col("incoming.subs_cancel_timestamp"),
    F.current_timestamp().alias("eff_start_timestamp"),  # New start date
    F.lit(None).cast("timestamp").alias("eff_end_timestamp")  # Active record
)

# Step 5: Get unchanged records (not in df_changed)
df_unchanged = subscriptions_df_existing.join(
    df_changed.select("user_id", "subs_id"), ["user_id", "subs_id"], "left_anti"
)

# Step 6: Merge all data (unchanged + updated old + new records)
df_final = df_existing_updates \
    .unionByName(df_new_records, allowMissingColumns=True) \
    .unionByName(df_unchanged, allowMissingColumns=True)


# Step 7: Write Updated Data Back to AWS S3 in Parquet (Partitioned by subs_start_timestamp)
df_final.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("subs_start_timestamp") \
    .save(subscriptions_file_path_existed)

# subscriptions_df.write.partitionBy("create_date").mode("overwrite").parquet("s3://emr-dev-exp-079300184175/output/subscriptions/")
# subscriptions_df.write.mode("overwrite").parquet("s3://emr-dev-exp-079300184175/output/subscriptions/create_date='2023-09-05'")




# subscriptions_df.write.mode("overwrite").parquet("/Users/bilberry/DE/emr-test/output/subscriptions/create_date='2023-09-05'")
# subscriptions_df.write.partitionBy("create_date").mode("overwrite").parquet("/Users/bilberry/DE/emr-test/output/subscriptions")

# s3://emr-dev-exp-079300184175/output/transactions/
# s3://emr-dev-exp-079300184175/output/users/