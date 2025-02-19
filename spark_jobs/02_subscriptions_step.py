from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config

spark = SparkSession.builder \
    .appName("media_app_subscriptions") \
    .enableHiveSupport() \
    .getOrCreate()
    
subscriptions_data = "subscriptions"
subscriptions_file_path_incoming =f"{config.S3_BUCKET_SSOT}/{subscriptions_data}_{config.batch_date}.csv"
subscriptions_file_path_existed = f"{config.S3_BUCKET_PROD}/{subscriptions_data}/dt={config.latest_date}"
subscriptions_file_path_latest = f"{config.S3_BUCKET_PROD}/{subscriptions_data}/dt={config.batch_date}"

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
    StructField("eff_start_date", DateType(), True),
    StructField("eff_end_date", DateType(), True)
])

# Read incoming new data
subscriptions_df_incoming = spark.read.csv(subscriptions_file_path_incoming,
                                schema=subscriptions_schema_incoming,
                                header=True)

# Load Existing Table from AWS Glue Catalog 
subscriptions_df_existing = spark.read.format("parquet").load(subscriptions_file_path_existed)

# Add Effective Start and End Dates to Incoming Data (For New Records)
subscriptions_df_incoming = subscriptions_df_incoming.withColumn("eff_start_date", config.batch_date_dt) \
    .withColumn("eff_end_date", F.lit(None).cast("date"))
    
# Union existing and incoming dataframes
df_union = subscriptions_df_existing.unionByName(subscriptions_df_incoming)

# Define Window filter changed records 
window_row_num = Window.partitionBy("user_id", "subs_id").orderBy(F.col("subs_start_timestamp").desc())

# Apply Window Function to assign row numbers
df_ranked = df_union.withColumn("row_number", F.row_number().over(window_row_num))

# Apply filter to get the latest active records
subscriptions_df_latest = df_ranked.filter((F.col("row_number") == 1) & F.col("subs_cancel_timestamp").isNull()) \
    .drop("row_number")

# Apply filter to get historical and inactive records    
subscriptions_df_historic = df_ranked.filter((F.col("row_number") > 1) | F.col("subs_cancel_timestamp").isNotNull()) \
    .drop("row_number")
    
# Update effective end date for historical records
subscriptions_df_historic = subscriptions_df_historic.withColumn("eff_end_date", config.batch_date_dt)


# Write the latest snapshot data to S3 storage
subscriptions_df_latest.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("price_plan_id") \
    .save(subscriptions_file_path_latest)

# Update historic snapshot data on S3 storage    
subscriptions_df_historic.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("price_plan_id") \
    .save(subscriptions_file_path_existed)


