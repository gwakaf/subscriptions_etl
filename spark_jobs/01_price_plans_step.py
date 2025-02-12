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
price_plans_file_path_existed = f"{S3_BUCKET_PROD}/{price_plans_data}/dt={latest_date}"
price_plans_file_path_latest = f"{S3_BUCKET_PROD}/{price_plans_data}/dt={batch_date}"

price_plans_schema_incoming = StructType([
    StructField("price_plan_id", IntegerType(), True),
    StructField("price_plan_name", StringType(), True),
    StructField("price", DecimalType(), True),
    StructField("payment_period", StringType(), True),
    StructField("price_plan_type", StringType(), True)
])

price_plans_schema_existed = StructType([
    StructField("price_plan_id", IntegerType(), True),
    StructField("price_plan_name", StringType(), True),
    StructField("price", DecimalType(), True),
    StructField("payment_period", StringType(), True),
    StructField("price_plan_type", StringType(), True),
    StructField("eff_start_date", DateType(), True),
    StructField("eff_end_date", DateType(), True)
])

# Read incoming new data
price_plans_df_incoming = spark.read.csv(price_plans_file_path_incoming,
                                schema=price_plans_schema_incoming,
                                header=True)

# Load Existing Table from AWS Glue Catalog 
price_plans_df_existing = spark.read.format("parquet").load(price_plans_file_path_existed)

# Add Effective Start and End Dates to Incoming Data (For New Records)
subscriptions_df_incoming = price_plans_df_incoming.withColumn("eff_start_date", batch_date_dt) \
    .withColumn("eff_end_date", F.lit(None).cast("date"))

# Union existing and incoming dataframes
df_union = price_plans_df_existing.unionByName(price_plans_df_incoming)

# Define Window filter changed records 
window_row_num = Window.partitionBy("user_id", "subs_id").orderBy(F.col("subs_start_timestamp").desc())

# Apply Window Function to assign row numbers
df_ranked = df_union.withColumn("row_number", F.row_number().over(window_row_num))

# Apply filter to get the latest active records
price_plans_df_latest = df_ranked.filter((F.col("row_number") == 1) & F.col("subs_cancel_timestamp").isNull()) \
    .drop("row_number")

# Apply filter to get historical and inactive records    
price_plans_df_historic = df_ranked.filter((F.col("row_number") > 1) | F.col("subs_cancel_timestamp").isNotNull()) \
    .drop("row_number")
    
# Update effective end date for historical records
price_plans_df_historic = price_plans_df_historic.withColumn("eff_end_date", batch_date_dt)


# Write the latest snapshot data to S3 storage
price_plans_df_latest.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("price_plan_id") \
    .save(price_plans_file_path_latest)

# Update historic snapshot data on S3 storage    
price_plans_df_historic.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("price_plan_id") \
    .save(price_plans_file_path_existed)