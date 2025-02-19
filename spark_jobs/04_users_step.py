from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, BooleanType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config

spark = SparkSession.builder \
    .appName("media_app_subscriptions") \
    .enableHiveSupport() \
    .getOrCreate()
    
users_data = "subscriptions"
users_file_path_incoming =f"{config.S3_BUCKET_SSOT}/{users_data}_{config.batch_date}.csv"
users_file_path_existed = f"{config.S3_BUCKET_PROD}/{users_data}/dt={config.latest_date}"
users_file_path_latest = f"{config.S3_BUCKET_PROD}/{users_data}/dt={config.batch_date}"


#user_id,use_name,city,date_od_birth,email,mobile,created_timestamp,updated_timestamps
users_schema_incoming = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("email", StringType(), True),
    StructField("mobile", StringType(), True),
    StructField("user_created_timestamp", TimestampType(), True),
    StructField("user_updated_timestamp", TimestampType(), True),
    StructField("is_subs_active", BooleanType, False)
])

users_schema_existed = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("email", StringType(), True),
    StructField("mobile", StringType(), True),
    StructField("user_created_timestamp", TimestampType(), True),
    StructField("user_updated_timestamp", TimestampType(), True),
    StructField("is_subs_active", BooleanType, False),
    StructField("eff_start_date", DateType(), False),
    StructField("eff_end_date", DateType(), True)
])

# Read incoming new data
users_df_incoming = spark.read.csv(users_file_path_incoming,
                                schema=users_schema_incoming,
                                header=True)

# Load Existing Table from AWS Glue Catalog 
users_df_existing = spark.read.format("parquet").load(users_file_path_existed)

# Add Effective Start and End Dates to Incoming Data (For New Records)
users_df_incoming = users_df_incoming.withColumn("eff_start_date", config.batch_date_dt) \
    .withColumn("eff_end_date", F.lit(None).cast("date"))
    
# Union existing and incoming dataframes
df_union = users_df_existing.unionByName(users_df_incoming)

# Define Window filter changed records 
window_row_num = Window.partitionBy("user_id").orderBy(F.col("user_updated_timestamp").desc())

# Apply Window Function to assign row numbers
df_ranked = df_union.withColumn("row_number", F.row_number().over(window_row_num))

# Apply filter to get the latest active records
users_df_latest = df_ranked.filter((F.col("row_number") == 1) & F.col("is_subs_active")==True) \
    .drop("row_number")

# Apply filter to get historical and inactive records    
users_df_historic = df_ranked.filter((F.col("row_number") > 1) | F.col("is_subs_active")== False) \
    .drop("row_number")
    
# Update effective end date for historical records
users_df_historic = users_df_historic.withColumn("eff_end_date", config.batch_date_dt)


# Write the latest snapshot data to S3 storage
users_df_latest.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("eff_start_date") \
    .save(users_file_path_latest)

# Update historic snapshot data on S3 storage    
users_df_historic.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("eff_start_date") \
    .save(users_file_path_existed)


