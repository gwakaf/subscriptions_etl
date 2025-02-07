from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
from pyspark.sql import functions as F
import sys

spark = SparkSession.builder.appName("media_app_price_plans").getOrCreate()

price_plans_schema = StructType([
    StructField("price_paln_id", IntegerType(), True),
    StructField("price_plan_name", StringType(), True),
    StructField("price", DecimalType(), True),
    StructField("payment_period", StringType(), True),
    StructField("price_plan_type", StringType(), True)
])

price_plans_df = spark.read.csv("s3://emr-dev-exp-079300184175/data/price_plans.csv",
                                schema=price_plans_schema,
                                header=True)

price_plans_df.write.mode("overwrite").parquet("s3://emr-dev-exp-079300184175/output/price_plans/")