import sys
from datetime import datetime, timedelta

try:
    batch_date = sys.argv[1] # {{ ds_nodash }}
except Exception as e:
    print(f"Error with an input execution date parameter. Error {e}")
    sys.exit(1)
    
batch_date_dt = datetime.strptime(batch_date, "%Y%m%d")
latest_date_dt = batch_date_dt - timedelta(days=1)
latest_date = latest_date_dt.strftime("%Y%m%d")

S3_BUCKET = "s3://subscriprions-emr-etl"
S3_BUCKET_SSOT = f"{S3_BUCKET}/data/ssot"
S3_BUCKET_PROD = f"{S3_BUCKET}/prod"

