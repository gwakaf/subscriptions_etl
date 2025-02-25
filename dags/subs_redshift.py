from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta
import os
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

# AWS S3 Configuration parameters
S3_BUCKET = "s3://subscriprions-emr-etl"
S3_BUCKET_SOURCE = f"{S3_BUCKET}/data/source"
S3_BUCKET_SSOT = f"{S3_BUCKET}/data/ssot"
S3_BUCKET_PROD = f"{S3_BUCKET}/prod"
subscriptions_data = "subscriptions"
transactions_data = "transaction"
users_data = "users"
price_plans_data = "price_plans"
data_file_extension = ".csv"
LOCAL_SCRIPT_PATH = "/opt/airflow/spark_jobs"
ZIP_FILE_PATH = "/tmp/emr_scripts.zip"
S3_ZIP_FILES = f"s3://{S3_BUCKET}/emr_files/emr_scripts.zip"
host = os.getenv("REDSHIFT_HOST")
port = os.getenv("REDSHIFT_PORT")
dbname = os.getenv("REDSHIFT_DBNAME")
user = os.getenv("REDSHIFT_USER")
password = os.getenv("REDSHIFT_PASSWORD")
redshift_iam_role = os.getenv("REDSHIFT_IAM_ROLE")


@dag(
    start_date=datetime(2025, 1, 20),
    schedule="0 3 * * *",
    tags=['subs_emr_pipeline'],
    catchup=False
)
def subs_redshift():
    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_etl",
        external_dag_id="subs_emr_pipeline",
        mode="poke",
        timeout=3600,
        poke_interval=300,
        dag=dag,
    )
    
    create_redshift_table = RedshiftDataOperator(
        task_id="create_redshift_table",
        database="your_redshift_db",
        sql="""
            CREATE TABLE IF NOT EXISTS your_table (
                id BIGINT PRIMARY KEY,
                username VARCHAR(50),
                post_content VARCHAR(500),
                created_at TIMESTAMP
            )
            DISTSTYLE AUTO;
        """,
        cluster_identifier="your-cluster-id",
        aws_conn_id="aws_default",
        dag=dag,
    )
    
    copy_to_redshift = RedshiftDataOperator(
        task_id="copy_to_redshift",
        database="your_redshift_db",
        sql="COPY your_table FROM 's3://your-bucket/path/' IAM_ROLE 'your-redshift-role' FORMAT AS PARQUET;",
        cluster_identifier="your-cluster-id",
        aws_conn_id="aws_default",
        dag=dag,
    )
    
    
    wait_for_etl >> create_redshift_table >> copy_to_redshift    
    
    
    