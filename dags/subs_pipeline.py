from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta

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

#EMR cluster
job_overflow_overrides = {
    'Name': "emr_subs_pipeline_cluster",
    'ReleaseLabel': "emr-7.2.0",
    'LogUri': "s3n://aws-logs-name",
    'Applications': [
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "JupyterEnterpriseGateway"},
        {"Name": "Livy"},
        {"Name": "Spark"},
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Driver Node',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Worker Nodes',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,  
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EC2_Default_Role',
    'ServiceRole': 'EMR_DefaultRole',
}

#EMR steps

step_01_price_plans = [
    {
        'Name': 'Price Plans Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                "--py-files", S3_ZIP_FILES,
                f's3://{S3_BUCKET}/emr_files/01_price_plans_step.py',
                "{{ ds_nodash }}"],
        },
    }
]
step_02_subscriptions = [
    {
        'Name': 'Subscriptions Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                "--py-files", S3_ZIP_FILES,
                f's3://{S3_BUCKET}/emr_files/02_subscriptions_step.py',
                "{{ ds_nodash }}"
                ],

        },
    }
]
step_03_transactions = [
    {
        'Name': 'Transactions Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                "--py-files", S3_ZIP_FILES,
                f's3://{S3_BUCKET}/emr_files/03_transactions_step.py',
                "{{ ds_nodash }}"],
        },
    }
]
step_04_users = [
    {
        'Name': 'Users Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                "--py-files", S3_ZIP_FILES,
                f's3://{S3_BUCKET}/emr_files/04_users_step.py',
                "{{ ds_nodash }}"],
        },
    }
]

#pre-req: Glue crawler created in AWS console
glue_crawler_config = {
    "Name": "subs_pipeline_crawler",
}

@dag(
    start_date=datetime(2025, 1, 20),
    schedule="0 3 * * *",
    tags=['subs_emr_pipeline'],
    catchup=False
)
def subs_emr_pipeline():
    wait_for_subscriprions_data = S3KeySensor(
        task_id="wait_for_subscriprions_data",
        aws_conn_id="aws_s3",
        bucket_key=f"{S3_BUCKET_SOURCE}/{subscriptions_data}/*",
        wildcard_match=True,
    )
    wait_for_transactions_data = S3KeySensor(
        task_id="wait_for_transactions_data",
        aws_conn_id="aws_s3",
        bucket_key=f"{S3_BUCKET_SOURCE}/{transactions_data}*",
        wildcard_match=True,
    )
    wait_for_users_data = S3KeySensor(
        task_id="wait_for_users_data",
        aws_conn_id="aws_s3",
        bucket_key=f"{S3_BUCKET_SOURCE}/{users_data}*",
        wildcard_match=True,
    )
    wait_for_price_plans_data = S3KeySensor(
        task_id="wait_for_price_plans_data",
        aws_conn_id="aws_s3",
        bucket_key=f"{S3_BUCKET_SOURCE}/{price_plans_data}*",
        wildcard_match=True,
    )
    
    copy_subscriprions_data = S3CopyObjectOperator(
        task_id="copy_subscriprions_data",
        aws_conn_id="aws_s3",
        source_bucket_key=f"{S3_BUCKET_SOURCE}/{subscriptions_data}",
        dest_bucket_key=(
            f"{S3_BUCKET_SSOT}/{subscriptions_data}/"
            f"{subscriptions_data}_{{ ds_nodash }}{data_file_extension}"
        )
    )
    copy_transactions_data = S3CopyObjectOperator(
        task_id="copy_transactions_data",
        aws_conn_id="aws_s3",
        source_bucket_key=f"{S3_BUCKET_SOURCE}/{transactions_data}",
        dest_bucket_key=(
            f"{S3_BUCKET_SSOT}/{transactions_data}/"
            f"{transactions_data}_{{ ds_nodash }}{data_file_extension}"
        )
    )
    copy_users_data = S3CopyObjectOperator(
        task_id="copy_users_data",
        aws_conn_id="aws_s3",
        source_bucket_key=f"{S3_BUCKET_SOURCE}/{users_data}",
        dest_bucket_key=(
            f"{S3_BUCKET_SSOT}/{users_data}/"
            f"{users_data}_{{ ds_nodash }}{data_file_extension}"
        )
    )
    copy_price_plans_data = S3CopyObjectOperator(
        task_id="copy_price_plans_data",
        aws_conn_id="aws_s3",
        source_bucket_key=f"{S3_BUCKET_SOURCE}/{price_plans_data}",
        dest_bucket_key=(
            f"{S3_BUCKET_SSOT}/{price_plans_data}/"
            f"{price_plans_data}_{{ ds_nodash }}{data_file_extension}"
        )
    )
    
    zip_scripts = BashOperator(
        task_id="zip_scripts",
        bash_command=f"cd {LOCAL_SCRIPT_PATH} && zip -r {ZIP_FILE_PATH} ."
    )
    
    upload_emr_scripts = BashOperator(
        task_id="upload_emr_scripts_to_s3",
        bash_command=f"aws s3 cp {ZIP_FILE_PATH} {S3_ZIP_FILES}"
    )
    
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=job_overflow_overrides,
        aws_conn_id="aws_s3",
        wait_for_completion=True
    )
    
    add_price_plans_step = EmrAddStepsOperator(
        task_id="add_01_price_plans_step",
        job_flow_id=create_emr_cluster.output,
        steps=step_01_price_plans,
        aws_conn_id="aws_s3",
        wait_for_completion=True
    )
    add_subscriptions_step = EmrAddStepsOperator(
        task_id="add_02_subscriptions_step",
        job_flow_id=create_emr_cluster.output,
        steps=step_02_subscriptions,
        aws_conn_id="aws_s3",
        wait_for_completion=True
    )
    add_transactions_step = EmrAddStepsOperator(
        task_id="add_03_transactions_step",
        job_flow_id=create_emr_cluster.output,
        steps=step_03_transactions,
        aws_conn_id="aws_s3",
        wait_for_completion=True
    )
    add_users_step = EmrAddStepsOperator(
        task_id="add_04_users_step",
        job_flow_id=create_emr_cluster.output,
        steps=step_04_users,
        aws_conn_id="aws_s3",
        wait_for_completion=True
    )
    
    #ensure termination of EMR cluster after ETL steps
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=create_emr_cluster.output,
        aws_conn_id = 'AWSConnection',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    #run glue crawler to catalogue metadata on this pipeline
    glue_crawler = GlueCrawlerOperator(
        task_id="catalogue_metadata",
        config=glue_crawler_config,
        aws_conn_id="aws_s3"
    )
    
    chain([wait_for_subscriprions_data, wait_for_price_plans_data, wait_for_transactions_data, wait_for_users_data],
          [copy_subscriprions_data, copy_price_plans_data, copy_transactions_data, copy_users_data],
          create_emr_cluster, [add_price_plans_step,add_subscriptions_step,add_transactions_step, add_users_step],
          terminate_emr_cluster, glue_crawler)
    