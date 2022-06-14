from datetime import datetime

from emr_serverless.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessDeleteApplicationOperator,
    EmrServerlessStartJobOperator,
)

from airflow import DAG

# Replace these with your correct values
APPLICATION_ID = "00f0abcde4fg0001"
JOB_ROLE_ARN = "arn:aws:iam::012345678912:role/emr_serverless_default_role"
S3_LOGS_BUCKET = "my-logs-bucket"

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"s3://{S3_LOGS_BUCKET}/logs/"}
    },
}

with DAG(
    dag_id="example_e2e_emrserverless",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_spark_app", job_type="SPARK", release_label="emr-6.6.0",
        config={"name": "sample-job"}
    )

    application_id = create_app.output

    job1 = EmrServerlessStartJobOperator(
        task_id="start_job_1",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://<YOUR_BUCKET>/code/spark_job1.py",
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job2 = EmrServerlessStartJobOperator(
        task_id="start_job_2",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://<YOUR_BUCKET>/code/spark_job2.py",
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
    )

    (create_app >> [job1, job2] >> delete_app)
