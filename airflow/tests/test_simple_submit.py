from datetime import datetime

from emr_serverless.operators.emr import EmrServerlessStartJobOperator

from airflow.executors.debug_executor import DebugExecutor
from airflow.models import DAG, DagRun, TaskInstance, Variable
from airflow.utils.state import State

APPLICATION_ID = Variable.get("emr_serverless_application_id")
JOB_ROLE_ARN = Variable.get("emr_serverless_job_role")
S3_LOGS_BUCKET = Variable.get("emr_serverless_log_bucket")


def test_start_job():
    with DAG(
        dag_id="test_example_emr_serverless_job",
        schedule_interval="@daily",
        start_date=datetime(2021, 1, 1),
    ) as dag:
        job_starter = EmrServerlessStartJobOperator(
            task_id="start_job",
            application_id=APPLICATION_ID,
            execution_role_arn=JOB_ROLE_ARN,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": "local:///usr/lib/spark/examples/src/main/python/pi.py",
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{S3_LOGS_BUCKET}/logs/"
                    }
                },
            },
            config={"name": "sample-job"},
        )

        dag.clear()
        dag.run(
            executor=DebugExecutor(), start_date=dag.start_date, end_date=dag.start_date
        )

        # Validate DAG run was successful
        dagruns = DagRun.find(dag_id=dag.dag_id, execution_date=dag.start_date)
        assert len(dagruns) == 1
        assert dagruns[0].state == State.SUCCESS

        # Validate the job run was successful
        ti = TaskInstance(job_starter, run_id=dagruns[0].run_id)
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS


def test_job_failure():
    with DAG(
        dag_id="test_example_emr_serverless_job",
        schedule_interval="@daily",
        start_date=datetime(2021, 1, 1),
    ) as dag:
        job_starter = EmrServerlessStartJobOperator(
            task_id="start_job",
            application_id=APPLICATION_ID,
            execution_role_arn=JOB_ROLE_ARN,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": "local:///invalid.py",
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{S3_LOGS_BUCKET}/logs/"
                    }
                },
            },
            config={"name": "job-fail"},
        )

        dag.clear()
        try:
            # The whole DAG run will fail
            dag.run(
                executor=DebugExecutor(),
                start_date=dag.start_date,
                end_date=dag.start_date,
            )
        except:
            pass

        # Validate DAG run failed
        dagruns = DagRun.find(dag_id=dag.dag_id, execution_date=dag.start_date)
        assert len(dagruns) == 1
        assert dagruns[0].state == State.FAILED

        # Validate the job run failed
        ti = TaskInstance(job_starter, run_id=dagruns[0].run_id)
        ti.refresh_from_db()
        assert ti.state == State.FAILED
