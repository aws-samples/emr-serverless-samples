# EMR Serverless Airflow Examples

As of `apache-airflow-providers-amazon==5.0.0`, the EMR Serverless Operator is now part of the official [Apache Airflow Amazon Provider](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/index.html) and has been tested with open source Apache Airflow v2.2.2.

> **Warning** The operator in this repository is no longer maintained.

## Amazon Managed Workflows for Apache Airflow (MWAA)

Amazon MWAA supports Airflow versions v2.2.2 and v2.4.3. As of release 6.1.0, the Amazon provider requires Airflow >= 2.3.0.

Depending on the version of Airflow used in MWAA, the `requirements.txt` will look similar to this.

```
apache-airflow-providers-amazon==6.0.0
boto3>=1.23.9
```

> **Note** `boto3>=1.23.9` is required for EMR Serverless support

## Example DAGs

There are two example DAGs in this repository. They make use of [Variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) for relevant job roles, EMR Serverless application IDs, and S3 log buckets.

- [example_emr_serverless.py](./dags/example_emr_serverless.py) - Runs a simple Spark job on an already-created EMR Serverless appliction.
- [example_end_to_end.py](./dags/example_end_to_end.py) - Creates an EMR Serverless Spark application, runs two independent jobs, and deletes the application.

The second example is useful if you want to have a completely ephemeral EMR Serverless environment. When you delete the application, it no longer shows up in the AWS Console nor are you able to access the Spark UI in the console for the jobs. However, logs can be written to S3 for debugging purposes.

## Testing

I've made a couple simple end-to-end tests that run against a pre-existing EMR Serverless application.

These are only intended to be run prior to a release to ensure Operator stability and completeness.

There are several environment variables that need to be populated, including AWS credentials.

```
# Build the test container
docker build -t emr-serverless-airflow-tests .

# Run the tests
docker run --rm -it \
    -e AIRFLOW_VAR_EMR_SERVERLESS_APPLICATION_ID=00abcdefgh123456 \
    -e AIRFLOW_VAR_EMR_SERVERLESS_JOB_ROLE=arn:aws:iam::123456789012:role/emr-serverless-job-role \
    -e AIRFLOW_VAR_EMR_SERVERLESS_LOG_BUCKET=emr-serverless-log-bucket \
    -eAWS_ACCESS_KEY_ID -eAWS_SECRET_ACCESS_KEY -eAWS_SESSION_TOKEN \
    emr-serverless-airflow-tests
```

If you want to run the tests without rebuilding, you add `-v $(pwd)/tests:/opt/emr/tests` to the docker run command.