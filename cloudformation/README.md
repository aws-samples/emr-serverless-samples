# EMR Serverless CloudFormation Templates

Several templates are included in this repository depending on your use-case.

1. [`emr_serverless_full_deployment.yaml`](./emr_serverless_full_deployment.yaml) EMR Serverless dependencies and Spark application - Creates the necessary IAM roles, an S3 bucket for logging, and a sample Spark 3.2 application.
2. [`emr_serverless_spark_app.yaml`](./emr_serverless_spark_app.yaml) EMR Serverless Spark application - A simple Spark 3.2 Application with pre-defined capacity
3. [`mwaa_emr_serverless.yaml`](./mwaa_emr_serverless.yaml) Amazon Managed Workflows for Apache Airflow (MWAA) and EMR Serverless environment - Everything you need to run EMR Serverless jobs in MWAA.

## Running a sample app

In order to run an EMR Serverless job, you need at least an Application ID and an IAM execution role. We'll also use an S3 bucket below for our Spark logs. 

The first CloudFormation stack above creates all of those resources for you with a job execution role that allows Glue Catalog access, read access to any S3 bucket, and write access to your S3 logs bucket.

Replace the `$APPLICATION_ID`, `$S3_BUCKET`, `$JOB_ROLE_ARN` values below with the values in the `Outputs` tab of your CloudFormation stack, or your own resources that you created. You can find more information in the docs on [getting started with EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html).

```
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "local:///usr/lib/spark/examples/src/main/python/pi.py"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'${S3_BUCKET}'/logs/"
            }
        }
    }'
```

You can retrieve the status of your job using `aws emr-serverless get-job-run` and see logs as the job is running in the following S3 location:

```
s3://${S3_BUCKET}/logs/applications/${APPLICATION_ID}/jobs/${JOB_RUN_ID}/SPARK_DRIVER/
```

You can also use the [EMR Serverless Console](https://console.aws.amazon.com/emr/home#/serverless) to manage your applications and monitor your jobs.

## Cleanup

Once you've run your sample job. Make sure to stop your application and remove any objects from your S3 bucket. You can then delete the CloudFormation stack.

```
aws emr-serverless stop-application --application-id $APPLICATION_ID
```