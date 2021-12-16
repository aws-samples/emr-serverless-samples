# EMR Serverless PySpark job

This example shows how to run a PySpark job on EMR Serverless that analyzes data from the [NOAA Global Surface Summary of Day](https://registry.opendata.aws/noaa-gsod/) dataset from the Registry of Open Data on AWS.

The script analyzes data from a given year and finds the weather location with the most extreme rain, wind, snow, and temperature.

_ℹ️ Throughout this demo, I utilize environment variables to allow for easy copy/paste_

## Setup

_You should have already completed the pre-requisites in this repo's [README](/README.md)._

- Define some environment variables to be used later

```shell
export S3_BUCKET=<YOUR_BUCKET_NAME>
export JOB_ROLE_ARN=arn:aws:iam::<ACCOUNT_ID>:role/emr-serverless-job-role
```

- First, make sure the `extreme_weather.py` script is uploaded to an S3 bucket in the `us-east-1 region.

```shell
aws s3 cp extreme_weather.py s3://${S3_BUCKET}/code/pyspark/
```

- Now, let's create and start an Application on EMR Serverless. Applications are where you submit jobs and are associated with a specific open source framework and release version. For this application, we'll configure [pre-initialized capacity](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-capacity-api.html) to ensure this application can begin running jobs immediately.

_ℹ️ Please note that leaving a pre-initialized application running WILL incur costs in your AWS Account._

```shell
aws emr-serverless create-application \
  --type SPARK \
  --name serverless-demo \
  --release-label "emr-6.5.0-preview" \
    --initial-capacity '{
        "DRIVER": {
            "workerCount": 2,
            "resourceConfiguration": {
                "cpu": "2vCPU",
                "memory": "4GB"
            }
        },
        "EXECUTOR": {
            "workerCount": 10,
            "resourceConfiguration": {
                "cpu": "4vCPU",
                "memory": "4GB"
            }
        }
    }' \
    --maximum-capacity '{
        "cpu": "200vCPU",
        "memory": "200GB",
        "disk": "1000GB"
    }'
```

This will return information about your application. In this case, we've created an application that can handle 2 simultaneous Spark apps with an initial set of 10 executors, each with 4vCPU and 4GB of memory, that can scale up to 200vCPU or 50 executors.

```json
{
    "applicationId": "00et0dhmhuokmr09",
    "arn": "arn:aws:emr-serverless:us-east-1:123456789012:/applications/00et0dhmhuokmr09",
    "name": "serverless-demo"
}
```

We'll set an `APPLICATION_ID` environment variable to reuse later.

```shell
export APPLICATION_ID=00et0dhmhuokmr09
```

- Get the state of your application

```shell
aws emr-serverless get-application \
    --application-id $APPLICATION_ID
```

Once your application is in `CREATED` state, you can go ahead and start it.

```shell
aws emr-serverless start-application \
    --application-id $APPLICATION_ID
```

Once your application is in `STARTED` state, you can submit jobs.

With [pre-initialized capacity](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-capacity-api.html), you can define a minimum amount of resources that EMR Serverless keeps ready to respond to interactive queries. EMR Serverless will scale your application up as necessary to respond to workloads, but return to the pre-initialized capacity when there is no activity. You can start or stop an application to effectively pause your application so that you are not billed for resources you're not using. If you don't need second-level response times in your workloads, you can use the default capacity and EMR Serverless will decomission all resources when a job is complete and scale back up as more workloads come in.

## Run your job

Now that you've created your application, you can submit jobs to it at any time.

We define our `sparkSubmitParameters` with resources that match our pre-initialized capacity, but EMR Serverless will still automatically scale as necessary.

_ℹ️ Note that with Spark jobs, you must account for Spark overhead and configure our executor with less memory than the application._

In this case, we're also configuring Spark logs to be delivered to our S3 bucket.

```shell
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${S3_BUCKET}'/code/pyspark/extreme_weather.py",
            "sparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=3g --conf spark.executor.cores=4 --conf spark.executor.memory=3g --conf spark.executor.instances=10"
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

```json
{
    "applicationId": "00esprurjpeqpq09",
    "arn": "arn:aws:emr-serverless:us-east-1:123456789012:/applications/00esprurjpeqpq09/jobruns/00esps8ka2vcu801",
    "jobRunId": "00esps8ka2vcu801"
}
```

Let's set our `JOB_RUN_ID` variable so we can use it to monitor the job progress.

```shell
export JOB_RUN_ID=00esps8ka2vcu801
```

```shell
aws emr-serverless get-job-run \
    --application-id $APPLICATION_ID \
    --job-run-id $JOB_RUN_ID
```

The job should start within a few seconds since we're making use of pre-initialized capacity.

We can also look at our logs while the job is running.

```shell
aws s3 ls s3://${S3_BUCKET}/logs/applications/$APPLICATION_ID/jobs/$JOB_RUN_ID/
```

Or copy the stdout of the job.

```shell
aws s3 cp s3://${S3_BUCKET}/logs/applications/$APPLICfATION_ID/jobs/$JOB_RUN_ID/SPARK_DRIVER/stdout.gz - | gunzip
```

## Clean up

When you're all done, make sure to call `stop-application` to decommission your capacity and `delete-application` if you're all done.


```shell
aws emr-serverless stop-application \
    --application-id $APPLICATION_ID
```


```shell
aws emr-serverless delete-application \
    --application-id $APPLICATION_ID
```

## Spark UI Debugging

- (Optional) Follow the steps in [building the Spark UI Docker container](/utilities/spark-ui/)

- Get credentials and set LOG_DIR

```shell
export LOG_DIR=s3://${S3_BUCKET}/logs/applications/$APPLICfATION_ID/jobs/$JOB_RUN_ID/sparklogs/
```

- Fire up Docker

```shell
docker run --rm -it \
    --user spark \
    -p 18080:18080 \
    -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=$LOG_DIR -Dspark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
    -e AWS_REGION=us-east-1 \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    ghcr.io/aws-samples/emr-serverless-spark-ui:latest
```

- Access the Spark UI via http://localhost:18080
