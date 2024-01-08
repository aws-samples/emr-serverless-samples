# EMR Serverless Java job

This example shows how to run a Java Spark job on EMR Serverless using pre-intialized capacity. 

We'll use a simple Hello World application from the [./hello-world](./hello-world/) directory.

_ℹ️ Throughout this demo, I utilize environment variables to allow for easy copy/paste_

## Setup

_You should have already completed the pre-requisites in this repo's [README](/README.md)._

- Define some environment variables to be used later

```shell
export S3_BUCKET=<YOUR_BUCKET_NAME>
export JOB_ROLE_ARN=arn:aws:iam::<ACCOUNT_ID>:role/emr-serverless-job-role
```

## Create EMR Serverless application

We create an EMR Serverless 6.9.0 application, which will run Spark 3.3.0. We'll also create an initial capacity of 1 driver and 2 workers.

```bash
aws emr-serverless create-application \
  --type SPARK \
  --name serverless-java-demo \
  --release-label "emr-6.9.0" \
    --initial-capacity '{
        "DRIVER": {
            "workerCount": 1,
            "workerConfiguration": {
                "cpu": "4vCPU",
                "memory": "16GB"
            }
        },
        "EXECUTOR": {
            "workerCount": 3,
            "workerConfiguration": {
                "cpu": "4vCPU",
                "memory": "16GB"
            }
        }
    }'
```

We'll set an `APPLICATION_ID` environment variable to reuse later.

```bash
export APPLICATION_ID=00et0dhmhuokmr09
```

Then start our application.

```shell
aws emr-serverless start-application \
    --application-id $APPLICATION_ID
```

This will create the application and pre-provision the capacity defined above. The capacity will continue running for 15 minutes, which is the default idle timeout for the auto-stop configuration. 

## Build and package job

First we package a jar containing our application and copy the resulting JAR to S3.

```bash
mvn package
aws s3 cp target/java-demo-1.0.jar s3://${S3_BUCKET}/code/java-spark/ 
```

## Run our job

Now we can run our hello-world app. We'll also configure Spark logs to be delivered to our S3 bucket.


```bash
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${S3_BUCKET}'/code/java-spark/java-demo-1.0.jar",
            "sparkSubmitParameters": "--class HelloWorld"
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

> [!TIP] We don't specify any Spark CPU or memory configurations - the defaults [defined here](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html#spark-defaults) fit into the pre-init capacity we created above. We used a value of `16GB` for our pre-init capacity because the Spark default is `14GB` plus 10% for Spark memory overhead.

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