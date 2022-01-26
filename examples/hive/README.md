# EMR Serverless Hive query

This example shows how to run a Hive query on EMR Serverless that analyzes data from the [NOAA Global Surface Summary of Day](https://registry.opendata.aws/noaa-gsod/) dataset from the Registry of Open Data on AWS.

The script analyzes data from a given year and finds the weather location with the most extreme rain, wind, snow, and temperature.

_ℹ️ Throughout this demo, I utilize environment variables to allow for easy copy/paste_

## Setup

_You should have already completed the pre-requisites in this repo's [README](/README.md)._

- This example requires that you have access to the AWS Glue Data Catalog. 

- Define some environment variables to be used later.

```shell
export S3_BUCKET=<YOUR_BUCKET_NAME>
export JOB_ROLE_ARN=arn:aws:iam::<ACCOUNT_ID>:role/emr-serverless-job-role
```

- First, make sure the `extreme_weather.sql` and `create_table.sql` scripts are uploaded to an S3 bucket in the `us-east-1` region.

```shell
aws s3 cp extreme_weather.sql s3://${S3_BUCKET}/code/hive/
aws s3 cp create_table.sql s3://${S3_BUCKET}/code/hive/
```

- Now, let's create and start an Application on EMR Serverless. Applications are where you submit jobs and are associated with a specific open source framework and release version.

_ℹ️ Please note that leaving a pre-initialized application running WILL incur costs in your AWS Account._

```shell
aws emr-serverless create-application \
  --type HIVE \
  --name serverless-demo \
  --release-label "emr-5.34.0-preview" \
  --initial-capacity '{
        "DRIVER": {
            "workerCount": 1,
            "resourceConfiguration": {
                "cpu": "2vCPU",
                "memory": "4GB",
                "disk": "30gb"
            }
        },
        "TEZ_TASK": {
            "workerCount": 10,
            "resourceConfiguration": {
                "cpu": "4vCPU",
                "memory": "8GB",
                "disk": "30gb"
            }
        }
    }' \
    --maximum-capacity '{
        "cpu": "400vCPU",
        "memory": "1024GB",
        "disk": "1000GB"
    }'
```

This will return information about your application.

```json
{
    "applicationId": "00et0f0b79s06o09",
    "arn": "arn:aws:emr-serverless:us-east-1:123456789012:/applications/00et0f0b79s06o09",
    "name": "serverless-demo"
}
```

We'll set an `APPLICATION_ID` environment variable to reuse later.

```shell
export APPLICATION_ID=00et0f0b79s06o09
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

```shell
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "hive": {
            "initQueryFile": "s3://'${S3_BUCKET}'/code/hive/create_table.sql",
            "query": "s3://'${S3_BUCKET}'/code/hive/extreme_weather.sql",
            "parameters": "--hiveconf hive.exec.scratchdir=s3://'${S3_BUCKET}'/hive/scratch --hiveconf hive.metastore.warehouse.dir=s3://'${S3_BUCKET}'/hive/warehouse"
        }
    }' \
    --configuration-overrides '{
        "applicationConfiguration": [
            {
                "classification": "hive-site",
                "properties": {
                    "hive.driver.cores": "2",
                    "hive.driver.memory": "4g",
                    "hive.tez.container.size": "8192",
                    "hive.tez.cpu.vcores": "4"
                }
            }
        ],
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'${S3_BUCKET}'/hive-logs/"
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
aws s3 ls s3://${S3_BUCKET}/hive-logs/applications/$APPLICATION_ID/jobs/$JOB_RUN_ID/
```

Or copy the stdout of the job.

```shell
aws s3 cp s3://${S3_BUCKET}/hive-logs/applications/$APPLICATION_ID/jobs/$JOB_RUN_ID/HIVE_DRIVER/stdout.gz - | gunzip
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

## Tez UI Debugging

- Follow the steps in [building the Tez UI Docker container](/utilities/tez-ui/) to build the container locally

- Get credentials and set S3_LOG_URI

```shell
export AWS_ACCESS_KEY_ID=AKIAaaaa
export AWS_SECRET_ACCESS_KEY=bbbb
export AWS_SESSION_TOKEN=yyyy

export S3_LOG_URI=s3://${S3_BUCKET}/hive-logs
```

- Fire up Docker

```shell
docker run --rm -d \
    --name emr-serverless-tez-ui \
    -p 8088:8088 -p 8188:8188 -p 9999:9999 \
    -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    -e S3_LOG_URI -e JOB_RUN_ID -e APPLICATION_ID \
    emr/tez-ui
```

- Open the Tez UI at http://localhost:9999/tez-ui/

- When you're done, stop the Docker image

```shell
docker stop emr-serverless-tez-ui
```