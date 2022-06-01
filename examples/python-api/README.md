# EMR Serverless Python API Example

This example shows how to call the EMR Serverless API using the boto3 module.

In it, we create a new [`virtualenv`](https://virtualenv.pypa.io/en/latest/), install `boto3~=1.23.9`, and create a new EMR Serverless Application and Spark job.

## Pre-requisites

- Access to [EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html)
- An Amazon S3 bucket
- boto3~=1.23.9

The steps below were tested on macOS.

```bash
# Create and activate new virtualenv
python3 -m venv python-sdk-test
source python-sdk-test/bin/activate

# Install botocore in the virtualenv
python3 -m pip install boto3~=1.23.9
```

To verify your installation, you can run the following command which will show any EMR Serverless applications you currently have running.

```bash
python3 -c 'import boto3; import pprint; pprint.pprint(boto3.client("emr-serverless").list_applications())'
```

## Example Python API Usage

The script shown below will:

- Create a new EMR Serverless Application
- Start a new Spark job with a sample `wordcount` application
- Stop and delete your Application when done

It is intended as a high-level demo of how to use the boto3 with the EMR Serverless API.

```python
import boto3

client = boto3.client("emr-serverless")

# Create Application
response = client.create_application(
    name="my-application", releaseLabel="emr-6.6.0", type="SPARK"
)

print(
    "Created application {name} with application id {applicationId}. Arn: {arn}".format_map(
        response
    )
)

# Start Application
# Note that application must be in `CREATED` or `STOPPED` state.
# Use client.get_application(applicationId='<application_id>') to fetch state.
client.start_application(applicationId="<application_id>")

# Submit Job
# Note that application must be in `STARTED` state.
response = client.start_job_run(
    applicationId="<application_id>",
    executionRoleArn="<execution_role_arn>",
    jobDriver={
        "sparkSubmit": {
            "entryPoint": "s3://us-east-1.elasticmapreduce/emr-containers/samples/wordcount/scripts/wordcount.py",
            "entryPointArguments": ["s3://DOC-EXAMPLE-BUCKET/output"],
            "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1",
        }
    },
    configurationOverrides={
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": "s3://DOC-EXAMPLE-BUCKET/logs"}
        }
    },
)

# Get the status of the job
client.get_job_run(applicationId="<application_id>", jobRunId="<job_run_id>")

# Shut down and delete your application
client.stop_application(applicationId="<application_id>")
client.delete_application(applicationId="<application_id>")

```

Once the job is running, you can also view Spark logs.

```bash
# View Spark logs
aws s3 ls s3://DOC-EXAMPLE-BUCKET/logs/applications/<application_id>/jobs/<job_run_id>/
```

## Full EMR Serverless Python Example

For a more complete example, please see the [`emr_serverless.py`](./emr_serverless.py) file.

It can be used to run a full end-to-end PySpark sample job on EMR Serverless.

All you need to provide is a Job Role ARN and an S3 Bucket the Job Role has access to write to.

```bash
python emr_serverless.py \
    --job-role-arn arn:aws:iam::123456789012:role/emr-serverless-job-role \
    --s3-bucket my-s3-bucket
```
