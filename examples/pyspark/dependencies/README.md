# Python Depedencies

You can create isolated Python virtual environments to package multiple Python libraries for a PySpark job. Here is an example of how you can package [Great Expectations](https://greatexpectations.io/) and profile a set of sample data.

## Pre-requisites

- Access to [EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/setting-up.html)
- [Docker](https://www.docker.com/get-started)
- An S3 bucket in `us-east-1` and an IAM Role to run your EMR Serverless jobs

Set the following variables according to your environment.

```shell
export S3_BUCKET=<YOUR_S3_BUCKET_NAME>
export APPLICATION_ID=<EMR_SERVERLESS_APPLICATION_ID>
export JOB_ROLE_ARN=<EMR_SERVERLESS_IAM_ROLE>
```

## Profile data with EMR Serverless and Great Expectations

The example below builds a [virtual environment](https://virtualenv.pypa.io/en/latest/) with the necessary dependencies to use Great Expectations to profile a limited set of data from the [New York City Taxi and Limo trip data](https://registry.opendata.aws/nyc-tlc-trip-records-pds/). 

All the commands below should be executed in this (`examples/pyspark/dependencies`) directory.

1. Build your virtualenv archive

This command builds the included `Dockerfile` and exports the resulting `pyspark_ge.tar.gz` file to your local filesystem.

```shell
docker build --output . .
aws s3 cp pyspark_ge.tar.gz s3://${S3_BUCKET}/artifacts/pyspark/
```

2. Copy your code

There's a sample `ge_profile.py` script included here.

```shell
aws s3 cp ge_profile.py s3://${S3_BUCKET}/code/pyspark/
```

3. Run your job

- `entryPoint` should point to your script on S3
- `entryPointArguments` defines the output location of the Great Expectations profiler
- The virtualenv archive is added via the `--archives` parameter

```shell
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${S3_BUCKET}'/code/pyspark/ge_profile.py",
            "entryPointArguments": ["s3://'${S3_BUCKET}'/tmp/ge-profile"],
            "sparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=3 --conf spark.executor.memory=4g --conf spark.executor.instances=2 --conf spark.archives=s3://'${S3_BUCKET}'/artifacts/pyspark/pyspark_ge.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
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

When the job finishes, it will write a `part-00000` file out to `s3://${S3_BUCKET}/tmp/ge-profile`.

4. Copy and view the output

```shell
aws s3 cp s3://${S3_BUCKET}/tmp/ge-profile/part-00000 ./ge.html
open ./ge.html
```