# Python Depedencies

You can create isolated Python virtual environments to package multiple Python libraries for a PySpark job. Here is an example of how you can package [Great Expectations](https://greatexpectations.io/) and profile a set of sample data.

## Pre-requisites

- Access to [EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/setting-up.html)
- [Docker](https://www.docker.com/get-started)
- An S3 bucket in `us-east-1` and an IAM Role to run your EMR Serverless jobs

> **Note**: If using Docker on Apple Silicon ensure you use `--platform linux/amd64`

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
- The driver and executor Python paths are configured via the various `--conf spark.emr-serverless` parameters

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

## PySpark jobs with Java dependencies

Sometimes you need to pull in Java dependencies like Kafka or PostgreSQL libraries. Unfortunately, `spark.jars.packages` will not work at this time so you will need to create a dependency uberjar and upload that to S3.

To do this, we'll create a [`pom.xml`](./pom.xml) that specifies our dependencies and use a [maven Docker container](./Dockerfile.jars) to build the uberjar. In this example, we'll package `org.postgresql:postgresql:42.4.0` and use the example script in [./pg_query.py](./pg_query.py) to query a Postgres database.

> **Note**: The code in `pg_query.py` is for demonstration purposes only - never store credentials directly in your code. 😁

1. Build an uberjar with your dependencies

```shell
docker build -f Dockerfile.jars --output . .
```

This will create a `uber-jars-1.0-SNAPSHOT.jar` file locally that you will copy to S3 in the next step.

2. Copy your code and jar

```shell
aws s3 cp pg_query.py s3://${S3_BUCKET}/code/pyspark/
aws s3 cp uber-jars-1.0-SNAPSHOT.jar s3://${S3_BUCKET}/code/pyspark/jars/
```

3. Set the following variables according to your environment.

```shell
export S3_BUCKET=<YOUR_S3_BUCKET_NAME>
export APPLICATION_ID=<EMR_SERVERLESS_APPLICATION_ID>
export JOB_ROLE_ARN=<EMR_SERVERLESS_IAM_ROLE>
```

4. Start your job with `--jars`

```shell
aws emr-serverless start-job-run \
    --name pg-query \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${S3_BUCKET}'/code/pyspark/pg_query.py",
            "sparkSubmitParameters": "--jars s3://'${S3_BUCKET}'/code/pyspark/jars/uber-jars-1.0-SNAPSHOT.jar"
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

5. See the output of your job!

Once your job finishes, you can copy the output locally to view the stdout.

```shell
export JOB_RUN_ID=<YOUR_JOB_RUN_ID>

aws s3 cp s3://${S3_BUCKET}/logs/applications/${APPLICATION_ID}/jobs/${JOB_RUN_ID}/SPARK_DRIVER/stdout.gz - | gunzip 
```
