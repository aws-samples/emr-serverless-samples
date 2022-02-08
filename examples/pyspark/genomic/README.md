# Genomic analysis using Glow on EMR Serverless

Some customers may want to use [Glow](https://glow.readthedocs.io/en/latest/index.html) to work with genomic data.

This is an example of how to package Glow dependencies and run a job on EMR Serverless.

## Getting Started

First, we'll need to build a `virtualenv` that we can upload to S3. Similar to our base example, we'll use a Dockerfile to do this.

In addition, we're _also_ going to create an uber-jar directly from the [Glow](https://github.com/projectglow/glow) project.

- In this directory, run the following command and it will build the Dockerfile and export `pyspark_glow.tar.gz` and `glow-spark3-assembly-1.1.2-SNAPSHOT.jar` to your local filesystem.

```shell
docker build --output ./dependencies .
```

- Now copy those files to S3.

```shell
export S3_BUCKET=<YOUR_S3_BUCKET_NAME>
aws s3 sync ./dependencies/ s3://${S3_BUCKET}/artifacts/pyspark/glow/
```

- And this demo code

```shell
aws s3 cp glow_demo.py s3://${S3_BUCKET}/code/pyspark/
```

## Submitting a new job

Make sure to follow the [EMR Serverless getting started guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html) to spin up your Spark application.

- Define your EMR Serverless app ID and job role and submit the job.

Unfortunately, in preview EMR Serverless does not have outbound internet access. So we can't use the `--packages` option. Instead, we'll provide an `--archives` option for our PySpark virtualenv and `--jars` to our Glow uberjar on S3.

```shell
export APPLICATION_ID=<EMR_SERVERLESS_APPLICATION_ID>
export JOB_ROLE_ARN=<EMR_SERVERLESS_IAM_ROLE>

aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${S3_BUCKET}'/code/pyspark/glow_demo.py",
            "entryPointArguments": ["pyspark_glow.tar.gz"],
            "sparkSubmitParameters": "--conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec --conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=3 --conf spark.executor.memory=4g --conf spark.executor.instances=20 --archives=s3://'${S3_BUCKET}'/artifacts/pyspark/glow/pyspark_glow.tar.gz --jars s3://'${S3_BUCKET}'/artifacts/pyspark/glow/glow-spark3-assembly-1.1.2-SNAPSHOT.jar"
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

- Set your job run ID and get the status of the job.

```shell
export JOB_RUN_ID=001234567890

aws emr-serverless get-job-run \
    --application-id $APPLICATION_ID \
    --job-run-id $JOB_RUN_ID
```

- View the logs while the job is running.

```shell
aws s3 cp s3://${S3_BUCKET}/logs/applications/${APPLICATION_ID}/jobs/${JOB_RUN_ID}/SPARK_DRIVER/stdout.gz - | gunzip
```

## Alternatives

One other alternative I came up with while researching this is using a temp image to fetch your dependencies.

It uses an EMR on EKS image, so you'll need to [authenticate before building](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/docker-custom-images-steps.html).

```shell
docker build -f Dockerfile.jars . --output jars
```

This will export ~100 jars to your local filesystem, which you can upload to S3 and specificy via the `--jars` variable as a comma-delimited list.
