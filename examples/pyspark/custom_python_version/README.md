# Custom Python versions on EMR Serverless

Occasionally, you'll require a specific Python version for your PySpark jobs. EMR Serverless release versions 6.x uses Python 3.7 by default and EMR Serverless 7.x uses Python 3.9. You can upgrade in several different ways. You can either use a [custom images](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-custom-image.html) and install an updated Python version from the official package repository, or build your own specific version of Python and include it in a custom image or packaged virtual environment.

## Custom Images

EMR 6.x is based on Amazon Linux 2 while EMR 7.x is based on Amazon Linux 2023. In both cases, you can install upgraded versions of Python on the image.

- Python 3.8 on EMR 6.x

```dockerfile
FROM --platform=linux/amd64 public.ecr.aws/emr-serverless/spark/emr-6.15.0:latest
USER root

RUN amazon-linux-extras install -y python3.8
ENV PYSPARK_PYTHON=/usr/bin/python3.8

USER hadoop:hadoop
```

- Python 3.11 on EMR 7.x

```dockerfile
FROM --platform=linux/amd64 public.ecr.aws/emr-serverless/spark/emr-7.0.0:latest
USER root

RUN dnf install -y python3.11
ENV PYSPARK_PYTHON=/usr/bin/python3.11

USER hadoop:hadoop
```

> [!WARNING]
> If you want to use Python >= 3.10 with interactive workloads on EMR Serverless, see [#57](https://github.com/aws-samples/emr-serverless-samples/issues/57#issuecomment-1894532538) for more details.

See the docs in [../custom-images](../custom-images/) for an end-to-end example of creating an EMR Serverless application with a custom image.

You can also build your own specific version of Python in a custom image. See the [Dockerfile](./Dockerfile) for a sample build, just note that you'll want to use the EMR Serverless base image and update the `PYSPARK_PYTHON` environment variable similar to the examples above.

## Virtual Environments

Occasionally, you'll require a specific Python version. While EMR Serverless uses Python 3.7.x by default, you can upgrade by building your own virtual environment with the desired version and copying the binaries when you package your virtual environment.

Let's say you want to make use of the new `match` statements in Python 3.10 - We'll use a Dockerfile to install Python 3.10.6 and create our custom virtual environment.

> [!IMPORTANT]
> EMR release 7.x now supports Python 3.9.x by default. To change the Python version in 7.x releases, use `public.ecr.aws/amazonlinux/amazonlinux:2023-minimal` as your base image.

Once created, we'll upload the new virtual environment and a sample Python script only compatible with 3.10 to S3.

```
# Define a variable for code storage and job logs
S3_BUCKET=<YOUR_S3_BUCKET>

# Build our custom venv with BuildKit backend
DOCKER_BUILDKIT=1 docker build --output . .

# Upload the artifacts to S3
aws s3 cp pyspark_3.10.6.tar.gz     s3://${S3_BUCKET}/artifacts/pyspark/
aws s3 cp python_3.10.py            s3://${S3_BUCKET}/code/pyspark/
```

We'll then submit our job with the venv archive provided in `spark.archives` and configure our Python environment variables appropriately.

```
aws emr-serverless start-job-run \
    --name custom-python \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${S3_BUCKET}'/code/pyspark/python_3.10.py",
            "sparkSubmitParameters": "--conf spark.archives=s3://'${S3_BUCKET}'/artifacts/pyspark/pyspark_3.10.6.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
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

If we copy the output of the job, we should see the new Python version works as expected.

```
 aws s3 cp s3://${S3_BUCKET}/logs/applications/${APPLICATION_ID}/jobs/${JOB_RUN_ID}/SPARK_DRIVER/stdout.gz - | gunzip
```

```
/home/hadoop/environment/bin/python
3.10.6 (main, Aug 12 2022, 18:37:31) [GCC 7.3.1 20180712 (Red Hat 7.3.1-15)]
Damon
```