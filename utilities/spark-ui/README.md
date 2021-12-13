# Spark UI

You can use this Docker image to start the Apache Spark History Server (SHS) and view the Spark UI locally.

## Pre-requisite

- Install Docker

## Build Docker image

You can either build this Docker image yourself, or use the public image here: `ghcr.io/aws-samples/emr-serverless-spark-ui:latest`

1. Download the Dockerfile in the `spark-ui` directory from the GitHub repository.
2. Login to ECR
```shell
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 755674844232.dkr.ecr.us-east-1.amazonaws.com
```
3. Build the image
```shell
docker build -t emr/spark-ui .
```

## Start the Spark History Server

You can use a pair of AWS access key and secret key, or temporary AWS credentials.

1. Set `LOG_DIR` to the location of your Spark eventlogs.

```shell
export LOG_DIR=s3://${S3_BUCKET}/logs/applications/$APPLICATION_ID/jobs/$JOB_RUN_ID/sparklogs/
```

2. Set your AWS access key and secret key, and optionally session token.

```shell
export AWS_ACCESS_KEY_ID="ASIAxxxxxxxxxxxx"
export AWS_SECRET_ACCESS_KEY="yyyyyyyyyyyyyyy"
export AWS_SESSION_TOKEN="zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
```

3. Run the Docker image

```shell
docker run --rm -it \
    -p 18080:18080 \
    -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=$LOG_DIR -Dspark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
    -e AWS_REGION=us-east-1 \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    emr/spark-ui
```

4. Access the Spark UI via http://localhost:18080