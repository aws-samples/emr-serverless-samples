# Spark UI

You can use this Docker image to start the Apache Spark History Server (SHS) and view the Spark UI locally.

## Pre-requisite

- Install Docker

## Build Docker image

1. Clone this repository and change into the `utilities/spark-ui` directory.
```shell
git clone https://github.com/aws-samples/emr-serverless-samples.git
cd emr-serverless-samples/utilities/spark-ui/
```
2. Login to ECR
```shell
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 755674844232.dkr.ecr.us-east-1.amazonaws.com
```
3. Build the image
```shell
docker build -t emr/spark-ui .
```

## Start the Spark History Server

You can use a pair of AWS access key and secret key, or temporary AWS credentials. This credentials should have access to s3 bucket. If customer enables encryption for the logs stored in s3 bucket, then credentials should have access to KMS key as well.

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

## Troubleshooting

You may get following exception during SHS startup.

1. **Issue/Exception:** com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.model.AmazonS3Exception: The ciphertext refers to a customer master key that does not exist, does not exist in this region, or you are not allowed to access. (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied) 
   
   **Reason:** Given user credentials may not have the access to KMS key which used to encrypt the logs in s3 bucket. Add kms policy with decrypt permission and verify.
2. **Issue/Exception:**  com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: 9VRVX4FX3ETVQ47T; S3 Extended Request ID: pxyErt0+HhMfrBye5fokQe1H6TynqIpSHM6YNdXg87DZA4Tji5kl6xh3leYpNb2Ej+plDXVJQsY=; Proxy: null) 

   **Reason:** Given user credentials may not have the access s3 bucket. Add s3 policy with read permission and verify.
