# Tez UI

You can use this Docker image to start the Tez UI and Application Timeline Server and view the Tez UI locally.

## Pre-requisite

- Install Docker

## Build Docker image

You can either build this Docker image yourself, or use the public image here: `ghcr.io/aws-samples/emr-serverless-tez-ui:latest`

1. Download the files in the `tez-ui` directory from the GitHub repository.
2. Build the image
```shell
docker build -t emr/tez-ui .
```

## Start the Tez UI

You can use a pair of AWS access key and secret key, or temporary AWS credentials.

1. Set a few environment variables relevant to your job.

```shell
export S3_LOG_URI=s3://${S3_BUCKET}/logs
export APPLICATION_ID=001122334455
export JOB_RUN_ID=667788990011
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
    -p 8088:8088 -p 8188:8188 -p 9999:9999 \
    -e AWS_REGION=us-east-1 -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
    -e S3_LOG_URI -e JOB_RUN_ID -e APPLICATION_ID \
    emr/tez-ui
```

4. Access the Tez UI via http://localhost:8088