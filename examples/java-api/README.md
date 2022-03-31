# EMR Serverless Java SDK Example

This example shows how to call the EMR Serverless API using the Java SDK.

In it, we use a new maven project with the latest preview jar for EMR Serverless.

## Pre-requisites

- Access to [EMR Serverless Preview](https://pages.awscloud.com/EMR-Serverless-Preview.html)
- An Amazon S3 bucket in the `us-east-1` region
- A preview version of EMR Serverless Java SDK jar
    1. Download ['java-sdk.jar`](s3://elasticmapreduce/emr-serverless-preview/artifacts/latest/dev/sdk/java-sdk.jar)
    2. Install to your local maven repo

The steps below were tested on macOS and require `curl` and `mvn`.

```bash
# Download the java-sdk.jar and rename it
curl -o aws-java-sdk-emrserverless.jar https://elasticmapreduce.s3.amazonaws.com/emr-serverless-preview/artifacts/latest/dev/sdk/java-sdk.jar

# Install the preview jar in your local maven repo
mvn install:install-file \
    -Dfile=aws-java-sdk-emrserverless.jar \
    -DgroupId=com.amazonaws \
    -DartifactId=aws-java-sdk-emrserverless \
    -Dversion=PREVIEW \
    -Dpackaging=jar
```

## Running

In the `myapp` folder.

```bash
mvn exec:java -Dexec.mainClass="com.example.myapp.App"
```