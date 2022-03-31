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

## Example Java Usage

The example below will:

- Create a new EMR Serverless Application
- Start a new Spark job with a sample `SparkPi` application
- Stop and delete your Application when done

It is intended as a high-level demo of how to call the EMR Serverless API from the Java SDK.

In the `myapp` folder.

- Ensure you install the necessary dependencies

```bash
mvn install
```

- Run the sample app with your own S3 bucket and IAM role

```bash
mvn exec:java -Dexec.mainClass="com.example.myapp.App -Dexec.args="--bucket <S3_BUCKET> --role-arn arn:aws:iam::123456789012:role/emr-serverless-job-role"
```

Once the job is running, you can also view Spark logs.

```bash
# View Spark logs
aws s3 ls s3://<S3_BUCKET>/emr-serverless/logs/applications/<application_id>/jobs/<job_run_id>/
```

Or copy the stdout to view the results.

```bash
aws s3 cp s3://<S3_BUCKET>/emr-serverless/logs/applications/<application_id>/jobs/<job_run_id>/SPARK_DRIVER/stdout.gz - | gunzip
```

If you like, you can build a jar and run it independently and modify the Spark job arguments as well.

```bash
mvn package
java -cp target/myapp-1.0-SNAPSHOT.jar com.example.myapp.App -h
```
