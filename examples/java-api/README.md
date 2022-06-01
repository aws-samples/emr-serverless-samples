# EMR Serverless Java SDK Example

This example shows how to call the EMR Serverless API using the Java SDK.

In it, we use a new maven project with the latest preview jar for EMR Serverless.

## Pre-requisites

- Maven 3
- Access to [EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html)
- An Amazon S3 bucket

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
mvn exec:java -Dexec.mainClass="com.example.myapp.App" -Dexec.args="--bucket <S3_BUCKET> --role-arn arn:aws:iam::123456789012:role/emr-serverless-job-role"
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
