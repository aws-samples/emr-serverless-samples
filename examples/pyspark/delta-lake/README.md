# EMR Serverless Delta Lake with Poetry example

This example shows how to use the [`emr-cli`](https://github.com/awslabs/amazon-emr-cli) to deploy a Poetry-based project with Delta Lake to EMR Serverless.

As of EMR 6.9.0, Delta Lake jars are provided on the EMR Serverless image. This means you can use the `spark.jars` Spark configuration item to specify the path to the local Delta Lake jars. If you use a different version than what's provided with EMR Serverless, you can still use the `--packages` option to specify your version.

## Getting Started

> **Note**: This assumes you already have an EMR Serverless application or have completed the pre-requisites in this repo's [README](/README.md).

- Define some environment variables to be used later

```shell
export APPLICATION_ID=<APPLICATION_ID>
export S3_BUCKET=<YOUR_BUCKET_NAME>
export JOB_ROLE_ARN=arn:aws:iam::<ACCOUNT_ID>:role/emr-serverless-job-role
```

You can either `git clone` this project or use the `emr init` command to create a Poetry project and add the `delta-take` dependency yourself.

- Option 1: `git clone`

```
git clone https://github.com/aws-samples/emr-serverless-samples.git
cd emr-serverless-samples/examples/pyspark/delta-lake
poetry install
```

- Option 2: `emr init`

```
emr init --project-type poetry delta-lake
cd delta-lake
poetry add delta-spark==2.1.0
```

Copy `main.py` from this directory to your new folder.

## Deploying

```bash
emr run \
    --application-id ${APPLICATION_ID} \
    --job-role ${JOB_ROLE_ARN} \
    --s3-code-uri s3://${S3_BUCKET}/tmp/emr-cli-delta-lake/ \
    --entry-point main.py \
    --job-args ${S3_BUCKET} \
    --spark-submit-opts "--conf spark.jars=/usr/share/aws/delta/lib/delta-core.jar,/usr/share/aws/delta/lib/delta-storage.jar" \
    --build --wait
```