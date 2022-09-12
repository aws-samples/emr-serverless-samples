# EMR Serverless Samples

This repository contains example code for getting started with EMR Serverless and using it with Apache Spark and Apache Hive.

In addition, it provides Container Images for both the Spark History Server and Tez UI in order to debug your jobs.

For full details about using EMR Serverless, please see the [EMR Serverless documentation](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html).

## Pre-Requisites

_These demos assume you are using an Administrator-level role in your AWS account_

1. **Amazon EMR Serverless is now Generally Available!** Check out the console to [Get Started with EMR Serverless](https://console.aws.amazon.com/emr/home#/serverless).

2. Create an Amazon S3 bucket in region where you want to use EMR Serverless (we'll assume `us-east-1`).

```shell
aws s3 mb s3://BUCKET-NAME --region us-east-1
```

3. Create an EMR Serverless execution role (replacing `BUCKET-NAME` with the one you created above)

This role provides both S3 access for specific buckets as well as read and write access to the Glue Data Catalog.

```shell
aws iam create-role --role-name emr-serverless-job-role --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "emr-serverless.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  }'

aws iam put-role-policy --role-name emr-serverless-job-role --policy-name S3Access --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadFromOutputAndInputBuckets",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::noaa-gsod-pds",
                "arn:aws:s3:::noaa-gsod-pds/*",
                "arn:aws:s3:::BUCKET-NAME",
                "arn:aws:s3:::BUCKET-NAME/*"
            ]
        },
        {
            "Sid": "WriteToOutputDataBucket",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::BUCKET-NAME/*"
            ]
        }
    ]
}'

aws iam put-role-policy --role-name emr-serverless-job-role --policy-name GlueAccess --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "GlueCreateAndReadDataCatalog",
        "Effect": "Allow",
        "Action": [
            "glue:GetDatabase",
            "glue:GetDataBases",
            "glue:CreateTable",
            "glue:GetTable",
            "glue:GetTables",
            "glue:GetPartition",
            "glue:GetPartitions",
            "glue:CreatePartition",
            "glue:BatchCreatePartition",
            "glue:GetUserDefinedFunctions"
        ],
        "Resource": ["*"]
      }
    ]
  }'
```
  
Now you're ready to go! Check out the examples below.

## Examples

- [CloudFormation Templates](./cloudformation/README.md)

  Sample templates for creating an EMR Serverless application as well as various dependencies.

- [CloudWatch Dashboard Template](/./cloudformation/emr-serverless-cloudwatch-dashboard/README.md)

  Template for creating a CloudWatch Dashboard for monitoring your EMR Serverless application.

- [CDK Examples](./cdk/README.md)

  Examples of building EMR Serverless environments with Amazon CDK.

- [Airflow Operator](./airflow/README.md)

  Sample DAGs and preview version of the Airflow Operator. Check the [releases page](https://github.com/aws-samples/emr-serverless-samples/releases) for updates.

- [EMR Serverless PySpark job](/examples/pyspark/README.md)

  This sample script shows how to use EMR Serverless to run a PySpark job that analyzes data from the open [NOAA Global Surface Summary of Day](https://registry.opendata.aws/noaa-gsod/) dataset.

- [Python Dependencies](/examples/pyspark/dependencies/README.md)

  Shows how to package Python dependencies ([Great Expectations](https://greatexpectations.io/)) using a [Virtualenv](https://virtualenv.pypa.io/en/latest/) and [`venv-pack`](https://jcristharif.com/venv-pack/).

- [Custom Python version](/examples/pyspark/custom_python_version/README.md)

  Shows how to use a different Python version than the default (3.7.10) provided by EMR Serverless.

- [Genomics analysis using Glow](/examples/pyspark/genomic/README.md)

  This sample shows how to use EMR Serverless to combine both Python and Java dependencies in order to run genomic analysis using [Glow](https://projectglow.io/) and [1000 Genomes](https://registry.opendata.aws/1000-genomes/).

- [EMR Serverless Hive query](/examples/hive/README.md)

  This sample script shows how to use Hive in EMR Serverless to query the same NOAA data.

### SDK Usage

You can call [EMR Serverless APIs](https://docs.aws.amazon.com/emr-serverless/latest/APIReference/Welcome.html) using standard AWS SDKs. The examples below show how to do this.

- [EMR Serverless boto3 example](/examples/python-api/README.md)
- [EMR Serverless Java SDK example](/examples/java-api/README.md)

## Utilities

- [EMR Serverless Estimator](https://github.com/aws-samples/aws-emr-utilities/tree/main/utilities/emr-serverless-estimator) - Estimate the cost of running Spark jobs on EMR Serverless based on Spark event logs.

_The following UIs are available in the EMR Serverless console, but you can still use them locally if you wish._

- [Spark UI](/utilities/spark-ui/)- Use this Dockerfile to run Spark history server in a container.

- [Tez UI](/utilities/tez-ui/)- Use this Dockerfile to run Tez UI and Application Timeline Server in a container.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
