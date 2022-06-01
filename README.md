# EMR Serverless Samples

This repository contains example code for getting started with EMR Serverless and using it with Apache Spark and Apache Hive.

In addition, it provides Container Images for both the Spark History Server and Tez UI in order to debug your jobs.

For full details about using EMR Serverless, please see the [EMR Serverless documentation](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html).

## Pre-Requisites

_These demos assume you are using an Administrator-level role in your AWS account_

1. **Amazon EMR Serverless is currently in preview.** Please follow the sign-up steps at https://pages.awscloud.com/EMR-Serverless-Preview.html to request access.

2. Create an Amazon S3 bucket in the us-east-1 region

```shell
aws s3 mb s3://BUCKET-NAME --region us-east-1
```

3. Create an EMR Serverless execution role (replacing `BUCKET-NAME` with the one you created above)

This role provides both S3 access for specific buckets as well as full read and write access to the Glue Data Catalog.

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
  

## Examples

- [CloudFormation Templates](./cloudformation/README.md)

  Sample templates for creating an EMR Serverless application as well as various dependencies.

- [EMR Serverless PySpark job](/examples/pyspark/README.md)

  This sample script shows how to use EMR Serverless to run a PySpark job that analyzes data from the open [NOAA Global Surface Summary of Day](https://registry.opendata.aws/noaa-gsod/) dataset.

- [Python Dependencies](/examples/pyspark/dependencies/README.md)

  Shows how to package Python dependencies ([Great Expectations](https://greatexpectations.io/)) using a [Virtualenv](https://virtualenv.pypa.io/en/latest/) and [`venv-pack`](https://jcristharif.com/venv-pack/).

- [Genomics analysis using Glow](/examples/pyspark/genomic/README.md)

  This sample shows how to use EMR Serverless to combine both Python and Java dependencies in order to run genomic analysis using [Glow](https://projectglow.io/) and [1000 Genomes](https://registry.opendata.aws/1000-genomes/).

- [EMR Serverless Hive query](/examples/hive/README.md)

  This sample script shows how to use Hive in EMR Serverless to query the same NOAA data.

### SDK Usage

During the preview, additional artifacts are required in order to access the EMR Serverless API. The examples below show how to do this.

- [EMR Serverless boto3 example](/examples/python-api/README.md)
- [EMR Serverless Java SDK example](/examples/java-api/README.md)

## Utilities

- [Spark UI](/utilities/spark-ui/)

  You can use this Dockerfile to run Spark history server in your container.

- [Tez UI](/utilities/tez-ui/)

  You can use this Dockerfile to run Tez UI and Application Timeline Server in your container.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.