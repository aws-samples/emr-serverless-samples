# EMR Serverless Spark Connect interactive sessions

This example shows how to run interactive PySpark from a local Jupyter notebook (or VS Code, PyCharm, etc.) against an Amazon EMR Serverless application using [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html). You write code on your laptop; EMR Serverless provisions the Spark driver and executors, executes the DataFrame and SQL operations, and returns the results.

Spark Connect interactive sessions ship in Amazon EMR release [`emr-7.13.0`](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-7130-release.html) and later. See the AWS documentation: [Run interactive sessions with Amazon EMR Serverless through Spark Connect](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/spark-connect.html).

_ℹ️ Throughout this demo, environment variables are used to allow easy copy/paste._

## Setup

_You should have already completed the pre-requisites in this repo's [README](/README.md)._

- Define some environment variables to be used later.

  ```shell
  export S3_BUCKET=<your-bucket>
  export JOB_ROLE_ARN=arn:aws:iam::<account-id>:role/emr-serverless-job-role
  ```

- Create an EMR Serverless application with Spark Connect sessions enabled. [Pre-initialized capacity](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-capacity-api.html) keeps 1 driver + 2 executors warm so sessions come up in under ~90 seconds.

  ```shell
  aws emr-serverless create-application \
    --type SPARK \
    --name spark-connect-demo \
    --release-label emr-7.13.0 \
    --interactive-configuration '{"sessionEnabled": true}' \
    --initial-capacity '{
      "DRIVER":   {"workerCount": 1, "workerConfiguration": {"cpu": "4vCPU", "memory": "16GB"}},
      "EXECUTOR": {"workerCount": 2, "workerConfiguration": {"cpu": "4vCPU", "memory": "16GB"}}
    }'
  ```

- Export the resulting application ID.

  ```shell
  export APPLICATION_ID=00xxxxxxxxxxxxx
  ```

- Start the application.

  ```shell
  aws emr-serverless start-application --application-id $APPLICATION_ID
  ```

- Upload sample parquet data the notebook will read.

  ```shell
  aws s3 cp your-data.parquet s3://${S3_BUCKET}/demo/spark-connect/data/trips/
  ```

## Required IAM permissions

The caller (your notebook's AWS credentials) needs the following, scoped to the application and its sessions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "emr-serverless:StartSession",
        "emr-serverless:ListSessions"
      ],
      "Resource": "arn:aws:emr-serverless:*:<account-id>:/applications/<application-id>"
    },
    {
      "Effect": "Allow",
      "Action": [
        "emr-serverless:GetSession",
        "emr-serverless:GetSessionEndpoint",
        "emr-serverless:TerminateSession",
        "emr-serverless:GetResourceDashboard"
      ],
      "Resource": "arn:aws:emr-serverless:*:<account-id>:/applications/<application-id>/sessions/*"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::<account-id>:role/emr-serverless-job-role",
      "Condition": {
        "StringLike": {"iam:PassedToService": "emr-serverless.amazonaws.com"}
      }
    }
  ]
}
```

## Run the notebook

Open [`spark-connect-interactive.ipynb`](./spark-connect-interactive.ipynb) in Jupyter, JupyterLab, or any IDE with notebook support. In cell 2, set:

- `APPLICATION_ID` to the value from `create-application`
- `EXECUTION_ROLE` to `$JOB_ROLE_ARN`
- `REGION` to your AWS region
- `DATA_S3_URI` to your parquet path

Then run all cells. The notebook installs `pyspark[connect]==3.5.6`, calls `StartSession`, connects a local `SparkSession` to the remote driver, runs aggregations on S3 data, shows the live Spark UI link, and terminates the session.

## Clean up

The notebook calls `TerminateSession` in the last cell. The application itself auto-stops after 15 minutes of inactivity by default, but you can stop or delete it explicitly.

```shell
aws emr-serverless stop-application --application-id $APPLICATION_ID
aws emr-serverless delete-application --application-id $APPLICATION_ID
```

## Notes and limitations

- The local PySpark version must match the Spark version on the EMR Serverless release you target. For `emr-7.13.0`, that is `3.5.6`. A mismatch causes connection errors.
- Python UDFs require the local Python minor version to match the worker Python (3.11 on `emr-7.13.0`). Built-in DataFrame and SQL operations do not.
- The Spark Connect URL must include `:443`, `use_ssl=true`, and `x-aws-proxy-auth=<token>`. Without them the PySpark client defaults to port 15002 and fails.
- Authentication tokens are valid for 1 hour. Re-fetch via `GetSessionEndpoint` to refresh.
- Sessions have a default 1-hour idle timeout and 24-hour hard limit. Each application supports up to 25 concurrent sessions by default.
- Not supported for Spark Connect sessions: Lake Formation fine-grained access control, Trusted Identity Propagation, and Serverless storage.
