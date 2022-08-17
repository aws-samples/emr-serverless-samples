# EMR Serverless Airflow Examples

As of `apache-airflow-providers-amazon==5.0.0`, the EMR Serverless Operator is now part of the official [Apache Airflow Amazon Provider](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/index.html) and has been tested with open source Apache Airflow v2.2.2.

It's recommended to use the official provider package as that will receive future updates.

## Amazon Managed Workflows for Apache Airflow (MWAA)

The most recent version (as of 2022-08-14) of the Operator is available as a semantic-versioned zip in this repository. In order to use it, just add the following line to your `requirements.txt` file.

```
emr_serverless @ https://github.com/aws-samples/emr-serverless-samples/releases/download/v1.0.1/mwaa_plugin.zip
```

Note that the operator depends on `boto3>=1.23.9`. This requirement is defined in the `setup.py` file.

## Example DAGs

There are two example DAGs in this repository. They make use of [Variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) for relevant job roles, EMR Serverless application IDs, and S3 log buckets.

- [example_emr_serverless.py](./dags/example_emr_serverless.py) - Runs a simple Spark job on an already-created EMR Serverless appliction.
- [example_end_to_end.py](./dags/example_end_to_end.py) - Creates an EMR Serverless Spark application, runs two independent jobs, and deletes the application.

The second example is useful if you want to have a completely ephemeral EMR Serverless environment. When you delete the application, it no longer shows up in the AWS Console nor are you able to access the Spark UI in the console for the jobs. However, logs can be written to S3 for debugging purposes.