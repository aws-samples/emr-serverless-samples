# EMR Serverless Airflow Examples

Until EMR Serverless is generally available, you can use this repository to install a custom EMR Serverless Operator for either Amazon MWAA or open source Apache Airflow.

## MWAA

We've released the operator as a semantic-versioned zip in this repository. In order to use it, just add the following lines to your `requirements.txt` file for the release that you would like.

```
emr-serverless @ https://github.com/aws-samples/emr-serverless-samples/releases/download/v0.0.1/plugins.zip
boto3 @ https://github.com/aws-samples/emr-serverless-samples/releases/download/v0.0.1/boto3-1.21.18-py3-none-any.whl
botocore @ https://github.com/aws-samples/emr-serverless-samples/releases/download/v0.0.1/botocore-1.24.18-py3-none-any.whl
```

Note that we need to install a custom "preview" wheel for both the boto3 and botocore libraries.

