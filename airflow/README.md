# EMR Serverless Airflow Examples

Until EMR Serverless is generally available, you can use this repository to install a custom EMR Serverless Operator for either Amazon MWAA or open source Apache Airflow.

## MWAA

We've released the operator as a semantic-versioned zip in this repository. In order to use it, just add the following lines to your `requirements.txt` file for the release that you would like.

```
emr-serverless @ https://github.com/aws-samples/emr-serverless-samples/releases/download/v0.0.2/mwaa_plugin.zip
```

Note that the operator depends on `boto3~=1.23.9`. This requirement is defined in the `setup.py` file.