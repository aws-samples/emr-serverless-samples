# Python Depedencies

While in preview, EMR Serverless does not fully supporting providing Python dependencies via the `--archives` or `--py-files` flags. Here is an example of how you can package [Great Expectations](https://greatexpectations.io/) and profile a set of sample data.

## Pre-requisites

- Access to the [EMR Serverless Preview](https://pages.awscloud.com/EMR-Serverless-Preview.html)
- [Docker](https://www.docker.com/get-started)
- An S3 bucket in `us-east-1` and an IAM Role to run your EMR Serverless jobs

Set the following variables according to your environment.

```shell
export S3_BUCKET=<YOUR_S3_BUCKET_NAME>
export APPLICATION_ID=<EMR_SERVERLESS_APPLICATION_ID>
export JOB_ROLE_ARN=<EMR_SERVERLESS_IAM_ROLE>
```

## Profile data with EMR Serverless and Great Expectations

The example below builds a [virtual environment](https://virtualenv.pypa.io/en/latest/) with the necessary dependencies to use Great Expectations to profile a limited set of data from the [New York City Taxi and Limo trip data](https://registry.opendata.aws/nyc-tlc-trip-records-pds/). 

All the commands below should be executed in this (`examples/pyspark/dependencies`) directory.

1. Build your virtualenv archive

This command builds the included `Dockerfile` and exports the resulting `pyspark_ge.tar.gz` file to your local filesystem.

```shell
docker build --output . .
aws s3 cp pyspark_ge.tar.gz s3://${S3_BUCKET}/artifacts/pyspark/
```

2. Copy your code

There's a sample `ge_profile.py` script included here. You can use that or if you want to include this in your own code, you'll need to have the following as part of your script after the Spark session has been initialized.

```python
userFilesDir = [match for match in sys.path if "userFiles" in match][0]
sys.path.append(f"{userFilesDir}/pyspark_ge.tar.gz/lib/python3.7/site-packages")
```

```shell
aws s3 cp ge_profile.py s3://${S3_BUCKET}/code/pyspark/
```

3. Run your job

- `entryPoint` should point to your script on S3
- `entryPointArguments` defines the virtualenv archive to add to your path and the output location of the Great Expectations profiler
- The virtualenv archive is added via the `--archives` parameter

```shell
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${S3_BUCKET}'/code/pyspark/ge_profile.py",
            "entryPointArguments": ["pyspark_ge.tar.gz", "s3://'${S3_BUCKET}'/tmp/ge-profile"],
            "sparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=3 --conf spark.executor.memory=4g --conf spark.executor.instances=2 --archives=s3://'${S3_BUCKET}'/artifacts/pyspark/pyspark_ge.tar.gz"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'${S3_BUCKET}'/logs/"
            }
        }
    }'
```

When the job finishes, it will write a `part-00000` file out to `s3://${S3_BUCKET}/tmp/ge-profile`.

4. Copy and view the output

```shell
aws s3 cp s3://${S3_BUCKET}/tmp/ge-profile/part-00000 ./ge.html
open ./ge.html
```