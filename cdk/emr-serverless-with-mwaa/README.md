
# EMR Serverless with Amazon Managed Workflows for Apache Airflow (MWAA) Stack

This is a CDK Python project that deploys an MWAA environment with the EMR Serverless Operator pre-installed with two sample DAGs.

## Getting Started

- Install [CDK v2](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html)
- Activate the Python virtualenv and install dependencies

```
source .venv/bin/activate
pip install -r requirements.txt
```

One you've got CDK and Python setup, you can use `cdk deploy --all --outputs-file out.json` to deploy the stack and write outputs to a JSON file.

The stack that's created uses [pre-initialized capacity](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-capacity.html) so that Spark jobs can start instantly, but note that this can result in additional cost as resources are maintained for a certain period of time after jobs finish their runs.

## Run an EMR Serverless job in Airflow

By default, the stack creates two sample DAGs.
- [example_end_to_end.py](./assets/airflow/dags/example_end_to_end.py) - End-to-end DAG that creates an EMR Serverless application, runs a job, then shuts down the application - great for when you just need to run a single non-SLA job utilizing only the resources that job needs
- [example_emr_serverless.py](./assets/airflow/dags/example_emr_serverless.py) - Simple DAG that utilizes an existing EMR Serverless application - great for when you have a persistent application that runs multiple jobs.

Both DAGs require variables for job role, S3 log bucket and (for the simple one), EMR Serverless application ID. This information can be found in the `out.json` file created above. Together with the `jq` utility, you can create a variable file that you can import to Airflow under `Admin --> Variables`.

```
cat out.json | jq '{"emr_serverless_application_id": .EMRServerless.ApplicationID, "emr_serverless_job_role": .Dependencies.emrserverlessjobrole, "emr_serverless_log_bucket": .Dependencies.s3bucket}' > airflow_variables.json
```

Your Airflow UI URL can also be found in the `out.json` file under `MWAAEMRServerless.mwaaurl`.

Optionally, you can also utilize the REST API as this MWAA environment is publicly available. 

```
HOSTNAME=$(aws mwaa get-environment --name emr-serverless-airflow --query Environment.WebserverUrl --output text)
CLI_TOKEN=$(aws mwaa create-cli-token --name emr-serverless-airflow --query CliToken --output text)

while read -r name value;
    do curl --request POST "https://$HOSTNAME/aws_mwaa/cli" \
        --header "Authorization: Bearer $CLI_TOKEN" \
        --header "Content-Type: text/plain" \
        --data-raw "variables set ${name} ${value}"
done < <(cat out.json | jq '{"emr_serverless_application_id": .EMRServerless.ApplicationID, "emr_serverless_job_role": .Dependencies.emrserverlessjobrole, "emr_serverless_log_bucket": .Dependencies.s3bucket}' | jq -r 'to_entries[] | "\(.key) \(.value)"')
```
