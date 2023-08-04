
# EMR Serverless with Step Functions

This is a CDK Python project that deploys Step Function State Machines with a sample EMR Serverless application with 
two jobs. To submit these jobs, the State Machine implements a mechanism to wait for the end of their execution before
proceeding to the next step.

## Getting Started

- Install [CDK v2](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html)
- Activate the Python virtualenv and install dependencies

```
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

One you've got CDK and Python setup, [bootstrap the CDK app](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html)
and deploy the Stacks

```
cdk bootstrap
cdk deploy --all
```

The stack that's created by [EMRServerlessStack](./stacks/emr_serverless.py) uses [pre-initialized capacity](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-capacity.html)
so that Spark jobs can start instantly, but note that this can result in additional cost as resources are maintained for
a certain period of time after jobs finish their runs.

## Run the State Machine

By default, the stack creates a main State Machine to submit EMR Serverless jobs by means of other State Machines (in 
the example there are 2 jobs, "reader" and a "writer"). Those State Machines submit the job and loop between 
a `check` and a `wait` state every 30 seconds until the job is completed.

- [./stacks/sfn.py](./stacks/sfn.py): the main State Machine
- [./stacks/emr_serverless_sm.py](./stacks/emr_serverless_sm.py): the State Machine to submit a EMR Serverless job

To submit the jobs it's necessary to navigate to the [Step Function](https://console.aws.amazon.com/states/) console and
open the "MainStateMachine_dev" state machine, clicking on "Start Execution" to trigger it's execution.

## Spark jobs

In this example, a Spark job writes data into the default S3 bucket created for the EMR Studio assets, while another jobs
reads the result. The PySpark code is uploaded into the same bucket so the State Machines can submit them into the EMR 
Serverless application. The jobs:

- [./assets/jobs/pyspark-reader-example.py](./assets/jobs/pyspark-reader-example.py)
- [./assets/jobs/pyspark-writer-example.py](./assets/jobs/pyspark-writer-example.py)

## Environments

The solution is parametrized so that multiple deployments of the same stacks can occur in the same account. That can be 
achieved by setting the `NAMESPACE` environment variable. By default, the NAMESPACE is set to `dev`, which means all the
deployed resources will have "dev" in their name (usually as suffix).

Setting or exporting the NAMESPACE variable will deploy resources with a different name. For instance, in this case the 
main State Machine will be called "MainStateMachine_test":

```
NAMESPACE=test cdk deploy --all
```

This is useful when testing new features in parallel. Attention: deploying the stacks with different namespaces will 
create multiple and similar resources (or duplicated, if unchanged). Use this feature with caution.

The same works when setting `AWS_REGION` for deploying the solution to a different region.

```
AWS_REGION=eu-west-1 NAMESPACE=test cdk destroy --all
```

## Cleaning up

When you are finished with testing, clean up resources to avoid future charges. This is particularly useful when testing
multiple features in different namespaces or regions.

```
cdk destroy --all  # to destroy resources deployed in the dev namespace
NAMESPACE=test cdk destroy --all  # if the resources are deployed in another namespace
AWS_REGION=eu-west-1 NAMESPACE=test cdk destroy --all  # if the resources are deployed in another region and namespace
```

Namespace `int` and `prod` are "protected", so that the S3 bucket for the 
[EMR Studio assets](./stacks/emr_studio.py#L43) cannot be automatically deleted and must to be destroyed manually.