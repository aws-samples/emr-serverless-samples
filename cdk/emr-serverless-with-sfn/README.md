
# EMR Serverless with Step Functions

This is a CDK Python project that deploys Step Function State Machines to run EMR Serverless jobs. An EMR Serverless
application is created with two example jobs. To submit these jobs, the State Machines can implement a synchronous or an 
asynchronous mechanism. Both examples are described below in details.

## Getting Started

- Install [CDK v2](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html)
- Activate the Python virtualenv and install dependencies

```
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Once you've got CDK and Python setup, [bootstrap the CDK app](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html)
and deploy the Stacks:

```
cdk bootstrap
cdk deploy --all
```

The stack that's created by [EMRServerlessStack](./stacks/emr_serverless.py) uses [pre-initialized capacity](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/application-capacity.html)
so that Spark jobs can start instantly, but note that this can result in additional cost as resources are maintained for
a certain period of time after jobs finish their runs.

## Run the State Machine

The stack creates a main State Machine to submit EMR Serverless jobs by means of other State Machines (in 
the example there are 2 jobs, a "reader" and a "writer"). One of these State Machines (the "writer") submit the job 
asynchronously, which loops between a `check` and a `wait` state every 30 seconds until the job is completed.
It means that the function to submit the job is not blocking and the loop can be extended with other operations 
to run without waiting for the job completion.

- [./stacks/sfn.py](./stacks/sfn.py): the main State Machine
- [./stacks/emr_serverless_sm.py](./stacks/emr_serverless_sm.py): the State Machine to submit a EMR Serverless job (notice
the asynchronous flag)

To submit the jobs it's necessary to navigate to the [Step Function](https://console.aws.amazon.com/states/) console,
open the "MainStateMachine_dev" state machine and click on "Start Execution" to trigger it's execution (with default
input).

## Spark jobs

In this CDK example, a Spark job writes some sample data into the default S3 bucket created for the EMR Studio assets, while another 
jobs reads it back (folder name is `data/`). The PySpark code is uploaded into the same bucket under the `jobs/` folder.
The PySpark example jobs:

- [./assets/jobs/pyspark-reader-example.py](./assets/jobs/pyspark-reader-example.py)
- [./assets/jobs/pyspark-writer-example.py](./assets/jobs/pyspark-writer-example.py)

## Environments

The solution is parametrized so that multiple deployments of the same stacks can co-exist in the same account under 
different names. That can be achieved by setting the `NAMESPACE` environment variable. By default, the NAMESPACE is set
to `dev`, which configures resources to have "dev" in their name (usually as suffix).

Setting or exporting the NAMESPACE variable will deploy resources with a different name. For instance, in this case the 
main State Machine will be called "MainStateMachine_test":

```
NAMESPACE=test cdk deploy --all
```

This is useful when testing new features in parallel. Attention: deploying the stacks with different namespaces will 
create multiple resources (duplicated, if unchanged). This will generate extra cost, use this feature with caution.

The same works when setting `AWS_REGION` for deploying the solution to a different region.

```
AWS_REGION=eu-west-1 NAMESPACE=test cdk destroy --all
```

## Cleaning up

When you are finished with testing, clean up resources to avoid future charges.

```
cdk destroy --all  # to destroy resources deployed in the dev namespace
NAMESPACE=test cdk destroy --all  # if the resources are deployed in another namespace
AWS_REGION=eu-west-1 NAMESPACE=test cdk destroy --all  # if the resources are deployed in another region and namespace
```

Namespaces `int` and `prod` are "protected", so that the S3 bucket for the 
[EMR Studio assets](./stacks/emr_studio.py#L43) cannot be automatically deleted and must be destroyed manually.
