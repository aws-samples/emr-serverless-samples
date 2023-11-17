#!/usr/bin/env python3
import os

from stacks.vpc import VPCStack
from stacks.emr_studio import EMRStudioStack
from stacks.emr_serverless import EMRServerlessStack
from stacks.sfn import SfnEmrServerlessJobsStack

import aws_cdk as cdk

app = cdk.App()

# Defining environment and namespace for the stacks
env = cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")
    )
namespace = os.getenv("NAMESPACE", "dev")  # to allow multiple deployments in the same account

vpc = VPCStack(
    scope=app, construct_id=f"VPCStack-{namespace}", namespace=namespace, env=env
)

emr_serverless = EMRServerlessStack(
    scope=app,
    construct_id=f"EMRServerless-{namespace}",
    vpc=vpc.vpc,
    namespace=namespace,
    env=env,
)

emr_studio = EMRStudioStack(
    scope=app,
    construct_id=f"EMRStudio-{namespace}",
    vpc=vpc.vpc,
    namespace=namespace,
    env=env,
)

SfnEmrServerlessJobsStack(
    scope=app,
    construct_id=f"SfnStack-{namespace}",
    emr_serverless_app_id=emr_serverless.serverless_app.attr_application_id,
    emr_serverless_app_arn=emr_serverless.serverless_app.attr_arn,
    bucket=emr_studio.bucket,
    namespace=namespace,
    env=env,
)

app.synth()
