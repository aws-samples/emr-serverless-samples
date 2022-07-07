#!/usr/bin/env python3

import aws_cdk as cdk

from stacks.common import CommonStack
from stacks.vpc import VPCStack
from stacks.emr_studio import EMRStudio
from stacks.emr_serverless import EMRServerlessStack
from stacks.mwaa import MwaaStack

app = cdk.App()

vpc = VPCStack(app, "VPCStack")
common = CommonStack(app, "Dependencies")
emr_serverless = EMRServerlessStack(app, "EMRServerless", vpc.vpc)
emr_studio = EMRStudio(app, "EMRStudio", vpc.vpc, "EMRServerlessAdmin", common.bucket)
mwaa = MwaaStack(app, "MWAAEMRServerless", vpc.vpc, common.bucket, emr_serverless.serverless_app.attr_arn, common.emr_serverless_job_role.role_arn)

app.synth()
