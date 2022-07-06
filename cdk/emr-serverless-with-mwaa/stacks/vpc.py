from aws_cdk import Stack as Stack, aws_ec2 as ec2
from constructs import Construct


class VPCStack(Stack):
    vpc: ec2.Vpc

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # We create a simple VPC here
        self.vpc = ec2.Vpc(self, "EMRServerlessDemo", max_azs=3)  # default is all AZs in region