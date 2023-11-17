from aws_cdk import Stack as Stack
from aws_cdk import aws_ec2 as ec2
from constructs import Construct


class VPCStack(Stack):
    vpc: ec2.Vpc

    def __init__(
        self, scope: Construct, construct_id: str, namespace: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # We create a simple VPC here
        self.vpc = ec2.Vpc(
            self, f"EMRServerlessDemo_{namespace}", max_azs=2
        )  # default is all AZs in region
