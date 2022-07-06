from aws_cdk import Stack
from aws_cdk import aws_emrserverless as emrs
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import CfnOutput
from constructs import Construct


class EMRServerlessStack(Stack):
    serverless_app: emrs.CfnApplication

    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.IVpc, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.serverless_app = emrs.CfnApplication(
            self,
            "spark_app",
            release_label="emr-6.6.0",
            type="SPARK",
            name="spark-3.2",
            network_configuration=emrs.CfnApplication.NetworkConfigurationProperty(
                subnet_ids=vpc.select_subnets().subnet_ids,
                security_group_ids=[self.create_security_group(vpc).security_group_id]
            ),
        )

        CfnOutput(self, "ApplicationID", value=self.serverless_app.attr_application_id)
    
    def create_security_group(self, vpc: ec2.IVpc) -> ec2.SecurityGroup:
        return ec2.SecurityGroup(self, "EMRServerlessSG", vpc=vpc)
