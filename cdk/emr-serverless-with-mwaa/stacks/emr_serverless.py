from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_emrserverless as emrs
from aws_cdk import aws_iam as iam
from constructs import Construct


class EMRServerlessStack(Stack):
    serverless_app: emrs.CfnApplication

    def __init__(
        self, scope: Construct, construct_id: str, vpc: ec2.IVpc, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.serverless_app = emrs.CfnApplication(
            self,
            "spark_app",
            release_label="emr-6.6.0",
            type="SPARK",
            name="spark-3.2",
            network_configuration=emrs.CfnApplication.NetworkConfigurationProperty(
                subnet_ids=vpc.select_subnets().subnet_ids,
                security_group_ids=[self.create_security_group(vpc).security_group_id],
            ),
            initial_capacity=[
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Driver",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=2,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU", memory="16gb"
                        ),
                    ),
                ),
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Executor",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=10,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU", memory="16gb"
                        ),
                    ),
                ),
            ],
            auto_stop_configuration=emrs.CfnApplication.AutoStopConfigurationProperty(
                enabled=True, idle_timeout_minutes=100
            ),
        )

        CfnOutput(self, "ApplicationID", value=self.serverless_app.attr_application_id)

    def create_security_group(self, vpc: ec2.IVpc) -> ec2.SecurityGroup:
        return ec2.SecurityGroup(self, "EMRServerlessSG", vpc=vpc)
