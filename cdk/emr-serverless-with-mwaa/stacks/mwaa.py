from aws_cdk import (
    aws_ec2 as ec2,
    aws_mwaa as mwaa,
    aws_s3 as s3,
    aws_s3_deployment as s3d,
    aws_iam as iam,
)
import aws_cdk as cdk
from constructs import Construct

class MwaaStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.IVpc, bucket: s3.Bucket, serverless_app_arn: str, serverless_job_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Upload our EMR Serverless requirements
        files = s3d.BucketDeployment(
            self,
            "mwaa-assets",
            sources=[s3d.Source.asset("./assets/airflow")],
            destination_bucket=bucket,
        )

        # Define a name for the Airflow environment
        mwaa_name = "emr-serverless-airflow"

        # And a service role with additional EMR permissions
        # See https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-create-role.html
        mwaa_service_role = iam.Role(
            self,
            "mwaa-service-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow.amazonaws.com"),
                iam.ServicePrincipal("airflow-env.amazonaws.com"),
            ),
            inline_policies={
                "CDKmwaaPolicyDocument": self.mwaa_policy_document(
                    mwaa_name, bucket.bucket_arn
                ),
                "AirflowEMRServerlessExecutionPolicy": self.emr_serverless_management_policy(serverless_app_arn, serverless_job_arn),
            },
            path="/service-role/",
        )

        # And security group
        security_group = ec2.SecurityGroup(
            self, id="mwaa-sg", vpc=vpc, security_group_name="mwaa-sg"
        )
        security_group.connections.allow_internally(ec2.Port.all_traffic(), "MWAA")

        # Enable logging on everything
        logging_configuration = mwaa.CfnEnvironment.LoggingConfigurationProperty(
            task_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                enabled=True, log_level="INFO"
            ),
            worker_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                enabled=True, log_level="INFO"
            ),
            scheduler_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                enabled=True, log_level="INFO"
            ),
            dag_processing_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                enabled=True, log_level="INFO"
            ),
            webserver_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                enabled=True, log_level="INFO"
            ),
        )

        # Create our MWAA
        subnets = [subnet.subnet_id for subnet in vpc.private_subnets]
        airflow = mwaa.CfnEnvironment(
            self,
            "airflow-v2",
            name=mwaa_name,
            airflow_version="2.2.2",
            dag_s3_path=f"dags/",
            source_bucket_arn=bucket.bucket_arn,
            execution_role_arn=mwaa_service_role.role_arn,
            requirements_s3_path="requirements.txt",
            webserver_access_mode="PUBLIC_ONLY",
            environment_class="mw1.small",
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                subnet_ids=subnets,
                security_group_ids=[security_group.security_group_id],
            ),
            logging_configuration=logging_configuration,
        )
        airflow.node.add_dependency(files)

        # Register a couple outputs
        cdk.CfnOutput(self, "mwaa_bucket", value=bucket.bucket_name)
        cdk.CfnOutput(self, "mwaa_url", value=f"https://{airflow.attr_webserver_url}")

    def emr_serverless_management_policy(self, emr_serverless_app_arn: str, emr_serverless_job_role_arn: str):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "emr-serverless:CreateApplication",
                        "emr-serverless:GetApplication",
                        "emr-serverless:StartApplication",
                        "emr-serverless:StopApplication",
                        "emr-serverless:DeleteApplication",
                        "emr-serverless:StartJobRun",
                        "emr-serverless:GetJobRun"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources = ["*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "iam:PassRole",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[emr_serverless_job_role_arn],
                    conditions={
                        "StringLike": {
                            "iam:PassedToService": "emr-serverless.amazonaws.com"
                        }
                    },
                ),
            ]
        )

    def mwaa_policy_document(self, mwaa_env_name: str, mwaa_bucket_arn: str):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=["airflow:PublishMetrics"],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:aws:airflow:{self.region}:{self.account}:environment/{mwaa_env_name}"
                    ],
                ),
                iam.PolicyStatement(
                    actions=["s3:ListAllMyBuckets"],
                    effect=iam.Effect.DENY,
                    resources=[f"{mwaa_bucket_arn}/*", f"{mwaa_bucket_arn}"],
                ),
                iam.PolicyStatement(
                    actions=["s3:*"],
                    effect=iam.Effect.ALLOW,
                    resources=[f"{mwaa_bucket_arn}/*", f"{mwaa_bucket_arn}"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogStream",
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                        "logs:GetLogRecord",
                        "logs:GetLogGroupFields",
                        "logs:GetQueryResults",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:aws:logs:{self.region}:{self.account}:log-group:airflow-{mwaa_env_name}-*"
                    ],
                ),
                iam.PolicyStatement(
                    actions=["logs:DescribeLogGroups"],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "sqs:ChangeMessageVisibility",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:GetQueueUrl",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[f"arn:aws:sqs:{self.region}:*:airflow-celery-*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "kms:Decrypt",
                        "kms:DescribeKey",
                        "kms:GenerateDataKey*",
                        "kms:Encrypt",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                    conditions={
                        "StringEquals": {
                            "kms:ViaService": [
                                f"sqs.{self.region}.amazonaws.com",
                                f"s3.{self.region}.amazonaws.com",
                            ]
                        }
                    },
                ),
                iam.PolicyStatement(
                    actions=[
                        "emr-containers:StartJobRun",
                        "emr-containers:DescribeJobRun",
                        "emr-containers:CancelJobRun",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "elasticmapreduce:RunJobFlow",
                        "elasticmapreduce:DescribeStep",
                        "elasticmapreduce:DescribeCluster",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=["iam:PassRole"],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:aws:iam::{self.account}:role/EMR_DemoRole",
                        f"arn:aws:iam::{self.account}:role/EMR_EC2_DemoRole",
                        f"arn:aws:iam::{self.account}:role/EMR_EC2_DefaultRole",
                        f"arn:aws:iam::{self.account}:role/EMR_DefaultRole",
                    ],
                ),
            ]
        )
