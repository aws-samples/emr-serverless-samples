import aws_cdk as cdk
from aws_cdk import Aws
from aws_cdk import CfnOutput as CfnOutput
from aws_cdk import Stack as Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_emr as emr
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct


class EMRStudioStack(Stack):
    studio: emr.CfnStudio

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        namespace: str,
        **kwargs,
    ) -> None:
        """
        Creates the necessary security groups, asset bucket, and use roles and policies for EMR Studio.

        Studios require the following
        - An engine security group
        - A workspace security group
        - An s3 bucket for notebook assets
        - Service role and user roles
        - Session policies to limit user access inside the Studio

        In addition, we create a Service Catalog item for cluster templates.
        """
        super().__init__(scope, construct_id, **kwargs)
        # Create security groups specifically for EMR Studio
        [engine_sg, workspace_sg] = self.create_security_groups(vpc, namespace)

        # We also need to appropriately tag the VPC and subnets
        self.tag_vpc_and_subnets(vpc, namespace)

        # This is where Studio assets live like ipynb notebooks and git repos
        protected_namespaces = {"int", "prod"}

        removal_policy = (
            cdk.RemovalPolicy.RETAIN
            if namespace in protected_namespaces
            else cdk.RemovalPolicy.DESTROY
        )
        self.bucket = s3.Bucket(
            self,
            f"EMRStudioAssets_{namespace}",
            versioned=True,
            auto_delete_objects=False if namespace in protected_namespaces else True,
            bucket_name=f"emr-studio-assets-{Aws.REGION}-{namespace}",
            removal_policy=removal_policy,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,  # type: ignore
        )
        cdk.Tags.of(self.bucket).add("namespace", namespace)

        # The service role provides a way for Amazon EMR Studio to interoperate with other AWS services.
        service_role = self.create_service_role(namespace)

        studio = emr.CfnStudio(
            self,
            construct_id,
            name=f"EMRServerlessAdmin_{namespace}",
            auth_mode="IAM",
            vpc_id=vpc.vpc_id,
            default_s3_location=self.bucket.s3_url_for_object(),
            engine_security_group_id=engine_sg.security_group_id,
            workspace_security_group_id=workspace_sg.security_group_id,
            service_role=service_role.role_arn,
            subnet_ids=vpc.select_subnets().subnet_ids,
        )
        cdk.Tags.of(studio).add("namespace", namespace)

        CfnOutput(self, "EMRStudioURL", value=studio.attr_url)
        CfnOutput(
            self,
            "EMRStudioServerlessURL",
            value=f"{studio.attr_url}/#/serverless-applications",
        )

    def create_security_groups(self, vpc: ec2.Vpc, namespace: str):
        engine_sg = ec2.SecurityGroup(self, f"EMRStudioEngine_{namespace}", vpc=vpc)

        # The workspace security group requires explicit egress access to the engine security group.
        # For that reason, we disable the default allow all.
        workspace_sg = ec2.SecurityGroup(
            self, f"EMRWorkspaceEngine_{namespace}", vpc=vpc, allow_all_outbound=False
        )
        engine_sg.add_ingress_rule(
            workspace_sg,
            ec2.Port.tcp(18888),
            "Allow inbound traffic to EngineSecurityGroup ( from notebook to cluster for port 18888 )",
        )
        workspace_sg.add_egress_rule(
            engine_sg,
            ec2.Port.tcp(18888),
            "Allow outbound traffic from WorkspaceSecurityGroup ( from notebook to cluster for port 18888 )",
        )
        workspace_sg.connections.allow_to_any_ipv4(
            ec2.Port.tcp(443), "Required for outbound git access"
        )

        # We need to tag the security groups so EMR can make modifications
        cdk.Tags.of(engine_sg).add("for-use-with-amazon-emr-managed-policies", "true")
        cdk.Tags.of(engine_sg).add("namespace", namespace)
        cdk.Tags.of(workspace_sg).add(
            "for-use-with-amazon-emr-managed-policies", "true"
        )
        cdk.Tags.of(workspace_sg).add("namespace", namespace)

        return [engine_sg, workspace_sg]

    @staticmethod
    def tag_vpc_and_subnets(vpc: ec2.IVpc, namespace: str):
        cdk.Tags.of(vpc).add("for-use-with-amazon-emr-managed-policies", "true")
        cdk.Tags.of(vpc).add("namespace", namespace)
        for subnet in vpc.public_subnets + vpc.private_subnets:
            cdk.Tags.of(subnet).add("for-use-with-amazon-emr-managed-policies", "true")
            cdk.Tags.of(subnet).add("namespace", namespace)

    def create_service_role(self, namespace: str) -> iam.Role:
        return iam.Role(
            self,
            f"EMRStudioServiceRole_{namespace}",
            assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy(
                    self,
                    f"EMRStudioServiceRolePolicy_{namespace}",
                    statements=[
                        iam.PolicyStatement(
                            sid="AllowEMRReadOnlyActions",
                            actions=[
                                "elasticmapreduce:ListInstances",
                                "elasticmapreduce:DescribeCluster",
                                "elasticmapreduce:ListSteps",
                            ],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            sid="AllowEC2ENIActionsWithEMRTags",
                            actions=[
                                "ec2:CreateNetworkInterfacePermission",
                                "ec2:DeleteNetworkInterface",
                            ],
                            resources=[
                                cdk.Stack.format_arn(
                                    self,
                                    service="ec2",
                                    resource="network-interface",
                                    resource_name="*",
                                )
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceTag/for-use-with-amazon-emr-managed-policies": "true"
                                }
                            },
                        ),
                        iam.PolicyStatement(
                            sid="AllowEC2ENIAttributeAction",
                            actions=["ec2:ModifyNetworkInterfaceAttribute"],
                            resources=[
                                cdk.Stack.format_arn(
                                    self,
                                    service="ec2",
                                    resource=name,
                                    resource_name="*",
                                )
                                for name in [
                                    "instance",
                                    "network-interface",
                                    "security-group",
                                ]
                            ],
                        ),
                        iam.PolicyStatement(
                            sid="AllowEC2SecurityGroupActionsWithEMRTags",
                            actions=[
                                "ec2:AuthorizeSecurityGroupEgress",
                                "ec2:AuthorizeSecurityGroupIngress",
                                "ec2:RevokeSecurityGroupEgress",
                                "ec2:RevokeSecurityGroupIngress",
                                "ec2:DeleteNetworkInterfacePermission",
                            ],
                            resources=["*"],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceTag/for-use-with-amazon-emr-managed-policies": "true"
                                }
                            },
                        ),
                        iam.PolicyStatement(
                            sid="AllowDefaultEC2SecurityGroupsCreationWithEMRTags",
                            actions=["ec2:CreateSecurityGroup"],
                            resources=[
                                cdk.Stack.format_arn(
                                    self,
                                    service="ec2",
                                    resource="security-group",
                                    resource_name="*",
                                )
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:RequestTag/for-use-with-amazon-emr-managed-policies": "true"
                                }
                            },
                        ),
                        iam.PolicyStatement(
                            sid="AllowDefaultEC2SecurityGroupsCreationInVPCWithEMRTags",
                            actions=["ec2:CreateSecurityGroup"],
                            resources=[
                                cdk.Stack.format_arn(
                                    self,
                                    service="ec2",
                                    resource="vpc",
                                    resource_name="*",
                                )
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceTag/for-use-with-amazon-emr-managed-policies": "true"
                                }
                            },
                        ),
                        iam.PolicyStatement(
                            sid="AllowAddingEMRTagsDuringDefaultSecurityGroupCreation",
                            actions=["ec2:CreateTags"],
                            resources=[
                                cdk.Stack.format_arn(
                                    self,
                                    service="ec2",
                                    resource="security-group",
                                    resource_name="*",
                                )
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:RequestTag/for-use-with-amazon-emr-managed-policies": "true",
                                    "ec2:CreateAction": "CreateSecurityGroup",
                                }
                            },
                        ),
                        iam.PolicyStatement(
                            sid="AllowEC2ENICreationWithEMRTags",
                            actions=["ec2:CreateNetworkInterface"],
                            resources=[
                                cdk.Stack.format_arn(
                                    self,
                                    service="ec2",
                                    resource="network-interface",
                                    resource_name="*",
                                )
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:RequestTag/for-use-with-amazon-emr-managed-policies": "true"
                                }
                            },
                        ),
                        iam.PolicyStatement(
                            sid="AllowEC2ENICreationInSubnetAndSecurityGroupWithEMRTags",
                            actions=["ec2:CreateNetworkInterface"],
                            resources=[
                                cdk.Stack.format_arn(
                                    self,
                                    service="ec2",
                                    resource=name,
                                    resource_name="*",
                                )
                                for name in ["subnet", "security-group"]
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceTag/for-use-with-amazon-emr-managed-policies": "true"
                                }
                            },
                        ),
                        iam.PolicyStatement(
                            sid="AllowAddingTagsDuringEC2ENICreation",
                            actions=["ec2:CreateTags"],
                            resources=[
                                cdk.Stack.format_arn(
                                    self,
                                    service="ec2",
                                    resource="network-interface",
                                    resource_name="*",
                                )
                            ],
                            conditions={
                                "StringEquals": {
                                    "ec2:CreateAction": "CreateNetworkInterface"
                                }
                            },
                        ),
                        iam.PolicyStatement(
                            sid="AllowEC2ReadOnlyActions",
                            actions=[
                                "ec2:DescribeSecurityGroups",
                                "ec2:DescribeNetworkInterfaces",
                                "ec2:DescribeTags",
                                "ec2:DescribeInstances",
                                "ec2:DescribeSubnets",
                                "ec2:DescribeVpcs",
                            ],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            sid="AllowSecretsManagerReadOnlyActionsWithEMRTags",
                            actions=["secretsmanager:GetSecretValue"],
                            resources=[
                                cdk.Stack.format_arn(
                                    self,
                                    service="secretsmanager",
                                    resource="secret",
                                    resource_name="*",
                                )
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceTag/for-use-with-amazon-emr-managed-policies": "true"
                                }
                            },
                        ),
                        iam.PolicyStatement(
                            sid="S3permission",
                            actions=[
                                "s3:PutObject",
                                "s3:GetObject",
                                "s3:GetEncryptionConfiguration",
                                "s3:ListBucket",
                                "s3:DeleteObject",
                            ],
                            resources=["arn:aws:s3:::*"],
                        ),
                    ],
                )
            ],
        )
