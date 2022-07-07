from aws_cdk import (
    Stack as Stack,
    CfnOutput as CfnOutput,
    aws_emr as emr,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_servicecatalog as servicecatalog,
)
import aws_cdk as cdk
from constructs import Construct

class CommonStack(Stack):
    bucket: s3.Bucket
    emr_serverless_job_role: iam.Role

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
         **kwargs
    ) -> None:

        super().__init__(scope, construct_id, **kwargs)

        # We create a single S3 bucket for:
        # - Versioned MWAA artificats
        # - EMR Serverless logs
        # - EMR Studio assets
        self.bucket = s3.Bucket(
            self,
            "mwaa-bucket",
            versioned=True,
            auto_delete_objects=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )


        # We create an IAM role for job execution
        # By default, it can read and write our bucket above
        # MWAA needs to PassRole to it
        self.emr_serverless_job_role = iam.Role(
            self,
            "emr-serverless-job-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            ),
            inline_policies={
                "S3Access": self.s3_access_policy( self.bucket ),
                "GlueAccess": self.glue_access_policy(),
            },
        )

        CfnOutput(self, "s3_bucket", value=self.bucket.bucket_name)
        CfnOutput(self, "emr_serverless_job_role", value=self.emr_serverless_job_role.role_arn)

    
    def s3_access_policy(self, bucket: s3.Bucket):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources = [
                        "*"
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"{bucket.bucket_arn}/*"
                    ],
                ),
            ]
        )
    
    def glue_access_policy(self):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "glue:GetDatabase",
                        "glue:GetDataBases",
                        "glue:CreateTable",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:GetPartition",
                        "glue:GetPartitions",
                        "glue:CreatePartition",
                        "glue:BatchCreatePartition",
                        "glue:GetUserDefinedFunctions"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources = [
                        "*"
                    ],
                ),
            ]
        )


