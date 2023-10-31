from typing import cast

import aws_cdk as cdk
from aws_cdk import Aws
from aws_cdk import Stack as Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3deploy
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct, IConstruct

from stacks.emr_serverless_sm import EmrServerlessStateMachineConstruct


class SfnEmrServerlessJobsStack(Stack):
    vpc: ec2.Vpc

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        emr_serverless_app_id: str,
        emr_serverless_app_arn: str,
        bucket: s3.Bucket,
        namespace: str,
        **kwargs,
    ) -> None:
        """This stack creates a simple EMR Serverless demo within Step Functions.

        We create a state machine that submits... TODO continue description

        :param scope: The scope of the stack.
        :param construct_id: The ID of the stack.
        :param emr_serverless_app_id: The ID of the EMR Serverless app.
        :param emr_serverless_app_arn: The ARN of the EMR Serverless app.
        :param bucket: The S3 bucket to use for the EMR Serverless demo.
        :param namespace: The namespace of the stack.
        :param kwargs: other arguments.
        """
        super().__init__(
            scope,
            construct_id,
            description="This stack creates a simple EMR Serverless demo within Step Functions",
            **kwargs,
        )
        cdk.Tags.of(scope=cast(IConstruct, self)).add(key="namespace", value=namespace)

        # Uploading the PySpark jobs to S3
        s3deploy.BucketDeployment(
            scope=self,
            id="UploadPySparkJobsToS3",
            destination_bucket=bucket,
            sources=[s3deploy.Source.asset("./assets/jobs/")],
            destination_key_prefix=f"jobs/{namespace}",
        )

        emr_execution_role = iam.Role(
            scope=self,
            id="EMRExecutionRole",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            inline_policies={
                "EmrExecutionRolePolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "s3:PutObject",
                                "s3:GetObject",
                                "s3:ListBucket",
                                "s3:DeleteObject",
                            ],
                            resources=[
                                bucket.bucket_arn,
                                f"{bucket.bucket_arn}/*",
                            ],
                        ),
                    ]
                ),
            },
            path="/service-role/",
        )

        writer_output_path = f"s3://{bucket.bucket_name}/data/"

        job1 = EmrServerlessStateMachineConstruct(
            scope=self,
            id="PySparkJob1",
            namespace=namespace,
            sfn_label="spark-writer",
            emr_serverless_application_id=emr_serverless_app_id,
            emr_serverless_application_arn=emr_serverless_app_arn,
            spark_job_name="Writer",
            spark_job_entry_point=f"s3://{bucket.bucket_name}/jobs/{namespace}/pyspark-writer-example.py",
            spark_job_arguments=[writer_output_path],
            spark_job_submit_parameters="--conf spark.driver.memory=1G",
            emr_execution_role_arn=emr_execution_role.role_arn,
            asynchronous=True
        )

        job2 = EmrServerlessStateMachineConstruct(
            scope=self,
            id="PySparkJob2",
            namespace=namespace,
            sfn_label="spark-reader",
            emr_serverless_application_id=emr_serverless_app_id,
            emr_serverless_application_arn=emr_serverless_app_arn,
            spark_job_name="Reader",
            spark_job_entry_point=f"s3://{bucket.bucket_name}/jobs/{namespace}/pyspark-reader-example.py",
            spark_job_arguments=[writer_output_path],
            spark_job_submit_parameters="--conf spark.driver.memory=1G",
            emr_execution_role_arn=emr_execution_role.role_arn,
            asynchronous=False
        )

        writer_task = sfn.CustomState(
            scope=self,
            id="WriteStateMachine",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::states:startExecution.sync:2",
                "Parameters": {
                    "StateMachineArn": job1.state_machine.state_machine_arn,
                    "Input.$": "$",
                },
                "Retry": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "BackoffRate": 1,
                        "IntervalSeconds": 60,
                        "MaxAttempts": 3,
                        "Comment": "Retry x3 if the Spark job fails",
                    }
                ],
            },
        )

        reader_task = sfn.CustomState(
            scope=self,
            id="ReaderStateMachine",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::states:startExecution.sync:2",
                "Parameters": {
                    "StateMachineArn": job2.state_machine.state_machine_arn,
                    "Input.$": "$",
                },
                "Retry": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "BackoffRate": 1,
                        "IntervalSeconds": 60,
                        "MaxAttempts": 3,
                        "Comment": "Retry x3 if the Spark job fails",
                    }
                ],
            },
        )

        # Main State Machine definition
        sfn_definition = sfn.Chain.start(writer_task).next(reader_task)

        sfn_policy = iam.Policy(
            self,
            f"MainStateMachine-{namespace}-policy",
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "events:PutTargets",
                            "events:PutRule",
                            "events:DescribeRule",
                        ],
                        effect=iam.Effect.ALLOW,
                        resources=[
                            f"arn:{Aws.PARTITION}:events:{Aws.REGION}:{Aws.ACCOUNT_ID}:rule/StepFunctions*",
                            f"arn:aws:events:{Aws.REGION}:{Aws.ACCOUNT_ID}:rule/StepFunctions*",
                        ],
                    ),
                    iam.PolicyStatement(
                        actions=["states:StartExecution"],
                        effect=iam.Effect.ALLOW,
                        resources=[
                            job1.state_machine.state_machine_arn,
                            job2.state_machine.state_machine_arn,
                        ],
                    ),
                ],
            ),
        )

        sm = sfn.StateMachine(
            scope=self,
            id=f"main-state-machine-{namespace}",
            definition=sfn_definition,
            timeout=cdk.Duration.minutes(60),
            state_machine_name=f"MainStateMachine_{namespace}",
        )

        sm.role.attach_inline_policy(sfn_policy)
