from typing import Any, cast

from aws_cdk import Aws, Duration, Tags
from aws_cdk import aws_iam as iam
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct, IConstruct


class EmrServerlessStateMachineConstruct(Construct):
    """
    Construct for an EMR Serverless State Machine.
    If asynchronous is set to True, a loop is implemented, waiting for the job to complete and check the exit code.
    The loop can be extended with other steps while the job is running.
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        namespace: str,
        sfn_label: str,
        emr_serverless_application_id: str,
        emr_serverless_application_arn: str,
        emr_api_execution_timeout_min: int = 5,
        spark_job_name: str,
        spark_job_entry_point: str,
        spark_job_arguments: list[Any],
        spark_job_submit_parameters: str,
        emr_execution_role_arn: str,
        wait_duration_in_seconds: int = 30,
        asynchronous: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(scope, id)
        Tags.of(scope=cast(IConstruct, self)).add(key="namespace", value=namespace)

        state_machine_name = f"{sfn_label}-{namespace}"

        formatted_spark_job_submit_parameters = spark_job_submit_parameters.replace(
            "\n", " "
        )
        spark_args = ",".join(
            [f"'{i}'" if i[0] != "$" else i for i in spark_job_arguments]
        )
        start_job_run_task = sfn.CustomState(
            self,
            f"Submit Job ({'Sync' if not asynchronous else 'Async'})",
            state_json={
                "Type": "Task",
                "Resource": f"arn:aws:states:::emr-serverless:startJobRun{'.sync' if not asynchronous else ''}",
                "Parameters": {
                    "ApplicationId": f"{emr_serverless_application_id}",
                    "ClientToken.$": "States.UUID()",
                    "ExecutionRoleArn": f"{emr_execution_role_arn}",
                    "ExecutionTimeoutMinutes": emr_api_execution_timeout_min,
                    "JobDriver": {
                        "SparkSubmit": {
                            "EntryPoint": f"{spark_job_entry_point}",
                            "EntryPointArguments.$": f"States.Array({spark_args})",
                            "SparkSubmitParameters": f"{formatted_spark_job_submit_parameters}",
                        }
                    },
                    "Name": f"{spark_job_name}",
                    "Tags": {
                        "Region": Aws.REGION,
                        "Namespace": namespace,
                        "Label": state_machine_name,
                    },
                },
            },
        )

        wait_step = sfn.Wait(
            scope=self,
            id=f"Wait {wait_duration_in_seconds} seconds",
            time=sfn.WaitTime.duration(Duration.seconds(wait_duration_in_seconds)),
            comment=f"Waiting for the EMR job to finish.",
        )

        check_job_status_step = sfn.CustomState(
            scope=self,
            id="Get job info",
            state_json={
                "Type": "Task",
                "Parameters": {
                    "ApplicationId.$": "$.ApplicationId",
                    "JobRunId.$": "$.JobRunId",
                },
                "Resource": "arn:aws:states:::aws-sdk:emrserverless:getJobRun",
                "ResultPath": "$.CheckJobStatusResult",
            },
        )

        job_killed = sfn.Fail(
            self,
            "Job Failed",
            cause="EMR Job Failed",
            error="The job failed (please retry)",
        )
        job_succeed = sfn.Succeed(self, "Job Succeed")

        choice_step = (
            sfn.Choice(scope=self, id="Check job status")
            .when(
                sfn.Condition.string_equals(
                    "$.CheckJobStatusResult.JobRun.State", "CANCELLED"
                ),
                job_killed,
            )
            .when(
                sfn.Condition.string_equals(
                    "$.CheckJobStatusResult.JobRun.State", "CANCELLING"
                ),
                job_killed,
            )
            .when(
                sfn.Condition.string_equals(
                    "$.CheckJobStatusResult.JobRun.State", "FAILED"
                ),
                job_killed,
            )
            .when(
                sfn.Condition.string_equals(
                    "$.CheckJobStatusResult.JobRun.State", "SUCCESS"
                ),
                job_succeed,
            )
            .otherwise(wait_step)
        )

        if asynchronous:
            sfn_definition = (
                sfn.Chain.start(start_job_run_task)
                .next(wait_step)
                .next(check_job_status_step)
                .next(choice_step)
            )
        else:
            sfn_definition = sfn.Chain.start(start_job_run_task)

        self.state_machine = sfn.StateMachine(
            scope=self,
            id=state_machine_name,
            state_machine_name=state_machine_name,
            definition_body=sfn.DefinitionBody.from_chainable(sfn_definition),
        )

        # IAM Permissions
        sfn_policy = iam.Policy(
            self,
            f"{state_machine_name}-policy",
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
                        actions=[
                            "xray:PutTelemetryRecords",
                            "xray:GetSamplingRules",
                            "xray:GetSamplingTargets",
                            "xray:PutTraceSegments",
                            "emr-serverless:StartJobRun",
                            "emr-serverless:GetJobRun",
                            "emr-serverless:CancelJobRun",
                            "emr-serverless:TagResource",
                            "emr-serverless:GetDashboardForJobRun",
                        ],
                        resources=[
                            emr_serverless_application_arn,
                            f"{emr_serverless_application_arn}/*",
                        ],
                        effect=iam.Effect.ALLOW,
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "iam:PassRole",
                        ],
                        resources=[emr_execution_role_arn],
                        effect=iam.Effect.ALLOW,
                    ),
                ],
            ),
        )

        self.state_machine.role.attach_inline_policy(sfn_policy)
