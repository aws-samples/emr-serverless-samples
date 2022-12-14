#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import sys
from typing import TYPE_CHECKING, Dict, Optional, Sequence
from uuid import uuid4

from emr_serverless.hooks.emr import (
    EmrServerlessHook,
    DEFAULT_COUNTDOWN,
    DEFAULT_CHECK_INTERVAL_SECONDS,
)

from emr_serverless.sensors.emr import (
    EmrServerlessApplicationSensor,
    EmrServerlessJobSensor,
)

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.compat.functools import cached_property

DEFAULT_CONN_ID = "aws_default"

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property


class EmrServerlessCreateApplicationOperator(BaseOperator):
    """
    Operator to create Serverless EMR Application
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrServerlessCreateApplicationOperator`
    :param release_label: The EMR release version associated with the application.
    :param job_type: The type of application you want to start, such as Spark or Hive.
    :param wait_for_completion: If true, wait for the Application to start before returning. Default to True
    :param client_request_token: The client idempotency token of the application to create.
      Its value must be unique for each request.
    :param config: Optional dictionary for arbitrary parameters to the boto API create_application call.
    :param aws_conn_id: AWS connection to use
    """

    def __init__(
        self,
        release_label: str,
        job_type: str,
        client_request_token: str = "",
        config: Optional[dict] = None,
        wait_for_completion: bool = True,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        self.aws_conn_id = aws_conn_id
        self.release_label = release_label
        self.job_type = job_type
        self.wait_for_completion = wait_for_completion
        self.kwargs = kwargs
        self.config = config or {}
        super().__init__(**kwargs)

        self.client_request_token = client_request_token or str(uuid4())

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook."""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: "Context"):
        response = self.hook.conn.create_application(
            clientToken=self.client_request_token,
            releaseLabel=self.release_label,
            type=self.job_type,
            **self.config,
        )
        application_id = response["applicationId"]

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Application Creation failed: {response}")

        self.log.info("EMR serverless application created: %s", application_id)

        # This should be replaced with a boto waiter when available.
        self.hook.waiter(
            get_state_callable=self.hook.conn.get_application,
            get_state_args={"applicationId": application_id},
            parse_response=["application", "state"],
            desired_state={"CREATED"},
            failure_states=EmrServerlessApplicationSensor.FAILURE_STATES,
            object_type="application",
            action="created",
        )

        self.log.info("Starting application %s", application_id)
        self.hook.conn.start_application(applicationId=application_id)

        if self.wait_for_completion:
            # This should be replaced with a boto waiter when available.
            self.hook.waiter(
                get_state_callable=self.hook.conn.get_application,
                get_state_args={"applicationId": application_id},
                parse_response=["application", "state"],
                desired_state={"STARTED"},
                failure_states=EmrServerlessApplicationSensor.FAILURE_STATES,
                object_type="application",
                action="started",
            )

        return application_id


class EmrServerlessStartJobOperator(BaseOperator):
    """
    Operator to start EMR Serverless job.
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrServerlessStartJobOperator`
    :param application_id: ID of the EMR Serverless application to start.
    :param execution_role_arn: ARN of role to perform action.
    :param job_driver: Driver that the job runs on.
    :param configuration_overrides: Configuration specifications to override existing configurations.
    :param client_request_token: The client idempotency token of the application to create.
      Its value must be unique for each request.
    :param config: Optional dictionary for arbitrary parameters to the boto API start_job_run call.
    :param wait_for_completion: If true, waits for the job to start before returning. Defaults to True.
    :param aws_conn_id: AWS connection to use
    """

    template_fields: Sequence[str] = (
        "application_id",
        "execution_role_arn",
        "job_driver",
        "configuration_overrides",
    )

    def __init__(
        self,
        application_id: str,
        execution_role_arn: str,
        job_driver: dict,
        configuration_overrides: Optional[dict],
        client_request_token: str = "",
        config: Optional[dict] = None,
        wait_for_completion: bool = True,
        aws_conn_id: str = "aws_default",
        countdown: int = DEFAULT_COUNTDOWN,
        check_interval_seconds: int = DEFAULT_CHECK_INTERVAL_SECONDS,
        **kwargs,
    ):
        self.aws_conn_id = aws_conn_id
        self.application_id = application_id
        self.execution_role_arn = execution_role_arn
        self.job_driver = job_driver
        self.configuration_overrides = configuration_overrides
        self.wait_for_completion = wait_for_completion
        self.config = config or {}
        self.countdown = countdown
        self.check_interval_seconds = check_interval_seconds
        super().__init__(**kwargs)

        self.client_request_token = client_request_token or str(uuid4())

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook."""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: "Context") -> Dict:
        self.log.info("Starting job on Application: %s", self.application_id)

        app_state = self.hook.conn.get_application(applicationId=self.application_id)[
            "application"
        ]["state"]
        if app_state not in EmrServerlessApplicationSensor.SUCCESS_STATES:
            self.hook.conn.start_application(applicationId=self.application_id)

            self.hook.waiter(
                get_state_callable=self.hook.conn.get_application,
                get_state_args={"applicationId": self.application_id},
                parse_response=["application", "state"],
                desired_state={"STARTED"},
                failure_states=EmrServerlessApplicationSensor.FAILURE_STATES,
                object_type="application",
                action="started",
            )

        response = self.hook.conn.start_job_run(
            clientToken=self.client_request_token,
            applicationId=self.application_id,
            executionRoleArn=self.execution_role_arn,
            jobDriver=self.job_driver,
            configurationOverrides=self.configuration_overrides,
            **self.config,
        )

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"EMR serverless job failed to start: {response}")

        self.log.info("EMR serverless job started: %s", response["jobRunId"])
        if self.wait_for_completion:
            # This should be replaced with a boto waiter when available.
            self.hook.waiter(
                get_state_callable=self.hook.conn.get_job_run,
                get_state_args={
                    "applicationId": self.application_id,
                    "jobRunId": response["jobRunId"],
                },
                parse_response=["jobRun", "state"],
                desired_state=EmrServerlessJobSensor.SUCCESS_STATES,
                failure_states=EmrServerlessJobSensor.FAILURE_STATES,
                object_type="job",
                action="run",
                countdown=self.countdown,
                check_interval_seconds=self.check_interval_seconds
            )
        return response["jobRunId"]


class EmrServerlessDeleteApplicationOperator(BaseOperator):
    """
    Operator to delete EMR Serverless application
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrServerlessDeleteApplicationOperator`
    :param application_id: ID of the EMR Serverless application to delete.
    :param wait_for_completion: If true, wait for the Application to start before returning. Default to True
    :param aws_conn_id: AWS connection to use
    """

    template_fields: Sequence[str] = ("application_id",)

    def __init__(
        self,
        application_id: str,
        wait_for_completion: bool = True,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        self.aws_conn_id = aws_conn_id
        self.application_id = application_id
        self.wait_for_completion = wait_for_completion
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook."""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: "Context") -> None:
        self.log.info("Stopping application: %s", self.application_id)
        self.hook.conn.stop_application(applicationId=self.application_id)

        # This should be replaced with a boto waiter when available.
        self.hook.waiter(
            get_state_callable=self.hook.conn.get_application,
            get_state_args={
                "applicationId": self.application_id,
            },
            parse_response=["application", "state"],
            desired_state=EmrServerlessApplicationSensor.FAILURE_STATES,
            failure_states=set(),
            object_type="application",
            action="stopped",
        )

        self.log.info("Deleting application: %s", self.application_id)
        response = self.hook.conn.delete_application(applicationId=self.application_id)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Application deletion failed: {response}")

        if self.wait_for_completion:
            # This should be replaced with a boto waiter when available.
            self.hook.waiter(
                get_state_callable=self.hook.conn.get_application,
                get_state_args={"applicationId": self.application_id},
                parse_response=["application", "state"],
                desired_state={"TERMINATED"},
                failure_states=EmrServerlessApplicationSensor.FAILURE_STATES,
                object_type="application",
                action="deleted",
            )

        self.log.info("EMR serverless application deleted")
