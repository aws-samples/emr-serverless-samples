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

from emr_serverless.hooks.emr import EmrServerlessHook
from emr_serverless.sensors.emr import EmrServerlessJobSensor

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


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
    :param aws_conn_id: AWS connection to use
    """

    def __init__(
        self,
        release_label: str,
        job_type: str,
        client_request_token: str = "",
        config: Optional[dict] = None,
        wait_for_completion: bool = True,
        aws_conn_id: str = DEFAULT_CONN_ID,
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

    def execute(self, context: "Context"):
        emr_serverless_hook = EmrServerlessHook()
        response = emr_serverless_hook.create_serverless_application(
            client_request_token=self.client_request_token,
            release_label=self.release_label,
            job_type=self.job_type,
            **self.config,
        )
        application_id = response["applicationId"]

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Application Creation failed: {response}")

        self.log.info(f"EMR serverless application created: {application_id}")
        emr_serverless_hook.wait_for_application_state(application_id, {"CREATED"})

        self.log.info(f"Starting application {application_id}.")
        emr_serverless_hook.start_serverless_application(application_id=application_id)

        if self.wait_for_completion:
            emr_serverless_hook.wait_for_application_state(application_id, {"STARTED"})

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
    :param wait_for_completion: If true, waits for the job to start before returning. Defaults to True.
    :param aws_conn_id: AWS connection to use
    :param config: extra configuration for the job.
    """

    template_fields: Sequence[str] = ("application_id", "execution_role_arn", "job_driver", "configuration_overrides")

    def __init__(
        self,
        application_id: str,
        execution_role_arn: str,
        job_driver: dict,
        configuration_overrides: Optional[dict],
        client_request_token: str = "",
        wait_for_completion: bool = True,
        aws_conn_id: str = DEFAULT_CONN_ID,
        config: Optional[dict] = None,
        **kwargs,
    ):
        self.aws_conn_id = aws_conn_id
        self.application_id = application_id
        self.execution_role_arn = execution_role_arn
        self.job_driver = job_driver
        self.configuration_overrides = configuration_overrides
        self.wait_for_completion = wait_for_completion
        self.config = config or {}
        super().__init__(**kwargs)

        self.client_request_token = client_request_token or str(uuid4())

        if (
            configuration_overrides
            and "monitoringConfiguration" not in configuration_overrides.keys()
        ):
            raise AirflowException(
                'configurationOverrides needs a "monitoringConfiguration" entry'
            )

    def execute(self, context: "Context") -> Dict:
        emr_serverless_hook = EmrServerlessHook()

        self.log.info(f"Starting job on Application: {self.application_id}")
        response = emr_serverless_hook.start_serverless_job(
            client_request_token=self.client_request_token,
            application_id=self.application_id,
            execution_role_arn=self.execution_role_arn,
            job_driver=self.job_driver,
            configuration_overrides=self.configuration_overrides,
            **self.config,
        )

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"EMR serverless job failed to start: {response}")

        job_run_id = response["jobRunId"]
        self.log.info(f"EMR serverless job started: {job_run_id}")
        if self.wait_for_completion:
            query_status = emr_serverless_hook.poll_job_state(
                self.application_id, job_run_id
            )
            if query_status in EmrServerlessJobSensor.FAILURE_STATES:
                error_message = emr_serverless_hook.get_serverless_job_state_details(self.application_id, job_run_id)
                raise AirflowException(
                    f"EMR Serverless job failed. Final state is {query_status}. "
                    f"job_run_id is {job_run_id}. Error: {error_message}"
                )
        return response


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
        aws_conn_id: str = DEFAULT_CONN_ID,
        **kwargs,
    ):
        self.aws_conn_id = aws_conn_id
        self.application_id = application_id
        self.wait_for_completion = wait_for_completion
        super().__init__(**kwargs)

    def execute(self, context: "Context") -> None:
        emr_serverless_hook = EmrServerlessHook()

        self.log.info(f"Stopping application: {self.application_id}")
        emr_serverless_hook.stop_serverless_application(
            application_id=self.application_id
        )
        emr_serverless_hook.wait_for_application_state(self.application_id, {"STOPPED"})

        self.log.info(f"Deleting application: {self.application_id}")
        response = emr_serverless_hook.delete_serverless_application(
            application_id=self.application_id
        )

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Application deletion failed: {response}")

        if self.wait_for_completion:
            emr_serverless_hook.wait_for_application_state(
                self.application_id, {"TERMINATED"}
            )

        self.log.info("EMR serverless application deleted")
