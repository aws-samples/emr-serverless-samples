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
from time import sleep
from typing import Any, Dict, Optional, Set

from botocore.config import Config
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class EmrServerlessHook(AwsBaseHook):
    """
    Interact with EMR Serverless API.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    JOB_RUN_INTERMEDIATE_STATES = (
        "SUBMITTED",
        "PENDING",
        "SCHEDULED",
        "RUNNING",
    )
    JOB_RUN_FAILURE_STATES = (
        "FAILED",
        "CANCELLED",
        "CANCELLING",
    )
    JOB_RUN_SUCCESS_STATES = ("SUCCESS",)
    JOB_RUN_TERMINAL_STATES = JOB_RUN_SUCCESS_STATES + JOB_RUN_FAILURE_STATES

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "emr-serverless"
        kwargs["config"] = Config(user_agent_extra="EMRServerlessAirflowOperator/0.0.2")
        super().__init__(*args, **kwargs)

    def create_serverless_application(
        self, client_request_token: str, release_label: str, job_type: str, **kwargs
    ) -> Dict:
        """
        Create an EMR serverless application.

        :param client_request_token: The client idempotency token of the application to create.
          Its value must be unique for each request.
        :param release_label: The EMR release version associated with the application.
        :param job_type: Type of application
        :raises Exception: Exception from boto3 API call
        :returns: Response object from boto3 API call
        """

        try:
            return self.conn.create_application(
                clientToken=client_request_token,
                releaseLabel=release_label,
                type=job_type,
                **kwargs,
            )
        except Exception as ex:
            self.log.error(f"Exception while creating application: {ex}")
            raise Exception("Error creating application")

    def get_application_status(self, application_id: str) -> str:
        """
        Returns the state of a given application.

        :param application_id: ID of the application to check
        :returns: Current state of the application
        """

        try:
            response = self.conn.get_application(applicationId=application_id)
            return response["application"]["state"]
        except Exception as ex:
            self.log.error(f"Exception while getting application state: {ex}")
            raise Exception("Error getting application state")

    def start_serverless_application(self, application_id: str) -> None:
        """
        Start an EMR Serverless application.

        :param application_id: ID of the application to be deleted.
        :raises AirflowException: Exception from boto3 API call.
        """

        try:
            self.conn.start_application(applicationId=application_id)
        except Exception as ex:
            self.log.error(f"Exception while starting application: {ex}")
            raise Exception("Error starting application")

    def stop_serverless_application(self, application_id: str) -> None:
        """
        Stop an EMR Serverless application.

        :param application_id: ID of the application to be deleted.
        :raises AirflowException: Exception from boto3 API call.
        """

        try:
            self.conn.stop_application(applicationId=application_id)
        except Exception as ex:
            self.log.error(f"Exception while stopping application: {ex}")
            raise Exception("Error stopping application")

    def delete_serverless_application(self, application_id: str) -> Dict:
        """
        Delete an EMR Serverless application.

        :param application_id: ID of the application to be deleted.
        :raises AirflowException: Exception from boto3 API call.
        """

        try:
            return self.conn.delete_application(applicationId=application_id)
        except Exception as ex:
            self.log.error(f"Exception while deleting application: {ex}")
            raise Exception("Error deleting application")

    def start_serverless_job(
        self,
        client_request_token: str,
        application_id: str,
        execution_role_arn: str,
        job_driver: dict,
        configuration_overrides: Optional[dict],
        **kwargs,
    ) -> Dict:
        """
        Starts an EMR Serverless job on a created application.

        :param application_id: ID of the EMR Serverless application to start.
        :param execution_role_arn: ARN of role to perform action.
        :param job_driver: Driver that the job runs on.
        :param configuration_overrides: Configuration specifications to override existing configurations.
        :param client_request_token: The client idempotency token of the application to create.
          Its value must be unique for each request.
        :raises AirflowException: Exception from boto3 API call.
        :returns: Response object from boto3 API call.
        """

        if self.get_application_status(application_id=application_id) not in {
            "CREATED",
            "STARTING",
        }:
            self.conn.start_application(applicationId=application_id)

        try:
            return self.conn.start_job_run(
                clientToken=client_request_token,
                applicationId=application_id,
                executionRoleArn=execution_role_arn,
                jobDriver=job_driver,
                configurationOverrides=configuration_overrides,
                **kwargs,
            )
        except Exception as ex:
            self.log.error(f"Exception while starting job: {ex}")
            raise Exception("Error while starting job")

    def get_serverless_job_status(self, application_id: str, job_run_id: str) -> str:
        try:
            return self.conn.get_job_run(
                applicationId=application_id, jobRunId=job_run_id
            )["jobRun"]["state"]
        except Exception as ex:
            self.log.error(f"Exception while getting job state: {ex}")
            raise Exception("Error getting job state")

    def get_serverless_job_state_details(
        self, application_id: str, job_run_id: str
    ) -> Optional[str]:
        """
        Fetch the reason for a job failure (e.g. error message). Returns None or reason string.

        :param application_id: Application Id
        :param job_id: Id of submitted job run
        :return: str
        """
        # We absorb any errors if we can't retrieve the job status
        reason = None

        try:
            response = self.conn.get_job_run(
                applicationId=application_id,
                jobRunId=job_run_id,
            )
            reason = response["jobRun"]["stateDetails"]
        except KeyError:
            self.log.error("Could not get status of the EMR Serverless job")
        except ClientError as ex:
            self.log.error("AWS request failed, check logs for more info: %s", ex)

        return reason

    def wait_for_application_state(
        self,
        application_id: str,
        desired_states: Set,
        max_tries: Optional[int] = None,
        pollInterval: int = 5,
    ) -> None:
        try_number = 0

        while True:
            state = self.get_application_status(application_id=application_id)
            if state in desired_states:
                break
            try_number += 1
            if max_tries and try_number >= max_tries:
                raise Exception("Application did not reach desired state")
            sleep(pollInterval)

    def poll_job_state(
        self,
        applicationId: str,
        jobId: str,
        maxTries: Optional[int] = None,
        pollInterval: int = 30,
    ) -> Optional[str]:
        """
        Poll the state of submitted job run until query state reaches a final state.
        Returns one of the final states.
        :param application_id: Application Id where the job runs
        :param job_id: Id of submitted job run
        :param max_tries: Number of times to poll for query state before function exits
        :param poll_interval: Time (in seconds) to wait between calls to check query status on EMR
        :return: str
        """
        try_number = 1
        final_query_state = (
            None  # Query state when query reaches final state or max_tries reached
        )

        while True:
            query_state = self.get_serverless_job_status(applicationId, jobId)
            if query_state is None:
                self.log.info("Try %s: Invalid query state. Retrying again", try_number)
            elif query_state in self.JOB_RUN_TERMINAL_STATES:
                self.log.info(
                    "Try %s: Query execution completed. Final state is %s",
                    try_number,
                    query_state,
                )
                final_query_state = query_state
                break
            else:
                self.log.info(
                    "Try %s: Query is still in non-terminal state - %s",
                    try_number,
                    query_state,
                )
            if maxTries and try_number >= maxTries:  # Break loop if max_tries reached
                final_query_state = query_state
                break
            try_number += 1
            sleep(pollInterval)
        return final_query_state
