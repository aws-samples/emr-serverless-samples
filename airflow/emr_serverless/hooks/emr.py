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
from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from botocore.exceptions import ClientError


class EmrServerlessHook(AwsBaseHook):
    """
    Interact with Amazon EMR Serverless to create Applications, run and poll jobs,
    and return job status.
    """
    client_type = 'emr-serverless'

    # Jobs can only be submitted to an application once it is in one of these states
    APPLICATION_READY_STATES = (
        "CREATED",
        "STARTED",
    )

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
    JOB_RUN_SUCCESS_STATES = (
        "SUCCESS",
    )
    JOB_RUN_TERMINAL_STATES = JOB_RUN_SUCCESS_STATES + JOB_RUN_FAILURE_STATES
    

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(client_type="emr-serverless", *args, **kwargs)

    def create_application(
        self,
        name: str,
        releaseLabel: str,
        applicationType: str,
        initialCapacityConfig: Optional[Dict] = None,
        tags: Optional[Dict] = None,
        **kwargs,
    ) -> Dict:
        """
        Creates an EMR Serverless Application.
        """
        emr_serverless_client = self.conn

        response = emr_serverless_client.create_application(
            name=name, releaseLabel=releaseLabel, type=applicationType,
            # TODO: Add in additional config items
        )
        self.application_id = response.get('id')

        self.log.info("Created Amazon EMR Serverless application with the name %s.", response.get('name'))
        return response
    
    def submit_job_run(
        self,
        applicationId: str,
        jobRole: str,
        jobDriver: Dict,
        configurationOverrides: Optional[Dict],
        client_request_token: Optional[str] = None,
    ) -> str:
        """
        Submits a job to an Amazon EMR Serverless application
        """
        emr_serverless_client = self.conn

        params = {
            "applicationId": applicationId,
            "executionRoleArn": jobRole,
            "jobDriver": jobDriver,
            "configurationOverrides": configurationOverrides,
        }
        if client_request_token:
            params["clientToken"] = client_request_token

        response = emr_serverless_client.start_job_run(**params)
        return response.get('jobRunId')
    
    def get_job_state(self, applicationId: str, jobRunId: str) -> Optional[str]:
        try:
            response = self.conn.get_job_run(
                applicationId=applicationId,
                jobRunId=jobRunId,
            )
            return response['jobRun']['state']
        except self.conn.exceptions.ResourceNotFoundException:
            # If the job is not found, we raise an exception as something fatal has happened.
            raise AirflowException(f'Job ID {jobRunId} not found on Application {jobRunId}')
        except ClientError as ex:
            # If we receive a generic ClientError, we swallow the exception so that the
            # calling client has to make another request.
            self.log.error('AWS request failed, check logs for more info: %s', ex)
            return None
    
    def poll_job_state(
        self, applicationId: str, jobId: str, maxTries: Optional[int] = None, pollInterval: int = 30
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
        final_query_state = None  # Query state when query reaches final state or max_tries reached

        while True:
            query_state = self.get_job_state(applicationId, jobId)
            if query_state is None:
                self.log.info("Try %s: Invalid query state. Retrying again", try_number)
            elif query_state in self.JOB_RUN_TERMINAL_STATES:
                self.log.info("Try %s: Query execution completed. Final state is %s", try_number, query_state)
                final_query_state = query_state
                break
            else:
                self.log.info("Try %s: Query is still in non-terminal state - %s", try_number, query_state)
            if maxTries and try_number >= maxTries:  # Break loop if max_tries reached
                final_query_state = query_state
                break
            try_number += 1
            sleep(pollInterval)
        return final_query_state

    def stop_query(self, application_id: str, job_id: str) -> Dict:
        """
        Cancel the submitted job_run

        :param job_id: Id of submitted job_run
        :return: dict
        """
        return self.conn.cancel_job_run(
            applicationId=application_id,
            id=job_id,
        )
