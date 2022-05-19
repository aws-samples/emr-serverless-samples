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
from typing import TYPE_CHECKING, Dict, Optional
from uuid import uuid4

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from emr_serverless.hooks.emr import EmrServerlessHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


DEFAULT_CONN_ID = 'aws_default'

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property


class EmrServerlessOperator(BaseOperator):
    """
    An operator that submits jobs to an EMR Serverless application
    """
    def __init__(
        self,
        *,
        application_id: str,
        execution_role_arn: str,
        job_driver: Dict,
        configuration_overrides: Optional[Dict] = None,
        client_request_token: Optional[str] = None,
        aws_conn_id: str = DEFAULT_CONN_ID,
        **kwargs,
    ) -> None:
        self.application_id = application_id
        self.execution_role_arn = execution_role_arn
        self.job_driver = job_driver
        self.configuration_overrides = configuration_overrides or {}
        self.aws_conn_id = aws_conn_id
        self.client_request_token = client_request_token or str(uuid4())
        super().__init__(**kwargs)
    
    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook"""
        return EmrServerlessHook(
            self.aws_conn_id
        )

    def execute(self, context: 'Context') -> Optional[str]:
        """Run job on EMR Containers"""
        self.job_id = self.hook.submit_job_run(
            self.application_id,
            self.execution_role_arn,
            self.job_driver,
            self.configuration_overrides,
            self.client_request_token,
        )
        query_status = self.hook.poll_job_state(self.application_id, self.job_id)

        if query_status in EmrServerlessHook.JOB_RUN_FAILURE_STATES:
            error_message = self.hook.get_job_state(self.job_id)
            raise AirflowException(
                f"EMR Serverless job failed. Final state is {query_status}. "
                f"query_execution_id is {self.job_id}. Error: {error_message}"
            )
        elif not query_status or query_status in EmrServerlessHook.JOB_RUN_INTERMEDIATE_STATES:
            raise AirflowException(
                f"Final state of EMR Serverless job is {query_status}. "
                f"Max tries of poll status exceeded, query_execution_id is {self.job_id}."
            )

        return self.job_id

    def on_kill(self) -> None:
        """Cancel the submitted job run"""
        if self.job_id:
            self.log.info("Stopping job run with jobId - %s", self.job_id)
            response = self.hook.stop_query(self.job_id)
            http_status_code = None
            try:
                http_status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            except Exception as ex:
                self.log.error("Exception while cancelling query: %s", ex)
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error("Unable to request query cancel on EMR. Exiting")
                else:
                    self.log.info(
                        "Polling EMR for query with id %s to reach final state",
                        self.job_id,
                    )
                    self.hook.poll_query_status(self.job_id)

    
    def submit_spark_job(self) -> Optional[str]:
        """Submit a Spark job to EMR Serverless application"""
        self.job_id = self.hook.submit_job_run(
            self.application_id,
            self.execution_role_arn,
            {"sparkSubmit": self.job_driver},
            self.configuration_overrides
        )