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
from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Dict, FrozenSet, Optional, Sequence, Set, Union

from emr_serverless.hooks.emr import EmrServerlessHook

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property


class EmrServerlessJobSensor(BaseSensorOperator):
    """
    Asks for the state of the job run until it reaches a failure state or success state.
    If the job run fails, the task will fail.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EmrServerlessJobSensor`

    :param application_id: application_id to check the state of
    :param job_run_id: job_run_id to check the state of
    :param target_states: a set of states to wait for, defaults to 'SUCCESS'
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    """

    template_fields: Sequence[str] = (
        "application_id",
        "job_run_id",
    )

    def __init__(
        self,
        *,
        application_id: str,
        job_run_id: str,
        target_states: set
        | frozenset = frozenset(EmrServerlessHook.JOB_SUCCESS_STATES),
        aws_conn_id: str = "aws_default",
        **kwargs: Any,
    ) -> None:
        self.aws_conn_id = aws_conn_id
        self.target_states = target_states
        self.application_id = application_id
        self.job_run_id = job_run_id
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool:
        response = self.hook.conn.get_job_run(
            applicationId=self.application_id, jobRunId=self.job_run_id
        )

        state = response["jobRun"]["state"]

        if state in EmrServerlessHook.JOB_FAILURE_STATES:
            failure_message = f"EMR Serverless job failed: {self.failure_message_from_response(response)}"
            raise AirflowException(failure_message)

        return state in self.target_states

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook"""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        """
        return response["jobRun"]["stateDetails"]


class EmrServerlessApplicationSensor(BaseSensorOperator):
    """
    Asks for the state of the application until it reaches a failure state or success state.
    If the application fails, the task will fail.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EmrServerlessApplicationSensor`

    :param application_id: application_id to check the state of
    :param target_states: a set of states to wait for, defaults to {'CREATED', 'STARTED'}
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    """

    template_fields: Sequence[str] = ("application_id",)

    def __init__(
        self,
        *,
        application_id: str,
        target_states: set
        | frozenset = frozenset(EmrServerlessHook.APPLICATION_SUCCESS_STATES),
        aws_conn_id: str = "aws_default",
        **kwargs: Any,
    ) -> None:
        self.aws_conn_id = aws_conn_id
        self.target_states = target_states
        self.application_id = application_id
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool:
        response = self.hook.conn.get_application(applicationId=self.application_id)

        state = response["application"]["state"]

        if state in EmrServerlessHook.APPLICATION_FAILURE_STATES:
            failure_message = f"EMR Serverless job failed: {self.failure_message_from_response(response)}"
            raise AirflowException(failure_message)

        return state in self.target_states

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook"""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    @staticmethod
    def failure_message_from_response(response: dict[str, Any]) -> str | None:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        """
        return response["application"]["stateDetails"]
