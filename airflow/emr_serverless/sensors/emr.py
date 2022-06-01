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
from typing import TYPE_CHECKING, Any, FrozenSet, Set, Union

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
    :param target_states: a set of states to wait for, defaults to SUCCESS_STATES
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :param emr_conn_id: emr connection to use, defaults to 'emr_default'
    """

    INTERMEDIATE_STATES = {"PENDING", "RUNNING", "SCHEDULED", "SUBMITTED"}
    FAILURE_STATES = {"FAILED", "CANCELLING", "CANCELLED"}
    SUCCESS_STATES = {"SUCCESS"}
    TERMINAL_STATES = SUCCESS_STATES.union(FAILURE_STATES)

    def __init__(
        self,
        *,
        application_id: str,
        job_run_id: str,
        target_states: Union[Set, FrozenSet] = frozenset(SUCCESS_STATES),
        aws_conn_id: str = "aws_default",
        emr_conn_id: str = "emr_default",
        **kwargs: Any,
    ) -> None:
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.target_states = target_states
        self.application_id = application_id
        self.job_run_id = job_run_id
        super().__init__(**kwargs)

    def poke(self, context: "Context") -> bool:
        state = None

        try:
            state = self.hook.get_conn().get_job_run_status(
                applicationId=self.application_id, jobRunId=self.job_run_id
            )
        except Exception:
            raise AirflowException(f"Unable to get job state: {state}")

        return state in self.target_states

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook"""
        return EmrServerlessHook(emr_conn_id=self.emr_conn_id)


class EmrServerlessApplicationSensor(BaseSensorOperator):
    """
    Asks for the state of the application until it reaches a failure state or success state.
    If the application fails, the task will fail.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EmrServerlessApplicationSensor`

    :param application_id: application_id to check the state of
    :param target_states: a set of states to wait for, defaults to SUCCESS_STATES
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :param emr_conn_id: emr connection to use, defaults to 'emr_default'
    """

    INTERMEDIATE_STATES = {"CREATING", "STARTING", "STOPPING"}
    # TODO:  Question: Do these states indicate failure?
    FAILURE_STATES = {"STOPPED", "TERMINATED"}
    SUCCESS_STATES = {"CREATED", "STARTED"}

    def __init__(
        self,
        *,
        application_id: str,
        target_states: Union[Set, FrozenSet] = frozenset(SUCCESS_STATES),
        aws_conn_id: str = "aws_default",
        emr_conn_id: str = "emr_default",
        **kwargs: Any,
    ) -> None:
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.target_states = target_states
        self.application_id = application_id
        super().__init__(**kwargs)

    def poke(self, context: "Context") -> bool:
        state = None

        try:
            state = self.hook.get_conn().get_application_status(
                applicationId=self.application_id
            )
        except Exception:
            raise AirflowException(f"Unable to get application state: {state}")

        return state in self.target_states

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook"""
        return EmrServerlessHook(emr_conn_id=self.emr_conn_id)
