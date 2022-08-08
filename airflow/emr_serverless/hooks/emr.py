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
from typing import Any, Callable, Dict, List, Set

from botocore.config import Config
from botocore.exceptions import ClientError

from airflow.compat.functools import cached_property
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

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "emr-serverless"
        super().__init__(*args, **kwargs)

    @cached_property
    def conn(self):
        """Get the underlying boto3 EmrServerlessAPIService client (cached)"""
        return super().conn

    # This method should be replaced with boto waiters which would implement timeouts and backoff nicely.
    def waiter(
        self,
        get_state_callable: Callable,
        get_state_args: Dict,
        parse_response: List,
        desired_state: Set,
        failure_states: Set,
        object_type: str,
        action: str,
        countdown: int = 25 * 60,
        check_interval_seconds: int = 60,
    ) -> None:
        """
        Will run the sensor until it turns True.
        :param get_state_callable: A callable to run until it returns True
        :param get_state_args: Arguments to pass to get_state_callable
        :param parse_response: Dictionary keys to extract state from response of get_state_callable
        :param desired_state: Wait until the getter returns this value
        :param failure_states: A set of states which indicate failure and should throw an
            exception if any are reached before the desired_state
        :param object_type: Used for the reporting string. What are you waiting for? (application, job, etc)
        :param action: Used for the reporting string. What action are you waiting for? (created, deleted, etc)
        :param countdown: Total amount of time the waiter should wait for the desired state
            before timing out (in seconds). Defaults to 25 * 60 seconds.
        :param check_interval_seconds: Number of seconds waiter should wait before attempting
            to retry get_state_callable. Defaults to 60 seconds.
        """
        response = get_state_callable(**get_state_args)
        state: str = self.get_state(response, parse_response)
        while state not in desired_state:
            if state in failure_states:
                raise AirflowException(
                    f"{object_type.title()} reached failure state {state}."
                )
            if countdown >= check_interval_seconds:
                countdown -= check_interval_seconds
                self.log.info('Waiting for %s to be %s.', object_type.lower(), action.lower())
                sleep(check_interval_seconds)
                state = self.get_state(
                    get_state_callable(**get_state_args), parse_response
                )
            else:
                message = f"{object_type.title()} still not {action.lower()} after the allocated time limit."
                self.log.error(message)
                raise RuntimeError(message)

    def get_state(self, response, keys) -> str:
        value = response
        for key in keys:
            if value is not None:
                value = value.get(key, None)
        return value
