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

import os
from datetime import datetime

from airflow import DAG
from emr_serverless.operators.emr import EmrServerlessOperator

APPLICATION_ID = os.getenv("APPLICATION_ID", "00f0abcde4fg0001")
JOB_ROLE_ARN = os.getenv("JOB_ROLE_ARN", "arn:aws:iam::012345678912:role/emr_serverless_default_role")
S3_LOGS_BUCKET = os.getenv("S3_BUCKET", "my-logs-bucket")

# [START howto_operator_emr_serverless_config]
JOB_DRIVER_ARG = {
    "sparkSubmit": {
        "entryPoint": "local:///usr/lib/spark/examples/src/main/python/pi.py",
    }
}

CONFIGURATION_OVERRIDES_ARG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": f"s3://{S3_LOGS_BUCKET}/logs/"
        }
    },
}
# [END howto_operator_emr_serverless_config]

with DAG(
    dag_id='example_emr_serverless_job',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # An example of how to get the cluster id and arn from an Airflow connection
    # APPLICATION_ID = '{{ conn.emr_eks.extra_dejson["virtual_cluster_id"] }}'
    # JOB_ROLE_ARN = '{{ conn.emr_eks.extra_dejson["job_role_arn"] }}'

    # [START howto_operator_emr_serverless_job]
    job_starter = EmrServerlessOperator(
        task_id="start_job",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver=JOB_DRIVER_ARG,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
    )
    # [END howto_operator_emr_serverless_job]
