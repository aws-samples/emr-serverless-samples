"""
EMR Serverless Spark Connect — Session Manager (Option 2: high-level wrapper)

Manages the full session lifecycle: start → wait → get-endpoint → create SparkSession
with automatic token refresh.
"""

import logging
import time

import boto3
from pyspark.sql.connect.session import SparkSession

from .interceptors import CustomChannelBuilder

logger = logging.getLogger("EMRServerlessSparkConnect")


class EMRServerlessSparkSession:
    """High-level wrapper that manages EMR Serverless session lifecycle + token refresh."""

    def __init__(self, application_id, session_id, spark_session, emrs_client):
        self.application_id = application_id
        self.session_id = session_id
        self.spark = spark_session
        self.emrs_client = emrs_client

    @classmethod
    def create(
        cls,
        application_id: str,
        execution_role_arn: str,
        region: str = "us-west-2",
        endpoint_url: str = None,
        session_name: str = "spark-connect-session",
        idle_timeout_minutes: int = 15,
        max_wait: int = 900,
        spark_conf: dict = None,
    ) -> "EMRServerlessSparkSession":
        """Create a SparkSession with automatic token refresh.

        Args:
            application_id: EMR Serverless application ID
            execution_role_arn: IAM role ARN for execution
            region: AWS region
            endpoint_url: Custom EMR Serverless endpoint URL (for beta/gamma)
            session_name: Name for the session
            idle_timeout_minutes: Session idle timeout
            max_wait: Max seconds to wait for session ready
            spark_conf: Optional dict of spark config overrides

        Returns:
            EMRServerlessSparkSession wrapping a SparkSession with token auto-refresh
        """
        client_kwargs = {"region_name": region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        emrs_client = boto3.client("emr-serverless", **client_kwargs)

        # 1. Start session
        start_kwargs = {
            "applicationId": application_id,
            "name": session_name,
            "executionRoleArn": execution_role_arn,
            "idleTimeoutMinutes": idle_timeout_minutes,
        }
        if spark_conf:
            start_kwargs["configurationOverrides"] = {
                "runtimeConfiguration": [{
                    "classification": "spark-defaults",
                    "properties": spark_conf,
                }]
            }
        resp = emrs_client.start_session(**start_kwargs)
        session_id = resp["sessionId"]
        logger.info(f"Started session {session_id}")

        # 2. Wait for READY
        _wait_for_ready(emrs_client, application_id, session_id, max_wait)

        # 3. Get endpoint + token
        ep_resp = emrs_client.get_session_endpoint(
            applicationId=application_id, sessionId=session_id
        )
        endpoint = ep_resp["endpoint"]
        token = ep_resp["authToken"]

        # 4. Build Spark Connect URL
        # endpoint is like https://SESSION_ID.s.emr-serverless-services-beta.REGION.amazonaws.com
        # convert to sc:// format
        sc_url = endpoint.replace("https://", "sc://", 1)
        spark_connect_url = f"{sc_url}:443/;use_ssl=true;x-aws-proxy-auth={token}"
        logger.info(f"Spark Connect URL: {spark_connect_url}")

        # 5. Create SparkSession with custom channel builder (token auto-refresh)
        channel_builder = CustomChannelBuilder(
            application_id=application_id,
            emrs_session_id=session_id,
            url=spark_connect_url,
            emrs_client=emrs_client,
        )
        spark = SparkSession.builder.channelBuilder(channel_builder).getOrCreate()
        logger.info("SparkSession created with token auto-refresh")

        return cls(application_id, session_id, spark, emrs_client)

    def stop(self):
        """Stop the SparkSession and the EMR Serverless session."""
        if self.spark:
            try:
                self.spark.stop()
            except Exception as e:
                logger.error(f"Error stopping SparkSession: {e}")
            self.spark = None

        if self.session_id:
            try:
                self.emrs_client.terminate_session(
                    applicationId=self.application_id, sessionId=self.session_id
                )
                logger.info(f"Stopped session {self.session_id}")
            except Exception as e:
                logger.error(f"Error stopping session {self.session_id}: {e}")
            self.session_id = None

    def __getattr__(self, name):
        """Delegate attribute access to the underlying SparkSession."""
        return getattr(self.spark, name)


def _wait_for_ready(emrs_client, application_id, session_id, max_wait):
    """Poll GetSession until state is READY or terminal."""
    start = time.time()
    last_state = None
    while True:
        elapsed = time.time() - start
        if elapsed >= max_wait:
            raise TimeoutError(f"Session {session_id} not ready after {max_wait}s")

        resp = emrs_client.get_session(applicationId=application_id, sessionId=session_id)
        state = resp["session"]["state"]

        if state != last_state:
            logger.info(f"Session {session_id} state: {state}")
            last_state = state

        if state in ("READY", "IDLE", "STARTED"):
            return
        if state in ("FAILED", "TERMINATED", "STOPPED"):
            raise RuntimeError(f"Session {session_id} reached terminal state: {state}")

        time.sleep(2)
