"""Monkey-patch pyspark's Spark Connect ChannelBuilder so every channel built
from a SPARK_REMOTE URL pointing at EMR Serverless gets the token-refresh
interceptor attached automatically.

Re-uses the interceptor implementation from `emrs_custom_spark_connect`
(authored by Vikrant Kumar, bundled alongside this file for convenience until
the upstream package is published).

Usage (dbt or any stock Spark Connect client):

    export SPARK_REMOTE="sc://<sid>.s.emr-serverless-services.<region>.amazonaws.com:443/;use_ssl=true;x-aws-proxy-auth=<tok>"
    export EMRS_APPLICATION_ID="<app-id>"
    export AWS_REGION="<region>"

    python -c "import emrs_sparkconnect_autopatch; from dbt.cli.main import dbtRunner; dbtRunner().invoke(['run'])"

The import patches pyspark.sql.connect.client.ChannelBuilder.toChannel to
detect EMR-S URLs and swap in a channel with the refresh interceptor. Non-EMR-S
hosts pass through unchanged.
"""
import logging
import os
import re

import boto3
import grpc
from pyspark.sql.connect.client import ChannelBuilder as _ChannelBuilder

from emrs_custom_spark_connect.interceptors import SparkConnectGRPCInterceptor

logger = logging.getLogger("emrs_sparkconnect_autopatch")

_ORIG_TO_CHANNEL = _ChannelBuilder.toChannel
_EMRS_HOST_RE = re.compile(
    r"^([a-z0-9]+)\.s\.emr-serverless-services[\-\.a-z0-9]*\.amazonaws\.com$"
)


def _patched_to_channel(self) -> grpc.Channel:
    channel = _ORIG_TO_CHANNEL(self)
    m = _EMRS_HOST_RE.match(self.host)
    if not m:
        return channel  # not EMR-S, pass through

    session_id = m.group(1)
    application_id = os.environ.get("EMRS_APPLICATION_ID")
    if not application_id:
        import warnings
        warnings.warn(
            "EMR-S Spark Connect URL detected but EMRS_APPLICATION_ID env var is "
            "not set; token auto-refresh will not be active."
        )
        return channel

    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    emrs_client = boto3.client("emr-serverless", region_name=region)
    interceptor = SparkConnectGRPCInterceptor(application_id, session_id, emrs_client)
    logger.info(
        "Installed token-refresh interceptor for EMR-S app=%s session=%s",
        application_id, session_id,
    )
    return grpc.intercept_channel(channel, interceptor)


_ChannelBuilder.toChannel = _patched_to_channel
