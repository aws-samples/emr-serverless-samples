"""
EMR Serverless Spark Connect — gRPC Interceptor for Token Refresh

Intercepts every outgoing gRPC call, checks token expiry, refreshes via
GetSessionEndpoint if needed, and injects the fresh token as x-aws-proxy-auth metadata.
"""

import datetime
import logging
from collections import namedtuple

import grpc
from pyspark.sql.connect.client import ChannelBuilder

logger = logging.getLogger("EMRServerlessSparkConnect")

# namedtuple to create new ClientCallDetails with updated metadata
_ClientCallDetails = namedtuple(
    "_ClientCallDetails",
    ("method", "timeout", "metadata", "credentials", "wait_for_ready", "compression"),
)


class _ClientCallDetails(_ClientCallDetails, grpc.ClientCallDetails):
    pass


class SparkConnectGRPCInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Intercepts gRPC calls to inject a fresh EMR Serverless auth token."""

    def __init__(self, application_id: str, session_id: str, emrs_client):
        self.application_id = application_id
        self.session_id = session_id
        self.emrs_client = emrs_client
        self.cached_token = None
        # Refresh 5 minutes before actual expiry
        self.early_refresh_margin = 5 * 60
        self.cache_expiration_time = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

    def _refresh_token(self):
        logger.info(f"Refreshing token for session {self.session_id}")
        try:
            response = self.emrs_client.get_session_endpoint(
                applicationId=self.application_id,
                sessionId=self.session_id,
            )
            self.cached_token = response["authToken"]
            expires_at = response["authTokenExpiresAt"]
            # Handle both datetime and string responses
            if isinstance(expires_at, str):
                expires_at = datetime.datetime.fromisoformat(expires_at)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=datetime.timezone.utc)
            self.cache_expiration_time = expires_at - datetime.timedelta(seconds=self.early_refresh_margin)
            logger.info(f"Token refreshed. Next refresh at {self.cache_expiration_time}")
        except Exception as e:
            logger.error(f"Failed to refresh token for session {self.session_id}: {e}")
            raise

    def _with_metadata(self, client_call_details):
        now = datetime.datetime.now(datetime.timezone.utc)
        if self.cache_expiration_time < now:
            self._refresh_token()

        metadata = dict(client_call_details.metadata or {})
        metadata["x-aws-proxy-auth"] = self.cached_token
        return _ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=list(metadata.items()),
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
            compression=client_call_details.compression,
        )

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return continuation(self._with_metadata(client_call_details), request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return continuation(self._with_metadata(client_call_details), request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return continuation(self._with_metadata(client_call_details), request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return continuation(self._with_metadata(client_call_details), request_iterator)


class CustomChannelBuilder(ChannelBuilder):
    """Extends PySpark's ChannelBuilder to add the token-refresh interceptor."""

    def __init__(self, application_id: str, emrs_session_id: str, url: str, emrs_client):
        super().__init__(url)
        self._application_id = application_id
        self._emrs_session_id = emrs_session_id
        self._emrs_client = emrs_client

    def toChannel(self) -> grpc.Channel:
        channel = super().toChannel()
        interceptor = SparkConnectGRPCInterceptor(
            self._application_id, self._emrs_session_id, self._emrs_client
        )
        return grpc.intercept_channel(channel, interceptor)
