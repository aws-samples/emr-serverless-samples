"""End-to-end example: start an EMR Serverless Spark Connect session,
run dbt against it with automatic token refresh, then tear down.

This script demonstrates that stock dbt-core + dbt-spark works with EMR-S
Spark Connect beyond the 1-hour auth token TTL, by importing a small
patch shim (`emrs_sparkconnect_autopatch`) before dbt runs.

Usage:
    python run_dbt.py \\
        --application-id <APP_ID> \\
        --execution-role-arn <ROLE_ARN> \\
        --region us-east-1

Pre-requisites:
    pip install dbt-core "dbt-spark[session]" "pyspark[connect]>=3.5,<4" boto3
    # If on Python 3.12+:
    pip install "setuptools<81"
"""
import argparse
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

# Install the patch before any pyspark Spark Connect session is built.
import emrs_sparkconnect_autopatch  # noqa: E402, F401

import boto3  # noqa: E402


def wait_for_session(emrs, app_id, session_id, timeout=300):
    start = time.time()
    while True:
        state = emrs.get_session(
            applicationId=app_id, sessionId=session_id
        )["session"]["state"]
        if state in ("STARTED", "IDLE", "READY"):
            return
        if state in ("FAILED", "TERMINATED"):
            raise RuntimeError(f"Session reached terminal state: {state}")
        if time.time() - start > timeout:
            raise TimeoutError(f"Session not ready in {timeout}s (last state: {state})")
        time.sleep(5)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--application-id", required=True)
    p.add_argument("--execution-role-arn", required=True)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--idle-timeout-minutes", type=int, default=120,
                   help="EMR-S session idle timeout; set high enough to cover gaps between dbt runs.")
    p.add_argument("--project-dir", default=str(HERE / "dbt_project"))
    p.add_argument("--profiles-dir", default=str(HERE))
    p.add_argument("--dbt-command", default="run",
                   help="dbt subcommand (run, test, build, debug...)")
    args = p.parse_args()

    os.environ["AWS_REGION"] = args.region
    os.environ["EMRS_APPLICATION_ID"] = args.application_id
    os.environ["DBT_PROFILES_DIR"] = args.profiles_dir

    emrs = boto3.client("emr-serverless", region_name=args.region)

    # Start EMR-S session
    resp = emrs.start_session(
        applicationId=args.application_id,
        executionRoleArn=args.execution_role_arn,
        idleTimeoutMinutes=args.idle_timeout_minutes,
    )
    session_id = resp["sessionId"]
    print(f"Started EMR-S session: {session_id}")

    try:
        wait_for_session(emrs, args.application_id, session_id)
        ep = emrs.get_session_endpoint(
            applicationId=args.application_id, sessionId=session_id
        )
        url = ep["endpoint"].replace("https", "sc") + ":443/;use_ssl=true;"
        url += f"x-aws-proxy-auth={ep['authToken']}"
        os.environ["SPARK_REMOTE"] = url
        print("SPARK_REMOTE set; autopatch will install refresh interceptor on first gRPC call.")

        # Import dbt after env is set.
        from dbt.cli.main import dbtRunner
        result = dbtRunner().invoke([args.dbt_command, "--project-dir", args.project_dir])
        print(f"dbt {args.dbt_command} success={result.success}")
        sys.exit(0 if result.success else 1)
    finally:
        try:
            emrs.terminate_session(
                applicationId=args.application_id, sessionId=session_id
            )
            print(f"Terminated session {session_id}")
        except Exception as e:
            print(f"Warning: failed to terminate session {session_id}: {e}")


if __name__ == "__main__":
    main()
