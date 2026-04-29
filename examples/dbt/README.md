# dbt on EMR Serverless with Spark Connect

This example shows how to run **dbt-core** against an **Amazon EMR Serverless** application using Spark Connect, with automatic auth token refresh so long-running dbt projects survive past the 1-hour token TTL.

## The problem

EMR Serverless Spark Connect sessions authenticate each gRPC request with an `x-aws-proxy-auth` token embedded in the `SPARK_REMOTE` URL. The token expires after **1 hour**. Stock PySpark has no mechanism to refresh it, so a dbt project that runs longer than ~60 minutes fails mid-run with `StatusCode.UNKNOWN / "Stream removed"`.

EMR Serverless sessions themselves live up to 24 hours — it's only the auth token that's short-lived.

## The solution

Two small Python modules bundled in this example:

1. **`emrs_custom_spark_connect/`** — a gRPC channel interceptor that, before each outbound RPC, checks token expiry and calls `GetSessionEndpoint` for a fresh token when needed.
2. **`emrs_sparkconnect_autopatch.py`** — a ~50-line patch that plugs the interceptor into PySpark's default Spark Connect channel builder, so stock clients like dbt (which don't know anything about EMR-S) get transparent token refresh just by importing this module.

The SparkConnect session stays alive across token rotations — no teardown, no state loss. dbt-spark is stock, unmodified.

## Quick start

### 1. Install dependencies

```bash
pip install dbt-core "dbt-spark[session]" "pyspark[connect]>=3.5" boto3
# Python 3.12+ also needs:
pip install "setuptools<81"
```

### 2. Configure AWS credentials

Either via `~/.aws/credentials`, `AWS_PROFILE` env var, or instance/task role.

### 3. Make sure your EMR Serverless application has sessions enabled

Interactive sessions must be enabled on the application. The simplest path is to create the application with `sessionEnabled=true`:

```bash
aws emr-serverless create-application \\
    --name dbt-spark-connect \\
    --release-label emr-7.13.0 \\
    --type SPARK \\
    --interactive-configuration '{"sessionEnabled":true}'
```

To verify on an existing application:

```bash
aws emr-serverless get-application \\
    --application-id <APP_ID> \\
    --query 'application.interactiveConfiguration'
```

If the response does not show `"sessionEnabled": true`, stop the application and run `update-application` with the same `--interactive-configuration '{"sessionEnabled":true}'` before proceeding.

### 4. Run dbt

```bash
python run_dbt.py \\
    --application-id <APP_ID> \\
    --execution-role-arn <ROLE_ARN> \\
    --region us-east-1
```

The script starts an EMR-S session, waits for it to be ready, sets `SPARK_REMOTE`, invokes dbt, and terminates the session on exit.

## How customers adapt this to their own projects

The bundled shim and interceptor assume **zero changes** to your dbt project. To use the pattern with your own dbt models:

1. Copy both `emrs_sparkconnect_autopatch.py` and the `emrs_custom_spark_connect/` directory into your Python path.
2. In your dbt runner script, `import emrs_sparkconnect_autopatch` **before** any pyspark import and before invoking dbt.
3. Set env vars before running dbt:
   - `SPARK_REMOTE` — the EMR-S Spark Connect URL (returned by `GetSessionEndpoint`)
   - `EMRS_APPLICATION_ID` — used by the interceptor to refresh the token
   - `AWS_REGION` — used by the boto3 client
4. Your `profiles.yml` uses `type: spark`, `method: session`, `host: localhost` (stock dbt-spark config).

No dbt-spark fork, no dbt adapter changes, no custom connection method.

## Important notes

### Session idle timeout

By default EMR-S sessions auto-terminate after 15 minutes of idle time. If your dbt project has gaps longer than that (e.g., waiting on upstream data), set `idleTimeoutMinutes` high enough to cover the longest gap. The example uses 120 minutes.

### `enableHiveSupport()` is a no-op in Spark Connect mode

When `SPARK_REMOTE` is set, PySpark's `SparkSession.builder.enableHiveSupport().getOrCreate()` routes through the Spark Connect client, which does not honor `enableHiveSupport()`. You'll see a harmless warning:

```
UserWarning: Cannot modify the value of a static config: spark.sql.catalogImplementation.
```

Consequence: `CREATE TABLE AS SELECT` without an explicit `file_format` fails with `NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT`. Either:

- Add `{{ config(file_format='parquet') }}` to each table model, or
- Set `spark.sql.catalogImplementation=hive` at the EMR-S application level (application-wide runtime config)

View materializations are unaffected.

### Session lifecycle is your responsibility

`run_dbt.py` starts and terminates the EMR-S session for each invocation. For production use, you'll likely want to reuse a session across multiple dbt invocations (run, test, snapshot) to avoid startup overhead. The same env vars work — just keep the session alive between calls.

## Validated

| Elapsed since session start | dbt run result |
|---|---|
| t+0 min | ✅ PASS |
| t+55 min | ✅ PASS |
| **t+62 min (past 1-hr token TTL)** | ✅ **PASS** |
| t+68 min | ✅ PASS |

Without the autopatch shim, the same test fails at t+62 min with `StatusCode.UNKNOWN / "Stream removed" / "Received incorrect session identifier"`.

## Files

| File | Purpose |
|---|---|
| `emrs_custom_spark_connect/` | gRPC interceptor + Spark Connect session helper (bundled temporarily; will be a separate pip package) |
| `emrs_sparkconnect_autopatch.py` | Patch that plugs the interceptor into stock PySpark |
| `run_dbt.py` | End-to-end runner — starts session, runs dbt, tears down |
| `profiles.yml` | Sample dbt profile (type: spark, method: session) |
| `dbt_project/` | Minimal dbt project with one view model |
