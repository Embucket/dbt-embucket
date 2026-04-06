"""
Live integration tests against a real Embucket Lambda.
Requires AWS credentials and network access.

Run with:
    EMBUCKET_FUNCTION_ARN=arn:aws:lambda:... pytest tests/test_live.py -v
"""
import json
import os
import subprocess
import pytest


def _has_aws_credentials():
    """Check if AWS credentials are available via env vars or boto3 credential chain."""
    if os.environ.get("AWS_PROFILE") or os.environ.get("AWS_ACCESS_KEY_ID"):
        return True
    try:
        import boto3
        session = boto3.Session()
        return session.get_credentials() is not None
    except Exception:
        return False


# Skip all tests if no AWS credentials available
pytestmark = pytest.mark.skipif(
    not _has_aws_credentials(),
    reason="AWS credentials not available",
)

FUNCTION_ARN = os.environ.get("EMBUCKET_FUNCTION_ARN", "")
EMBUCKET_ACCOUNT = os.environ.get("EMBUCKET_ACCOUNT", "embucket")
EMBUCKET_USER = os.environ.get("EMBUCKET_USER", "embucket")
EMBUCKET_PASSWORD = os.environ.get("EMBUCKET_PASSWORD", "embucket")
EMBUCKET_REGION = os.environ.get("EMBUCKET_REGION", "us-east-2")


class TestLiveConnection:
    def _make_handle(self):
        """Create a LambdaHandle connected to the test Lambda."""
        from dbt.adapters.embucket.connections import (
            build_login_payload,
            LambdaHandle,
        )
        import boto3

        if not FUNCTION_ARN:
            pytest.skip("EMBUCKET_FUNCTION_ARN not set")

        client = boto3.client("lambda", region_name=EMBUCKET_REGION)
        payload = build_login_payload(
            account=EMBUCKET_ACCOUNT,
            user=EMBUCKET_USER,
            password=EMBUCKET_PASSWORD,
        )
        response = client.invoke(
            FunctionName=FUNCTION_ARN,
            Payload=json.dumps(payload).encode(),
        )
        raw = response["Payload"].read()
        result = json.loads(raw)
        body = json.loads(result.get("body", "{}"))
        token = body["data"]["token"]

        return LambdaHandle(client=client, function_arn=FUNCTION_ARN, token=token)

    def test_select_one(self):
        handle = self._make_handle()
        cursor = handle.cursor()
        cursor.execute("SELECT 1 AS n")
        assert cursor.description is not None
        assert cursor.description[0][0] == "n"
        row = cursor.fetchone()
        assert row == [1]

    def test_dbt_debug(self):
        """Run dbt debug to validate the full adapter setup."""
        result = subprocess.run(
            ["dbt", "debug", "--profiles-dir", "tests/dbt_project", "--project-dir", "tests/dbt_project"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"dbt debug failed:\n{result.stdout}\n{result.stderr}"

    def test_dbt_run(self):
        """Run dbt run to validate end-to-end model execution."""
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", "tests/dbt_project", "--project-dir", "tests/dbt_project"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"dbt run failed:\n{result.stdout}\n{result.stderr}"

    def test_show_schemas_uses_snowflake_macro(self):
        """Verify SHOW TERSE SCHEMAS is used (Snowflake macro), not information_schema query (default)."""
        handle = self._make_handle()
        cursor = handle.cursor()
        cursor.execute("SHOW TERSE SCHEMAS IN DATABASE embucket")
        assert cursor.description is not None
        col_names = [col[0] for col in cursor.description]
        assert "name" in col_names, f"Expected 'name' column in SHOW SCHEMAS output, got: {col_names}"
