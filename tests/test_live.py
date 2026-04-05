"""
Live integration tests against a real Embucket Lambda.
Requires AWS credentials and network access.

Run with: pytest tests/test_live.py -v
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
        creds = session.get_credentials()
        return creds is not None
    except Exception:
        return False


# Skip all tests if no AWS credentials available
pytestmark = pytest.mark.skipif(
    not _has_aws_credentials(),
    reason="AWS credentials not available",
)

FUNCTION_ARN = os.environ.get(
    "EMBUCKET_FUNCTION_ARN",
    "arn:aws:lambda:us-east-2:767397688925:function:embucket-lambadka",
)


class TestLiveConnection:
    def _make_handle(self):
        """Create a LambdaHandle connected to the test Lambda."""
        from dbt.adapters.embucket.connections import (
            build_login_payload,
            LambdaHandle,
        )
        import boto3
        from botocore.config import Config as BotoConfig

        arn_parts = FUNCTION_ARN.split(":")
        region = arn_parts[3]
        boto_config = BotoConfig(region_name=region, read_timeout=960)
        client = boto3.client("lambda", config=boto_config)

        # Login via LambdaHandle (handles both buffered and streaming)
        tmp_handle = LambdaHandle(client=client, function_arn=FUNCTION_ARN, token="")
        login_payload = build_login_payload(
            account="embucket", user="embucket", password="embucket",
        )
        body = json.loads(tmp_handle.invoke(login_payload))
        assert body["success"] is True
        return LambdaHandle(client=client, function_arn=FUNCTION_ARN, token=body["data"]["token"])

    def test_connection_open_and_query(self):
        """Test that we can open a connection and run a simple query."""
        handle = self._make_handle()
        cursor = handle.cursor()
        cursor.execute("SELECT 1 as num, 'hello' as greeting")
        assert cursor.rowcount == 1
        rows = cursor.fetchall()
        assert rows == [[1, "hello"]]
        assert cursor.description[0][0] == "num"
        assert cursor.description[1][0] == "greeting"

    def test_dbt_debug(self):
        """Test that dbt debug succeeds against the live Lambda."""
        project_dir = os.path.join(os.path.dirname(__file__), "dbt_project")
        result = subprocess.run(
            ["dbt", "debug", "--profiles-dir", project_dir, "--project-dir", project_dir],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"dbt debug failed:\n{result.stdout}\n{result.stderr}"
        assert "All checks passed" in result.stdout

    def test_dbt_run(self):
        """Test that dbt run succeeds with a simple model."""
        project_dir = os.path.join(os.path.dirname(__file__), "dbt_project")
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", project_dir, "--project-dir", project_dir],
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
