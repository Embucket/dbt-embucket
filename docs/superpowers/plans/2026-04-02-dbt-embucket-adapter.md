# dbt-embucket Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a dbt adapter for Embucket that inherits from dbt-snowflake and uses AWS Lambda Invoke as the transport layer.

**Architecture:** Inherit SnowflakeAdapter/SnowflakeCredentials/SnowflakeConnectionManager. Replace the connection layer with a boto3 Lambda client that sends Snowflake V1 REST API payloads via `lambda.invoke()`. Reuse all Snowflake macros via dispatch config.

**Tech Stack:** Python 3.10+, dbt-snowflake, boto3, hatchling (build)

---

## File Structure

| File | Responsibility |
|------|---------------|
| `pyproject.toml` | Package metadata, dependencies (dbt-snowflake, boto3) |
| `src/dbt/adapters/embucket/__init__.py` | AdapterPlugin registration |
| `src/dbt/adapters/embucket/connections.py` | EmbucketCredentials, EmbucketConnectionManager, LambdaHandle, LambdaCursor |
| `src/dbt/adapters/embucket/impl.py` | EmbucketAdapter(SnowflakeAdapter) |
| `src/dbt/include/embucket/__init__.py` | PACKAGE_PATH |
| `src/dbt/include/embucket/dbt_project.yml` | Dispatch config: embucket → snowflake → dbt |
| `src/dbt/include/embucket/macros/.gitkeep` | Empty macros dir (overrides go here later) |
| `src/dbt/include/embucket/profiles.yml` | Profile template for `dbt init` |
| `tests/test_connections.py` | Unit tests for LambdaCursor response parsing |
| `tests/test_dispatch.py` | Macro dispatch resolution test |
| `tests/test_live.py` | Live integration tests against Lambda |

---

### Task 1: Project Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `src/dbt/include/embucket/__init__.py`
- Create: `src/dbt/include/embucket/dbt_project.yml`
- Create: `src/dbt/include/embucket/macros/.gitkeep`
- Create: `src/dbt/include/embucket/profiles.yml`

- [ ] **Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
dynamic = ["version"]
name = "dbt-embucket"
description = "The Embucket adapter plugin for dbt (Snowflake-compatible, AWS Lambda transport)"
readme = "README.md"
requires-python = ">=3.10.0"
authors = [{ name = "Embucket" }]
dependencies = [
    "dbt-snowflake>=1.11.0,<2.0",
    "boto3>=1.26.0",
]

[tool.hatch.version]
path = "src/dbt/adapters/embucket/__version__.py"

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-v --color=yes"
```

- [ ] **Step 2: Create include package init**

Create `src/dbt/include/embucket/__init__.py`:

```python
import os

PACKAGE_PATH = os.path.dirname(__file__)
```

- [ ] **Step 3: Create dbt_project.yml with dispatch config**

Create `src/dbt/include/embucket/dbt_project.yml`:

```yaml
config-version: 2
name: embucket
version: 0.1.0

macro-paths: ["macros"]

dispatch:
  - macro_namespace: dbt
    search_order: ['embucket', 'snowflake', 'dbt']
```

- [ ] **Step 4: Create macros dir and profiles template**

Create `src/dbt/include/embucket/macros/.gitkeep` (empty file).

Create `src/dbt/include/embucket/profiles.yml`:

```yaml
fixed:
  type: embucket
prompts:
  function_arn:
    hint: 'arn:aws:lambda:REGION:ACCOUNT:function:FUNCTION_NAME'
  account:
    hint: 'Embucket account name'
    default: 'embucket'
  user:
    hint: 'Embucket username'
    default: 'embucket'
  password:
    hint: 'Embucket password'
    hide_input: true
  database:
    hint: 'default database'
  schema:
    hint: 'default schema'
  threads:
    hint: '1 or more'
    type: 'int'
    default: 1
```

- [ ] **Step 5: Create version file**

Create `src/dbt/adapters/embucket/__version__.py`:

```python
version = "0.1.0"
```

- [ ] **Step 6: Commit scaffolding**

```bash
git add pyproject.toml src/dbt/include/embucket/ src/dbt/adapters/embucket/__version__.py
git commit -m "feat: add project scaffolding with dispatch config"
```

---

### Task 2: LambdaCursor and LambdaHandle

**Files:**
- Create: `src/dbt/adapters/embucket/connections.py`
- Create: `tests/test_connections.py`

This is the core transport layer. `LambdaHandle` manages the boto3 client and session token. `LambdaCursor` implements the DB-API 2.0 cursor interface that dbt expects (`.execute()`, `.fetchall()`, `.fetchmany()`, `.description`, `.rowcount`).

- [ ] **Step 1: Write failing tests for LambdaCursor response parsing**

Create `tests/test_connections.py`:

```python
import json
import pytest
from dbt.adapters.embucket.connections import LambdaCursor


class TestLambdaCursorParsing:
    """Test that LambdaCursor correctly parses Snowflake V1 JSON responses."""

    def _make_cursor(self):
        # Cursor without a real handle - for testing parse logic only
        return LambdaCursor(handle=None)

    def test_parse_select_response(self):
        response_body = json.dumps({
            "data": {
                "rowtype": [
                    {"name": "num", "database": "", "schema": "", "table": "",
                     "nullable": False, "type": "fixed", "byteLength": None,
                     "length": None, "scale": 0, "precision": 38, "collation": None},
                    {"name": "greeting", "database": "", "schema": "", "table": "",
                     "nullable": False, "type": "text", "byteLength": 16777216,
                     "length": 16777216, "scale": None, "precision": None, "collation": None},
                ],
                "rowsetBase64": None,
                "rowset": [[1, "hello"]],
                "total": 1,
                "returned": 1,
                "queryResultFormat": "json",
                "sqlState": "02000",
                "queryId": "test-query-id-001",
            },
            "success": True,
            "message": "successfully executed",
            "code": None,
        })
        cursor = self._make_cursor()
        cursor._parse_response(response_body)

        assert cursor.rowcount == 1
        assert cursor.sfqid == "test-query-id-001"
        assert cursor.sqlstate == "02000"
        assert len(cursor.description) == 2
        assert cursor.description[0][0] == "num"
        assert cursor.description[1][0] == "greeting"
        assert cursor.fetchall() == [[1, "hello"]]

    def test_parse_empty_response(self):
        response_body = json.dumps({
            "data": {
                "rowtype": [
                    {"name": "name", "database": "", "schema": "", "table": "",
                     "nullable": False, "type": "text", "byteLength": 16777216,
                     "length": 16777216, "scale": None, "precision": None, "collation": None},
                ],
                "rowsetBase64": None,
                "rowset": [],
                "total": 0,
                "returned": 0,
                "queryResultFormat": "json",
                "sqlState": "02000",
                "queryId": "test-query-id-002",
            },
            "success": True,
            "message": "successfully executed",
            "code": None,
        })
        cursor = self._make_cursor()
        cursor._parse_response(response_body)

        assert cursor.rowcount == 0
        assert cursor.fetchall() == []

    def test_parse_multi_row_response(self):
        response_body = json.dumps({
            "data": {
                "rowtype": [
                    {"name": "id", "database": "", "schema": "", "table": "",
                     "nullable": True, "type": "fixed", "byteLength": None,
                     "length": None, "scale": 0, "precision": 38, "collation": None},
                    {"name": "letter", "database": "", "schema": "", "table": "",
                     "nullable": True, "type": "text", "byteLength": 16777216,
                     "length": 16777216, "scale": None, "precision": None, "collation": None},
                ],
                "rowsetBase64": None,
                "rowset": [[1, "a"], [2, "b"], [3, "c"]],
                "total": 3,
                "returned": 3,
                "queryResultFormat": "json",
                "sqlState": "02000",
                "queryId": "test-query-id-003",
            },
            "success": True,
            "message": "successfully executed",
            "code": None,
        })
        cursor = self._make_cursor()
        cursor._parse_response(response_body)

        assert cursor.rowcount == 3
        rows = cursor.fetchall()
        assert len(rows) == 3
        assert rows[0] == [1, "a"]
        assert rows[2] == [3, "c"]

    def test_parse_error_response(self):
        response_body = json.dumps({
            "data": {
                "rowtype": [],
                "rowsetBase64": None,
                "rowset": None,
                "queryResultFormat": None,
                "errorCode": "002003",
                "sqlState": "02000",
                "queryId": "test-query-id-004",
            },
            "success": False,
            "message": "SQL compilation error: table 'demo.public.nonexistent' not found",
            "code": "002003",
        })
        cursor = self._make_cursor()
        with pytest.raises(Exception, match="table 'demo.public.nonexistent' not found"):
            cursor._parse_response(response_body)

    def test_parse_ddl_response(self):
        """DDL statements like CREATE TABLE return no rows but succeed."""
        response_body = json.dumps({
            "data": {
                "rowtype": [
                    {"name": "status", "database": "", "schema": "", "table": "",
                     "nullable": False, "type": "text", "byteLength": 16777216,
                     "length": 16777216, "scale": None, "precision": None, "collation": None},
                ],
                "rowsetBase64": None,
                "rowset": [["Table MY_TABLE successfully created."]],
                "total": 1,
                "returned": 1,
                "queryResultFormat": "json",
                "sqlState": "02000",
                "queryId": "test-query-id-005",
            },
            "success": True,
            "message": "successfully executed",
            "code": None,
        })
        cursor = self._make_cursor()
        cursor._parse_response(response_body)
        assert cursor.rowcount == 1
        assert cursor.fetchall() == [["Table MY_TABLE successfully created."]]

    def test_fetchmany(self):
        response_body = json.dumps({
            "data": {
                "rowtype": [
                    {"name": "id", "database": "", "schema": "", "table": "",
                     "nullable": False, "type": "fixed", "byteLength": None,
                     "length": None, "scale": 0, "precision": 38, "collation": None},
                ],
                "rowsetBase64": None,
                "rowset": [[1], [2], [3], [4], [5]],
                "total": 5,
                "returned": 5,
                "queryResultFormat": "json",
                "sqlState": "02000",
                "queryId": "test-query-id-006",
            },
            "success": True,
            "message": "successfully executed",
            "code": None,
        })
        cursor = self._make_cursor()
        cursor._parse_response(response_body)
        batch = cursor.fetchmany(2)
        assert batch == [[1], [2]]
        batch = cursor.fetchmany(2)
        assert batch == [[3], [4]]
        batch = cursor.fetchmany(2)
        assert batch == [[5]]
        batch = cursor.fetchmany(2)
        assert batch == []

    def test_parse_null_values(self):
        response_body = json.dumps({
            "data": {
                "rowtype": [
                    {"name": "val", "database": "", "schema": "", "table": "",
                     "nullable": True, "type": "text", "byteLength": None,
                     "length": None, "scale": None, "precision": None, "collation": None},
                ],
                "rowsetBase64": None,
                "rowset": [[None]],
                "total": 1,
                "returned": 1,
                "queryResultFormat": "json",
                "sqlState": "02000",
                "queryId": "test-query-id-007",
            },
            "success": True,
            "message": "successfully executed",
            "code": None,
        })
        cursor = self._make_cursor()
        cursor._parse_response(response_body)
        assert cursor.fetchall() == [[None]]


class TestLambdaInvokePayload:
    """Test that LambdaCursor builds correct Lambda invoke payloads."""

    def test_build_login_payload(self):
        from dbt.adapters.embucket.connections import build_login_payload
        payload = build_login_payload(
            account="embucket",
            user="embucket",
            password="embucket",
            database="demo",
            schema="public",
        )
        assert payload["version"] == "2.0"
        assert payload["rawPath"] == "/session/v1/login-request"
        assert payload["requestContext"]["http"]["method"] == "POST"
        assert payload["headers"]["x-forwarded-for"] == "127.0.0.1"
        body = json.loads(payload["body"])
        assert body["data"]["ACCOUNT_NAME"] == "embucket"
        assert body["data"]["LOGIN_NAME"] == "embucket"
        assert body["data"]["PASSWORD"] == "embucket"
        assert body["data"]["CLIENT_APP_ID"] == "dbt-embucket"

    def test_build_query_payload(self):
        from dbt.adapters.embucket.connections import build_query_payload
        payload = build_query_payload(
            sql="SELECT 1",
            token="my-jwt-token",
            sequence_id=1,
        )
        assert payload["version"] == "2.0"
        assert payload["rawPath"] == "/queries/v1/query-request"
        assert "requestId=" in payload["rawQueryString"]
        assert payload["headers"]["authorization"] == 'Snowflake Token="my-jwt-token"'
        body = json.loads(payload["body"])
        assert body["sqlText"] == "SELECT 1"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/ramp/vcs/dbt-embucket && pip install -e ".[dev]" 2>/dev/null; pytest tests/test_connections.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement connections.py**

Create `src/dbt/adapters/embucket/connections.py`:

```python
import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
from botocore.config import Config as BotoConfig

from dbt.adapters.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.snowflake.connections import SnowflakeConnectionManager, SnowflakeCredentials
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

logger = AdapterLogger("Embucket")

# Lambda invoke read timeout: generous to cover long-running queries.
# Lambda max execution is 900s; add buffer.
LAMBDA_READ_TIMEOUT_SECONDS = 960


def build_login_payload(
    account: str,
    user: str,
    password: str,
    database: Optional[str] = None,
    schema: Optional[str] = None,
) -> dict:
    """Build a Lambda Function URL event for the Embucket login endpoint."""
    query_parts = []
    if database:
        query_parts.append(f"databaseName={database}")
    if schema:
        query_parts.append(f"schemaName={schema}")

    body = json.dumps({
        "data": {
            "ACCOUNT_NAME": account,
            "LOGIN_NAME": user,
            "PASSWORD": password,
            "CLIENT_APP_ID": "dbt-embucket",
            "CLIENT_APP_VERSION": "0.1.0",
            "CLIENT_ENVIRONMENT": {},
            "SESSION_PARAMETERS": {},
        }
    })
    return {
        "version": "2.0",
        "rawPath": "/session/v1/login-request",
        "rawQueryString": "&".join(query_parts),
        "requestContext": {
            "http": {
                "method": "POST",
                "path": "/session/v1/login-request",
                "sourceIp": "127.0.0.1",
            },
            "accountId": "anonymous",
            "apiId": "anonymous",
        },
        "headers": {
            "content-type": "application/json",
            "x-forwarded-for": "127.0.0.1",
            "host": "localhost",
        },
        "body": body,
        "isBase64Encoded": False,
    }


def build_query_payload(sql: str, token: str, sequence_id: int = 0) -> dict:
    """Build a Lambda Function URL event for the Embucket query endpoint."""
    request_id = str(uuid.uuid4())
    body = json.dumps({"sqlText": sql})
    return {
        "version": "2.0",
        "rawPath": "/queries/v1/query-request",
        "rawQueryString": f"requestId={request_id}",
        "requestContext": {
            "http": {
                "method": "POST",
                "path": "/queries/v1/query-request",
                "sourceIp": "127.0.0.1",
            },
            "accountId": "anonymous",
            "apiId": "anonymous",
        },
        "headers": {
            "content-type": "application/json",
            "x-forwarded-for": "127.0.0.1",
            "host": "localhost",
            "authorization": f'Snowflake Token="{token}"',
        },
        "body": body,
        "isBase64Encoded": False,
    }


class LambdaCursor:
    """A DB-API 2.0-like cursor that executes SQL via Lambda invoke."""

    def __init__(self, handle: Optional["LambdaHandle"]):
        self._handle = handle
        self.description: Optional[List[Tuple]] = None
        self.rowcount: int = -1
        self.sfqid: Optional[str] = None
        self.sqlstate: Optional[str] = None
        self._rows: List[List] = []
        self._row_index: int = 0

    def execute(self, sql: str, bindings: Optional[Any] = None) -> None:
        """Execute SQL by invoking the Lambda function."""
        if self._handle is None:
            raise DbtRuntimeError("Cannot execute: no connection handle")

        payload = build_query_payload(
            sql=sql,
            token=self._handle.token,
        )
        response = self._handle.invoke(payload)
        self._parse_response(response)

    def _parse_response(self, response_body: str) -> None:
        """Parse a Snowflake V1 JSON response into cursor state."""
        result = json.loads(response_body)

        if not result.get("success", False):
            msg = result.get("message", "Unknown error from Embucket")
            raise DbtDatabaseError(msg)

        data = result.get("data", {})
        self.sfqid = data.get("queryId")
        self.sqlstate = data.get("sqlState")

        # Parse column metadata into DB-API 2.0 description format:
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        rowtype = data.get("rowtype", [])
        self.description = [
            (
                col["name"],
                col.get("type", "text"),
                None,
                col.get("byteLength"),
                col.get("precision"),
                col.get("scale"),
                col.get("nullable", True),
            )
            for col in rowtype
        ]

        self._rows = data.get("rowset") or []
        self._row_index = 0
        self.rowcount = data.get("total", len(self._rows))

    def fetchall(self) -> List[List]:
        rows = self._rows[self._row_index:]
        self._row_index = len(self._rows)
        return rows

    def fetchmany(self, size: int = 1) -> List[List]:
        end = min(self._row_index + size, len(self._rows))
        rows = self._rows[self._row_index:end]
        self._row_index = end
        return rows

    def fetchone(self) -> Optional[List]:
        if self._row_index < len(self._rows):
            row = self._rows[self._row_index]
            self._row_index += 1
            return row
        return None

    def close(self) -> None:
        pass


class LambdaHandle:
    """Wraps a boto3 Lambda client and session token."""

    def __init__(self, client: Any, function_arn: str, token: str):
        self.client = client
        self.function_arn = function_arn
        self.token = token

    def cursor(self) -> LambdaCursor:
        return LambdaCursor(handle=self)

    def invoke(self, payload: dict) -> str:
        """Invoke the Lambda function and return the response body string."""
        response = self.client.invoke(
            FunctionName=self.function_arn,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )

        response_payload = json.loads(response["Payload"].read())

        if response.get("FunctionError"):
            error_msg = response_payload.get("errorMessage", "Lambda invocation failed")
            raise DbtRuntimeError(f"Lambda function error: {error_msg}")

        status_code = response_payload.get("statusCode", 200)
        if status_code >= 400:
            body = response_payload.get("body", "")
            raise DbtRuntimeError(
                f"Embucket returned HTTP {status_code}: {body}"
            )

        return response_payload.get("body", "{}")

    def close(self) -> None:
        pass


@dataclass
class EmbucketCredentials(SnowflakeCredentials):
    function_arn: str = ""

    def __post_init__(self):
        # Skip SnowflakeCredentials validation that doesn't apply
        pass

    @property
    def type(self):
        return "embucket"

    @property
    def unique_field(self):
        return self.function_arn

    def _connection_keys(self):
        return (
            "function_arn",
            "account",
            "user",
            "database",
            "schema",
        )


class EmbucketConnectionManager(SnowflakeConnectionManager):
    TYPE = "embucket"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds: EmbucketCredentials = connection.credentials

        # Extract region from ARN: arn:aws:lambda:REGION:ACCOUNT:function:NAME
        arn_parts = creds.function_arn.split(":")
        region = arn_parts[3] if len(arn_parts) > 3 else "us-east-1"

        boto_config = BotoConfig(
            region_name=region,
            read_timeout=LAMBDA_READ_TIMEOUT_SECONDS,
            retries={"max_attempts": 0},
        )
        lambda_client = boto3.client("lambda", config=boto_config)

        # Login to get session token
        login_payload = build_login_payload(
            account=creds.account,
            user=creds.user or "embucket",
            password=creds.password or "embucket",
            database=creds.database,
            schema=creds.schema,
        )

        login_response = lambda_client.invoke(
            FunctionName=creds.function_arn,
            InvocationType="RequestResponse",
            Payload=json.dumps(login_payload).encode("utf-8"),
        )
        login_result = json.loads(login_response["Payload"].read())

        if login_response.get("FunctionError"):
            raise DbtRuntimeError(
                f"Embucket login failed: {login_result.get('errorMessage', 'unknown error')}"
            )

        login_body = json.loads(login_result.get("body", "{}"))
        if not login_body.get("success", False):
            raise DbtRuntimeError(
                f"Embucket login failed: {login_body.get('message', 'unknown error')}"
            )

        token = login_body["data"]["token"]

        handle = LambdaHandle(
            client=lambda_client,
            function_arn=creds.function_arn,
            token=token,
        )

        connection.handle = handle
        connection.state = "open"
        return connection

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except DbtDatabaseError:
            raise
        except DbtRuntimeError:
            raise
        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            raise DbtRuntimeError(str(e)) from e

    def cancel(self, connection):
        logger.debug("Cancel not supported for Embucket Lambda connections")

    @classmethod
    def get_response(cls, cursor: LambdaCursor) -> AdapterResponse:
        code = cursor.sqlstate or "SUCCESS"
        return AdapterResponse(
            _message=f"{code} {cursor.rowcount}",
            rows_affected=cursor.rowcount,
            code=code,
            query_id=cursor.sfqid,
        )

    @classmethod
    def _split_queries(cls, sql):
        """Split SQL at semicolons without using snowflake-connector utilities."""
        # Simple split that handles basic semicolon separation
        queries = []
        current = []
        in_string = False
        string_char = None

        for char in sql:
            if in_string:
                current.append(char)
                if char == string_char:
                    in_string = False
            elif char in ("'", '"'):
                in_string = True
                string_char = char
                current.append(char)
            elif char == ";":
                query = "".join(current).strip()
                if query:
                    queries.append(query)
                current = []
            else:
                current.append(char)

        # Don't forget the last query (may not end with semicolon)
        last = "".join(current).strip()
        if last:
            queries.append(last)

        return queries

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        # Embucket returns string type names directly
        if isinstance(type_code, str):
            return type_code
        return str(type_code)

    def release(self):
        """Release the connection."""
        super(SnowflakeConnectionManager, self).release()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/ramp/vcs/dbt-embucket && pip install -e . && pytest tests/test_connections.py -v`
Expected: All 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/dbt/adapters/embucket/connections.py tests/test_connections.py
git commit -m "feat: add LambdaCursor, LambdaHandle, and EmbucketCredentials"
```

---

### Task 3: EmbucketAdapter and Plugin Registration

**Files:**
- Create: `src/dbt/adapters/embucket/__init__.py`
- Create: `src/dbt/adapters/embucket/impl.py`

- [ ] **Step 1: Create impl.py**

Create `src/dbt/adapters/embucket/impl.py`:

```python
from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.embucket.connections import EmbucketConnectionManager


class EmbucketAdapter(SnowflakeAdapter):
    ConnectionManager = EmbucketConnectionManager

    @classmethod
    def date_function(cls):
        return "CURRENT_TIMESTAMP()"

    def submit_python_job(self, parsed_model, compiled_code):
        raise NotImplementedError("Python models are not supported on Embucket")
```

- [ ] **Step 2: Create __init__.py with plugin registration**

Create `src/dbt/adapters/embucket/__init__.py`:

```python
from dbt.adapters.embucket.connections import EmbucketConnectionManager
from dbt.adapters.embucket.connections import EmbucketCredentials
from dbt.adapters.embucket.impl import EmbucketAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import embucket

Plugin = AdapterPlugin(
    adapter=EmbucketAdapter,
    credentials=EmbucketCredentials,
    include_path=embucket.PACKAGE_PATH,
)
```

- [ ] **Step 3: Verify plugin loads**

Run: `cd /Users/ramp/vcs/dbt-embucket && pip install -e . && python -c "from dbt.adapters.embucket import Plugin; print('Plugin type:', Plugin.adapter.ConnectionManager.TYPE)"`
Expected output: `Plugin type: embucket`

- [ ] **Step 4: Commit**

```bash
git add src/dbt/adapters/embucket/__init__.py src/dbt/adapters/embucket/impl.py
git commit -m "feat: add EmbucketAdapter and plugin registration"
```

---

### Task 4: Macro Dispatch Verification

**Files:**
- Create: `tests/test_dispatch.py`

This is a critical test. We must verify that Snowflake macros are used, NOT dbt defaults.

- [ ] **Step 1: Write dispatch test**

Create `tests/test_dispatch.py`:

```python
"""
Verify that the embucket adapter dispatches to snowflake macros, not dbt defaults.

dbt's macro resolution for adapter type 'embucket' would normally go:
  embucket__X → default__X (skipping snowflake)

Our dbt_project.yml dispatch config should make it:
  embucket__X → snowflake__X → default__X
"""
import os
import pytest
import yaml


def test_dispatch_config_exists():
    """The dbt_project.yml must have dispatch config routing to snowflake."""
    from dbt.include import embucket
    project_path = os.path.join(embucket.PACKAGE_PATH, "dbt_project.yml")
    with open(project_path) as f:
        project = yaml.safe_load(f)

    dispatch = project.get("dispatch", [])
    assert len(dispatch) > 0, "No dispatch config found in dbt_project.yml"

    dbt_dispatch = next(
        (d for d in dispatch if d.get("macro_namespace") == "dbt"),
        None,
    )
    assert dbt_dispatch is not None, "No dispatch config for 'dbt' namespace"

    search_order = dbt_dispatch.get("search_order", [])
    assert "snowflake" in search_order, (
        f"'snowflake' not in search_order: {search_order}"
    )
    assert search_order.index("embucket") < search_order.index("snowflake"), (
        "embucket must come before snowflake in search_order"
    )
    assert search_order.index("snowflake") < search_order.index("dbt"), (
        "snowflake must come before dbt in search_order"
    )


def test_snowflake_macros_available():
    """Snowflake adapter macros should be importable (dependency is installed)."""
    from dbt.include import snowflake
    macros_path = os.path.join(snowflake.PACKAGE_PATH, "macros")
    assert os.path.isdir(macros_path), f"Snowflake macros dir not found at {macros_path}"

    # Check that key macro files exist
    expected_macros = ["adapters.sql"]
    for macro_file in expected_macros:
        macro_path = os.path.join(macros_path, macro_file)
        assert os.path.isfile(macro_path), f"Snowflake macro {macro_file} not found"


def test_snowflake_macros_define_key_dispatches():
    """Key Snowflake macros should define snowflake__ implementations."""
    from dbt.include import snowflake
    adapters_sql = os.path.join(snowflake.PACKAGE_PATH, "macros", "adapters.sql")
    with open(adapters_sql) as f:
        content = f.read()

    # These are macros where we specifically need the snowflake version, not default
    key_macros = [
        "snowflake__list_schemas",
        "snowflake__get_columns_in_relation",
    ]
    for macro_name in key_macros:
        assert macro_name in content, (
            f"Snowflake macro '{macro_name}' not found in adapters.sql - "
            "dispatch would fall through to dbt default"
        )
```

- [ ] **Step 2: Run dispatch tests**

Run: `cd /Users/ramp/vcs/dbt-embucket && pytest tests/test_dispatch.py -v`
Expected: All 3 tests PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_dispatch.py
git commit -m "test: add macro dispatch verification tests"
```

---

### Task 5: Live Integration Test

**Files:**
- Create: `tests/test_live.py`
- Create: `tests/dbt_project/` (test dbt project)

Test against the real Lambda: `embucket-lambda-sturukin_10g` in us-east-2.

- [ ] **Step 1: Create a minimal test dbt project**

Create `tests/dbt_project/dbt_project.yml`:

```yaml
name: embucket_test
version: 0.1.0
config-version: 2
profile: embucket_test

dispatch:
  - macro_namespace: dbt
    search_order: ['embucket', 'snowflake', 'dbt']
```

Create `tests/dbt_project/profiles.yml`:

```yaml
embucket_test:
  target: dev
  outputs:
    dev:
      type: embucket
      function_arn: arn:aws:lambda:us-east-2:767397688925:function:embucket-lambda-sturukin_10g
      account: embucket
      user: embucket
      password: embucket
      database: embucket
      schema: public
      threads: 1
```

Note: The account ID `767397688925` in the ARN should be verified from the actual Lambda. The test uses the sturukin_10g function.

Create `tests/dbt_project/models/test_model.sql`:

```sql
select 1 as id, 'hello' as greeting
```

- [ ] **Step 2: Write live integration test**

Create `tests/test_live.py`:

```python
"""
Live integration tests against a real Embucket Lambda.
Requires AWS credentials and network access.

Run with: pytest tests/test_live.py -v -k live
"""
import json
import os
import subprocess
import pytest

# Skip all tests if no AWS credentials available
pytestmark = pytest.mark.skipif(
    not os.environ.get("AWS_PROFILE") and not os.environ.get("AWS_ACCESS_KEY_ID"),
    reason="AWS credentials not available",
)

FUNCTION_ARN = os.environ.get(
    "EMBUCKET_FUNCTION_ARN",
    "arn:aws:lambda:us-east-2:767397688925:function:embucket-lambda-sturukin_10g",
)


class TestLiveConnection:
    def test_connection_open_and_query(self):
        """Test that we can open a connection and run a simple query."""
        from dbt.adapters.embucket.connections import (
            EmbucketCredentials,
            EmbucketConnectionManager,
            LambdaHandle,
            build_login_payload,
        )
        import boto3
        from botocore.config import Config as BotoConfig

        arn_parts = FUNCTION_ARN.split(":")
        region = arn_parts[3]

        boto_config = BotoConfig(region_name=region, read_timeout=960)
        client = boto3.client("lambda", config=boto_config)

        # Login
        login_payload = build_login_payload(
            account="embucket",
            user="embucket",
            password="embucket",
        )
        response = client.invoke(
            FunctionName=FUNCTION_ARN,
            InvocationType="RequestResponse",
            Payload=json.dumps(login_payload).encode("utf-8"),
        )
        result = json.loads(response["Payload"].read())
        body = json.loads(result["body"])
        assert body["success"] is True
        token = body["data"]["token"]

        # Query
        handle = LambdaHandle(client=client, function_arn=FUNCTION_ARN, token=token)
        cursor = handle.cursor()
        cursor.execute("SELECT 1 as num, 'hello' as greeting")
        assert cursor.rowcount == 1
        rows = cursor.fetchall()
        assert rows == [[1, "hello"]]
        assert cursor.description[0][0] == "num"
        assert cursor.description[1][0] == "greeting"

    def test_dbt_debug(self):
        """Test that `dbt debug` succeeds against the live Lambda."""
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
        """Test that `dbt run` succeeds with a simple model."""
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
        from dbt.adapters.embucket.connections import (
            build_login_payload,
            build_query_payload,
            LambdaHandle,
        )
        import boto3
        from botocore.config import Config as BotoConfig

        arn_parts = FUNCTION_ARN.split(":")
        region = arn_parts[3]

        boto_config = BotoConfig(region_name=region, read_timeout=960)
        client = boto3.client("lambda", config=boto_config)

        # Login
        login_payload = build_login_payload(
            account="embucket", user="embucket", password="embucket",
            database="embucket",
        )
        response = client.invoke(
            FunctionName=FUNCTION_ARN,
            InvocationType="RequestResponse",
            Payload=json.dumps(login_payload).encode("utf-8"),
        )
        result = json.loads(response["Payload"].read())
        body = json.loads(result["body"])
        token = body["data"]["token"]

        # Run SHOW TERSE SCHEMAS (what the snowflake macro does)
        handle = LambdaHandle(client=client, function_arn=FUNCTION_ARN, token=token)
        cursor = handle.cursor()
        cursor.execute("SHOW TERSE SCHEMAS IN DATABASE embucket")
        assert cursor.description is not None
        col_names = [col[0] for col in cursor.description]
        # Snowflake's SHOW SCHEMAS returns columns like: created_on, name, kind, database_name, schema_name
        assert "name" in col_names, f"Expected 'name' column in SHOW SCHEMAS output, got: {col_names}"
```

- [ ] **Step 3: Verify the Lambda ARN is correct**

Run: `aws lambda get-function --function-name embucket-lambda-sturukin_10g --region us-east-2 --query 'Configuration.FunctionArn' --output text`
Use the output to fix the ARN in `tests/dbt_project/profiles.yml` and `tests/test_live.py` if needed.

- [ ] **Step 4: Run live tests**

Run: `cd /Users/ramp/vcs/dbt-embucket && pytest tests/test_live.py -v --timeout=120`
Expected: All tests PASS (connection, dbt debug, dbt run, schema macro verification)

- [ ] **Step 5: Commit**

```bash
git add tests/test_live.py tests/dbt_project/
git commit -m "test: add live integration tests against Embucket Lambda"
```

---

### Task 6: Final Verification and Cleanup

**Files:**
- Modify: various (fix any issues found)

- [ ] **Step 1: Run full test suite**

Run: `cd /Users/ramp/vcs/dbt-embucket && pytest tests/ -v --timeout=120`
Expected: All tests PASS

- [ ] **Step 2: Verify package installs cleanly**

Run: `pip install -e . && python -c "from dbt.adapters.embucket import Plugin; print('OK:', Plugin.adapter.ConnectionManager.TYPE)"`
Expected: `OK: embucket`

- [ ] **Step 3: Test dbt debug end-to-end**

Run: `cd /Users/ramp/vcs/dbt-embucket/tests/dbt_project && dbt debug --profiles-dir . --project-dir .`
Expected: All checks passed

- [ ] **Step 4: Test dbt run end-to-end**

Run: `cd /Users/ramp/vcs/dbt-embucket/tests/dbt_project && dbt run --profiles-dir . --project-dir .`
Expected: Model runs successfully

- [ ] **Step 5: Final commit if any fixes were needed**

```bash
git add -A && git commit -m "fix: address issues found during final verification"
```
