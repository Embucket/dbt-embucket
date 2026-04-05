import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Tuple, Union

import boto3
from botocore.config import Config as BotoConfig

from dbt.adapters.contracts.connection import AdapterResponse, Connection
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.snowflake.connections import SnowflakeConnectionManager, SnowflakeCredentials
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt.adapters.embucket.__version__ import version as adapter_version

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
            "CLIENT_APP_VERSION": adapter_version,
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


def build_query_payload(sql: str, token: str) -> dict:
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

        # Build type map for coercion (timestamp strings -> datetime, etc.)
        type_map = {}
        for i, col in enumerate(rowtype):
            col_type = col.get("type", "text")
            if col_type in ("timestamp_ntz", "timestamp_ltz", "timestamp_tz", "timestamp"):
                type_map[i] = "timestamp"
            elif col_type == "boolean":
                type_map[i] = "boolean"

        raw_rows = data.get("rowset") or []
        self._rows = [self._coerce_row(row, type_map) for row in raw_rows]
        self._row_index = 0
        self.rowcount = data.get("total", len(self._rows))

    @staticmethod
    def _coerce_row(row: List, type_map: dict) -> List:
        """Coerce JSON values to Python types based on column metadata."""
        if not type_map:
            return row
        coerced = list(row)
        for i, target_type in type_map.items():
            if i >= len(coerced) or coerced[i] is None:
                continue
            val = coerced[i]
            if target_type == "timestamp" and isinstance(val, str):
                try:
                    # Embucket returns timestamps as epoch seconds (e.g. "1757843994.302000")
                    epoch = float(val)
                    coerced[i] = datetime.utcfromtimestamp(epoch)
                except (ValueError, OSError):
                    # Fall back to ISO format parsing
                    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                        try:
                            coerced[i] = datetime.strptime(val, fmt)
                            break
                        except ValueError:
                            continue
            elif target_type == "boolean" and isinstance(val, str):
                coerced[i] = val.upper() in ("TRUE", "1", "T")
        return coerced

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
        """Invoke the Lambda function and return the response body string.

        Handles both buffered and streaming Lambda response formats:
        - Buffered: single JSON with statusCode, headers, body
        - Streaming: HTTP metadata JSON + null bytes + body JSON
        """
        response = self.client.invoke(
            FunctionName=self.function_arn,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )

        raw = response["Payload"].read()

        if response.get("FunctionError"):
            try:
                err = json.loads(raw)
                error_msg = err.get("errorMessage", "Lambda invocation failed")
            except (json.JSONDecodeError, ValueError):
                error_msg = raw.decode("utf-8", errors="replace")[:200]
            raise DbtRuntimeError(f"Lambda function error: {error_msg}")

        # Try parsing as buffered response first
        try:
            response_payload = json.loads(raw)
            status_code = response_payload.get("statusCode", 200)
            if status_code >= 400:
                body = response_payload.get("body", "")
                raise DbtRuntimeError(f"Embucket returned HTTP {status_code}: {body}")
            return response_payload.get("body", "{}")
        except json.JSONDecodeError:
            pass

        # Streaming response: metadata + null bytes + body
        separator = b"\x00" * 8
        if separator in raw:
            parts = raw.split(separator, 1)
            metadata = json.loads(parts[0])
            status_code = metadata.get("statusCode", 200)
            body_str = parts[1].decode("utf-8") if len(parts) > 1 else "{}"
            if status_code >= 400:
                raise DbtRuntimeError(f"Embucket returned HTTP {status_code}: {body_str[:200]}")
            return body_str

        raise DbtRuntimeError(f"Unexpected Lambda response format: {raw[:200]}")

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

        if not creds.function_arn:
            raise DbtRuntimeError("'function_arn' is required in the profile configuration")

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

        # Use a temporary handle to perform login (reuses invoke response parsing)
        tmp_handle = LambdaHandle(
            client=lambda_client, function_arn=creds.function_arn, token=""
        )
        login_body_str = tmp_handle.invoke(login_payload)
        login_body = json.loads(login_body_str)
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
            logger.debug(f"Error running SQL: {sql}")
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
        queries = []
        current = []
        in_string = False
        string_char = None
        i = 0
        length = len(sql)

        while i < length:
            char = sql[i]

            if in_string:
                current.append(char)
                if char == string_char:
                    in_string = False
                i += 1
            elif char in ("'", '"'):
                in_string = True
                string_char = char
                current.append(char)
                i += 1
            elif char == "-" and i + 1 < length and sql[i + 1] == "-":
                # Line comment: consume until end of line
                while i < length and sql[i] != "\n":
                    current.append(sql[i])
                    i += 1
            elif char == "/" and i + 1 < length and sql[i + 1] == "*":
                # Block comment: consume until */
                current.append(sql[i])
                current.append(sql[i + 1])
                i += 2
                while i < length:
                    if sql[i] == "*" and i + 1 < length and sql[i + 1] == "/":
                        current.append(sql[i])
                        current.append(sql[i + 1])
                        i += 2
                        break
                    current.append(sql[i])
                    i += 1
            elif char == ";":
                query = "".join(current).strip()
                if query:
                    queries.append(query)
                current = []
                i += 1
            else:
                current.append(char)
                i += 1

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
