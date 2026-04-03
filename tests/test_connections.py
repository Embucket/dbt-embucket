import json
import pytest
from dbt.adapters.embucket.connections import LambdaCursor


class TestLambdaCursorParsing:
    """Test that LambdaCursor correctly parses Snowflake V1 JSON responses."""

    def _make_cursor(self):
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
    """Test that payload builders create correct Lambda invoke payloads."""

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
        )
        assert payload["version"] == "2.0"
        assert payload["rawPath"] == "/queries/v1/query-request"
        assert "requestId=" in payload["rawQueryString"]
        assert payload["headers"]["authorization"] == 'Snowflake Token="my-jwt-token"'
        body = json.loads(payload["body"])
        assert body["sqlText"] == "SELECT 1"


class TestSplitQueries:
    def test_simple_split(self):
        from dbt.adapters.embucket.connections import EmbucketConnectionManager
        result = EmbucketConnectionManager._split_queries("SELECT 1; SELECT 2")
        assert result == ["SELECT 1", "SELECT 2"]

    def test_semicolon_in_line_comment(self):
        from dbt.adapters.embucket.connections import EmbucketConnectionManager
        sql = "-- this is a comment; not a split\nSELECT 1; SELECT 2"
        result = EmbucketConnectionManager._split_queries(sql)
        assert result == ["-- this is a comment; not a split\nSELECT 1", "SELECT 2"]

    def test_semicolon_in_block_comment(self):
        from dbt.adapters.embucket.connections import EmbucketConnectionManager
        sql = "SELECT /* comment; here */ 1; SELECT 2"
        result = EmbucketConnectionManager._split_queries(sql)
        assert result == ["SELECT /* comment; here */ 1", "SELECT 2"]

    def test_semicolon_in_string(self):
        from dbt.adapters.embucket.connections import EmbucketConnectionManager
        sql = "SELECT 'hello; world'; SELECT 2"
        result = EmbucketConnectionManager._split_queries(sql)
        assert result == ["SELECT 'hello; world'", "SELECT 2"]

    def test_no_trailing_semicolon(self):
        from dbt.adapters.embucket.connections import EmbucketConnectionManager
        result = EmbucketConnectionManager._split_queries("SELECT 1")
        assert result == ["SELECT 1"]
