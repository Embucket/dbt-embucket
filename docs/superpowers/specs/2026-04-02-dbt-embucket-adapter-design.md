# dbt-embucket Adapter Design

## Overview

A dbt adapter for [Embucket](https://github.com/Embucket/embucket) - a Snowflake-compatible query engine that runs on AWS Lambda over S3 Tables/Iceberg data. The adapter inherits from dbt-snowflake and replaces the transport layer to use AWS Lambda `Invoke` API instead of the Snowflake REST connector.

## Motivation

Embucket exposes a Snowflake V1 REST API on Lambda via function URLs. While you _can_ point `snowflake-connector-python` at the function URL, using `boto3 lambda.invoke()` directly is preferable:

- IAM authentication (no Snowflake credentials management)
- Works without a function URL (VPC-private deployments)
- Native AWS service integration

## Package Structure

```
dbt-embucket/
├── pyproject.toml
├── src/dbt/
│   ├── adapters/embucket/
│   │   ├── __init__.py          # AdapterPlugin(EmbucketAdapter, EmbucketCredentials, ...)
│   │   ├── connections.py       # EmbucketCredentials, EmbucketConnectionManager, LambdaCursor
│   │   └── impl.py              # EmbucketAdapter(SnowflakeAdapter)
│   └── include/embucket/
│       ├── dbt_project.yml      # dispatch config: embucket → snowflake → dbt
│       ├── macros/              # empty initially; overrides go here as needed
│       │   └── .gitkeep
│       └── profiles.yml         # profile template for `dbt init`
└── tests/
    └── ...
```

## Dependencies

- `dbt-snowflake` - parent adapter (provides macros, base classes)
- `boto3` - AWS Lambda invoke
- `dbt-core`, `dbt-adapters` - dbt framework (transitive via dbt-snowflake)

## Key Classes

### EmbucketCredentials (extends SnowflakeCredentials)

New fields:
- `function_arn: str` - Lambda function ARN
- `region: str` - AWS region (default: derived from ARN)

Inherited Snowflake fields used for the Embucket login request:
- `account`, `user`, `password`, `database`, `schema`

Connection keys shown in `dbt debug`: `function_arn`, `region`, `account`, `user`, `database`, `schema`.

### EmbucketConnectionManager (extends SnowflakeConnectionManager)

Overrides:
- `open(connection)` - Creates a boto3 Lambda client and performs the Embucket login flow via Lambda invoke. Returns a connection with a `LambdaConnectionHandle`.
- `cancel(connection)` - No-op or sends abort request via invoke.
- `execute(sql, ...)` - Uses Lambda invoke to send queries.

Transaction methods remain stubbed (inherited from Snowflake - Embucket doesn't use transactions either).

### LambdaConnectionHandle

A wrapper that holds:
- `boto3_client` - Lambda client
- `function_arn` - target function
- `session_token` - JWT from login response
- `session_id` - session identifier

### LambdaCursor

A cursor-like object that:
1. Accepts SQL via `execute(sql)`
2. Builds a Snowflake V1 query request payload
3. Invokes the Lambda with an HTTP-shaped event:
   ```json
   {
     "requestContext": {"http": {"method": "POST", "path": "/queries/v1/query-request"}},
     "headers": {"authorization": "Bearer <token>", "content-type": "application/json"},
     "body": "{\"sqlText\": \"SELECT 1\", ...}"
   }
   ```
4. Parses the Snowflake V1 JSON response into `description` (column metadata) and `fetchall()` results

### EmbucketAdapter (extends SnowflakeAdapter)

Minimal overrides:
- `ConnectionManager = EmbucketConnectionManager`
- `Relation` / `Column` - inherited from Snowflake as-is

## Connection Flow

```
dbt run
  → EmbucketConnectionManager.open()
    → boto3.client('lambda', region_name=...)
    → Lambda invoke: POST /session/v1/login-request
      payload: {"data": {"ACCOUNT_NAME": "embucket", "LOGIN_NAME": "...", "PASSWORD": "..."}}
    → Parse response: extract token
    → Return Connection(handle=LambdaConnectionHandle(...))

  → adapter.execute("CREATE TABLE ...")
    → LambdaCursor.execute(sql)
      → Lambda invoke: POST /queries/v1/query-request
        payload: {"sqlText": "CREATE TABLE ...", "sequenceId": N}
        headers: Authorization: Bearer <token>
      → Parse response: extract rowtype (columns), rowset (data)
    → Return AdapterResponse
```

## Lambda Invoke Payload Format

The Embucket Lambda handler expects Lambda Function URL event format:

```json
{
  "version": "2.0",
  "requestContext": {
    "http": {
      "method": "POST",
      "path": "/queries/v1/query-request"
    }
  },
  "headers": {
    "content-type": "application/json",
    "authorization": "Bearer <jwt-token>"
  },
  "body": "<json-encoded request body>",
  "isBase64Encoded": false
}
```

Response is a Lambda Function URL response:
```json
{
  "statusCode": 200,
  "headers": {"content-type": "application/json"},
  "body": "<json-encoded Snowflake V1 response>"
}
```

## Macro Resolution

The adapter's `dbt_project.yml` configures dispatch:

```yaml
name: embucket
version: 0.1.0

dispatch:
  - macro_namespace: dbt
    search_order: ['embucket', 'snowflake', 'dbt']
```

Resolution order: `embucket__X` → `snowflake__X` → `default__X`.

This ensures Snowflake-specific macros (materializations, DDL, metadata queries) are used rather than dbt defaults. This is a critical correctness requirement and must be tested.

## profiles.yml Template

```yaml
my_embucket:
  target: dev
  outputs:
    dev:
      type: embucket
      function_arn: arn:aws:lambda:us-east-2:123456789:function:embucket-lambada
      account: embucket
      user: embucket
      password: embucket
      database: demo
      schema: public
```

AWS credentials come from the standard boto3 chain (env vars, `~/.aws/credentials`, IAM role).

## Testing Strategy

1. **Macro dispatch test** - Verify that Snowflake macros are used (not dbt defaults) for key operations: `create_table_as`, `list_schemas`, `get_columns_in_relation`.
2. **Connection test** - `dbt debug` against a live Lambda (embucket-lambda-sturukin_10g).
3. **Basic operations** - `dbt run` with a simple model, `dbt seed`, `dbt test`.

## Limitations / Future Work

- 6MB synchronous invoke response limit (sufficient for dbt's typical DDL/metadata patterns)
- No streaming response support initially
- Dynamic tables and clone not supported by Embucket (macros may need overrides to error gracefully)
- Python models not supported (no Snowpark on Embucket)
