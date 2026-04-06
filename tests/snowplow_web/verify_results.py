#!/usr/bin/env python3
"""Verify snowplow-web dbt run produced expected tables with data."""
import argparse
import json
import sys

from dbt.adapters.embucket.connections import build_login_payload, LambdaHandle
import boto3
from botocore.config import Config as BotoConfig


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--function-arn", required=True)
    parser.add_argument("--database", default="demo")
    args = parser.parse_args()

    arn_parts = args.function_arn.split(":")
    region = arn_parts[3]

    cfg = BotoConfig(region_name=region, read_timeout=960)
    client = boto3.client("lambda", config=cfg)

    lp = build_login_payload(
        account=os.environ.get("EMBUCKET_ACCOUNT", "embucket"),
        user=os.environ.get("EMBUCKET_USER", "embucket"),
        password=os.environ.get("EMBUCKET_PASSWORD", "embucket"),
        database=args.database,
    )
    r = client.invoke(FunctionName=args.function_arn, InvocationType="RequestResponse",
                      Payload=json.dumps(lp).encode())
    body = json.loads(json.loads(r["Payload"].read())["body"])
    token = body["data"]["token"]
    handle = LambdaHandle(client=client, function_arn=args.function_arn, token=token)

    # Check schemas created
    cursor = handle.cursor()
    cursor.execute(f"SHOW SCHEMAS IN DATABASE {args.database}")
    schemas = [row[1] for row in cursor.fetchall()]
    print(f"Schemas: {schemas}")

    # Check key derived tables
    expected_tables = [
        ("public_snowplow_manifest_derived", "snowplow_web_page_views"),
        ("public_snowplow_manifest_derived", "snowplow_web_sessions"),
        ("public_snowplow_manifest_derived", "snowplow_web_users"),
    ]

    all_ok = True
    for schema, table in expected_tables:
        try:
            c = handle.cursor()
            c.execute(f"SELECT COUNT(*) FROM {args.database}.{schema}.{table}")
            count = c.fetchall()[0][0]
            status = "OK" if count > 0 else "EMPTY"
            if count == 0:
                all_ok = False
            print(f"  {schema}.{table}: {count} rows [{status}]")
        except Exception as e:
            print(f"  {schema}.{table}: ERROR - {e}")
            all_ok = False

    if all_ok:
        print("\nAll key tables have data!")
    else:
        print("\nSome tables are missing or empty!")
        sys.exit(1)


if __name__ == "__main__":
    main()
