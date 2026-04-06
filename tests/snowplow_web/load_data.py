#!/usr/bin/env python3
"""Load snowplow events data into Embucket via Lambda invoke."""
import argparse
import json
import re
import sys

from dbt.adapters.embucket.connections import build_login_payload, LambdaHandle
import boto3
from botocore.config import Config as BotoConfig


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--function-arn", required=True)
    parser.add_argument("--database", default="demo")
    parser.add_argument("--sql-file", required=True)
    args = parser.parse_args()

    arn_parts = args.function_arn.split(":")
    region = arn_parts[3]

    cfg = BotoConfig(region_name=region, read_timeout=960)
    client = boto3.client("lambda", config=cfg)

    # Login
    print(f"Logging in to {args.function_arn}...")
    lp = build_login_payload(
        account=os.environ.get("EMBUCKET_ACCOUNT", "embucket"),
        user=os.environ.get("EMBUCKET_USER", "embucket"),
        password=os.environ.get("EMBUCKET_PASSWORD", "embucket"),
        database=args.database,
    )
    r = client.invoke(FunctionName=args.function_arn, InvocationType="RequestResponse",
                      Payload=json.dumps(lp).encode())
    body = json.loads(json.loads(r["Payload"].read())["body"])
    if not body.get("success"):
        print(f"Login failed: {body.get('message')}")
        sys.exit(1)
    token = body["data"]["token"]
    handle = LambdaHandle(client=client, function_arn=args.function_arn, token=token)
    print("Login OK")

    # Read and execute SQL file
    with open(args.sql_file) as f:
        sql_content = f.read()

    # Split on semicolons (simple - no comments/strings handling needed for this DDL)
    statements = [s.strip() for s in sql_content.split(";") if s.strip()]

    for i, stmt in enumerate(statements, 1):
        # Skip empty/comment-only statements
        cleaned = re.sub(r'--.*$', '', stmt, flags=re.MULTILINE).strip()
        if not cleaned:
            continue
        preview = cleaned[:80].replace("\n", " ")
        print(f"  [{i}/{len(statements)}] {preview}...")
        try:
            cursor = handle.cursor()
            cursor.execute(stmt)
            rows = cursor.fetchall()
            if rows and cursor.description:
                cols = [d[0] for d in cursor.description]
                print(f"    OK: {dict(zip(cols, rows[0])) if len(rows) == 1 else f'{len(rows)} rows'}")
            else:
                print(f"    OK")
        except Exception as e:
            print(f"    WARNING: {e}")

    # Verify
    cursor = handle.cursor()
    cursor.execute(f"SELECT COUNT(*) as cnt FROM {args.database}.public_snowplow_manifest.events")
    count = cursor.fetchall()[0][0]
    print(f"\nEvents loaded: {count} rows")

    if count == 0:
        print("ERROR: No data loaded!")
        sys.exit(1)


if __name__ == "__main__":
    main()
