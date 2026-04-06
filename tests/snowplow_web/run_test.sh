#!/bin/bash
set -e

# Snowplow-web integration test using dbt-embucket adapter
# Uses the same test data and project structure as embucket-labs but routes through Lambda invoke

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK_DIR="$SCRIPT_DIR/work"

FUNCTION_ARN="${EMBUCKET_FUNCTION_ARN:?Set EMBUCKET_FUNCTION_ARN env var}"
DATABASE="demo"

echo ""
echo "###################################"
echo "dbt-embucket snowplow-web test"
echo "###################################"
echo ""
echo "Function ARN: $FUNCTION_ARN"
echo "Database: $DATABASE"
echo ""

# Step 1: Load data via the dbt-embucket adapter
echo "###################################"
echo "Step 1: Loading events data via Lambda invoke"
echo "###################################"
echo ""

python3 "$SCRIPT_DIR/load_data.py" \
    --function-arn "$FUNCTION_ARN" \
    --database "$DATABASE" \
    --sql-file "$SCRIPT_DIR/create_and_load_events.sql"

echo ""

# Step 2: Clone dbt-snowplow-web and set up project
echo "###################################"
echo "Step 2: Setting up dbt-snowplow-web project"
echo "###################################"
echo ""

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

git clone --depth 1 https://github.com/snowplow/dbt-snowplow-web.git "$WORK_DIR/dbt-snowplow-web" 2>&1 | tail -1

# Copy our custom project and profile configs
cp "$SCRIPT_DIR/dbt_project.yml" "$WORK_DIR/dbt-snowplow-web/"
cp "$SCRIPT_DIR/profiles.yml" "$WORK_DIR/dbt-snowplow-web/"

echo ""

# Step 3: Install dbt deps and run
echo "###################################"
echo "Step 3: Running dbt pipeline"
echo "###################################"
echo ""

cd "$WORK_DIR/dbt-snowplow-web"

echo "--- dbt debug ---"
dbt debug --target embucket
echo ""

echo "--- dbt deps ---"
dbt deps
echo ""

echo "--- dbt seed ---"
dbt seed --full-refresh --target embucket
echo ""

echo "--- dbt run ---"
dbt run --target embucket 2>&1 | tee "$WORK_DIR/dbt_run_output.log"
echo ""

echo "--- dbt test ---"
dbt test --target embucket 2>&1 | tee "$WORK_DIR/dbt_test_output.log" || true
echo ""

# Step 4: Verify results
echo "###################################"
echo "Step 4: Verifying results"
echo "###################################"
echo ""

python3 "$SCRIPT_DIR/verify_results.py" \
    --function-arn "$FUNCTION_ARN" \
    --database "$DATABASE"

echo ""
echo "###################################"
echo "Test complete!"
echo "###################################"
