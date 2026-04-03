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

    key_macros = [
        "snowflake__list_schemas",
        "snowflake__get_columns_in_relation",
    ]
    for macro_name in key_macros:
        assert macro_name in content, (
            f"Snowflake macro '{macro_name}' not found in adapters.sql - "
            "dispatch would fall through to dbt default"
        )
