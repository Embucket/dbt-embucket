from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.snowflake import constants
from dbt.adapters.embucket.connections import EmbucketConnectionManager


class EmbucketAdapter(SnowflakeAdapter):
    ConnectionManager = EmbucketConnectionManager

    @classmethod
    def date_function(cls):
        return "CURRENT_TIMESTAMP()"

    def submit_python_job(self, parsed_model, compiled_code):
        raise NotImplementedError("Python models are not supported on Embucket")

    def _parse_list_relations_result(self, result):
        """Override to always report DEFAULT table format.

        Embucket uses Iceberg internally but should be treated as a standard
        Snowflake-like warehouse from dbt's perspective. Without this override,
        the Snowflake incremental materialization detects an ICEBERG→DEFAULT
        format mismatch and refuses to update tables.
        """
        relation = super()._parse_list_relations_result(result)
        if relation.table_format != constants.INFO_SCHEMA_TABLE_FORMAT:
            relation = relation.replace(table_format=constants.INFO_SCHEMA_TABLE_FORMAT)
        return relation
