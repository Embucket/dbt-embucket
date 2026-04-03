from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.embucket.connections import EmbucketConnectionManager


class EmbucketAdapter(SnowflakeAdapter):
    ConnectionManager = EmbucketConnectionManager

    @classmethod
    def date_function(cls):
        return "CURRENT_TIMESTAMP()"

    def submit_python_job(self, parsed_model, compiled_code):
        raise NotImplementedError("Python models are not supported on Embucket")
