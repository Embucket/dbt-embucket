from dbt.adapters.embucket.connections import EmbucketConnectionManager
from dbt.adapters.embucket.connections import EmbucketCredentials
from dbt.adapters.embucket.impl import EmbucketAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import embucket

Plugin = AdapterPlugin(
    adapter=EmbucketAdapter,
    credentials=EmbucketCredentials,
    include_path=embucket.PACKAGE_PATH,
    dependencies=["snowflake"],
)
