"""STS Lakeflow Community Connector package."""

from databricks.labs.community_connector.sources.sts.sts import StsLakeflowConnect
from databricks.labs.community_connector.sources.sts.sts_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

__all__ = [
    "StsLakeflowConnect",
    "SUPPORTED_TABLES",
    "TABLE_SCHEMAS",
    "TABLE_METADATA",
]

