from __future__ import annotations

from databricks.labs.community_connector.sources.sts import StsLakeflowConnect
from databricks.labs.community_connector.sources.sts.sts import TABLE_CONFIG


_ORIGINAL_FETCH_TABLE_XML = StsLakeflowConnect._fetch_table_xml
_XML_CACHE_BY_URL: dict[str, str] = {}


def _cached_fetch_table_xml(
    self: StsLakeflowConnect, table_name: str, table_options: dict[str, str]
) -> tuple[str, str]:
    service_id = TABLE_CONFIG[table_name]["service_id"]
    source_url = self._build_source_url(table_name, service_id, table_options)

    cached_xml = _XML_CACHE_BY_URL.get(source_url)
    if cached_xml is not None:
        return cached_xml, source_url

    xml_text, source_url = _ORIGINAL_FETCH_TABLE_XML(self, table_name, table_options)
    _XML_CACHE_BY_URL[source_url] = xml_text
    return xml_text, source_url


StsLakeflowConnect._fetch_table_xml = _cached_fetch_table_xml
