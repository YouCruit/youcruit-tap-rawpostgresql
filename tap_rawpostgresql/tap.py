"""PostgreSQL tap class."""

from pathlib import PurePath
from typing import Any, Dict, List, Optional, Tuple, Union

import sqlalchemy  # type: ignore
from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.capabilities import (
    CapabilitiesEnum,
    PluginCapabilities,
    TapCapabilities,
)
from singer_sdk.streams.sql import SQLConnector

from tap_rawpostgresql.client import RawPostgreSQLConnector, RawPostgreSQLStream


class TapRawPostgreSQL(Tap):
    """PostgreSQL tap class."""

    name = "tap-rawpostgresql"

    _own_connection: Optional[sqlalchemy.engine.Connection] = None

    _catalog_dict: Optional[dict] = None

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
        ),
        th.Property(
            "host",
            th.StringType,
            required=True,
        ),
        th.Property(
            "port",
            th.IntegerType,
            required=True,
        ),
        th.Property(
            "database",
            th.StringType,
            required=True,
        ),
        th.Property(
            "schema",
            th.StringType,
            required=True,
        ),
        th.Property(
            "streams",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType,
                        required=True,
                    ),
                    th.Property(
                        "key_properties",
                        th.ArrayType(th.StringType),
                        required=True,
                    ),
                    th.Property(
                        "sql",
                        th.StringType,
                        required=True,
                    ),
                    th.Property(
                        "columns",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property(
                                    "name",
                                    th.StringType,
                                    required=True,
                                ),
                                th.Property(
                                    "type",
                                    th.StringType,
                                    required=True,
                                ),
                                th.Property(
                                    "nullable",
                                    th.BooleanType,
                                    required=False,
                                    default=True,
                                ),
                            ),
                        ),
                        required=True,
                    ),
                ),
            ),
            required=True,
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            required=False,
            default=100_000,
            description="Size of batch files",
        ),
        th.Property(
            "batch_config",
            th.ObjectType(
                th.Property(
                    "encoding",
                    th.ObjectType(
                        th.Property("format", th.StringType, required=True),
                        th.Property("compression", th.StringType, required=True),
                    ),
                    required=True,
                ),
                th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property("root", th.StringType, required=True),
                        th.Property(
                            "prefix", th.StringType, required=False, default=""
                        ),
                    ),
                    required=True,
                ),
            ),
            required=False,
        ),
    ).to_dict()

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        catalog: Union[PurePath, str, dict, None] = None,
        state: Union[PurePath, str, dict, None] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        connection: Optional[sqlalchemy.engine.Connection] = None,
    ) -> None:
        super().__init__(config, catalog, state, parse_env_config, validate_config)
        self._own_connection = connection

    def parse_raw_sql_stream(self, stream_config: dict) -> Dict[str, Any]:
        unique_stream_id = SQLConnector.get_fully_qualified_name(
            db_name=self.config["database"],
            schema_name=self.config["schema"],
            table_name=stream_config["name"],
            delimiter="-",
        )

        key_properties = stream_config["key_properties"]

        table_schema = th.PropertiesList()
        for column_def in stream_config["columns"]:
            column_name = column_def["name"]
            is_nullable = column_def.get("nullable", True)
            jsonschema_type: dict = SQLConnector.to_jsonschema_type(column_def["type"])
            table_schema.append(
                th.Property(
                    name=column_name,
                    wrapped=th.CustomType(jsonschema_type),
                    required=not is_nullable or column_name in key_properties,
                )
            )
        schema = table_schema.to_dict()

        # TODO
        replication_method = "FULL_TABLE"

        # Create the catalog entry object
        catalog_entry = CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=stream_config["name"],
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            is_view=False,
            replication_method=replication_method,
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=stream_config["name"],
                schema=schema,
                replication_method=replication_method,
                key_properties=key_properties,
                valid_replication_keys=None,  # Must be defined by user
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

        return catalog_entry.to_dict()

    def discover_raw_sql_streams(self) -> List[Tuple[Dict[str, Any], dict]]:
        return [
            (self.parse_raw_sql_stream(stream_config), stream_config)
            for stream_config in self.config["streams"]
        ]

    @property
    def streams(self) -> Dict[str, Stream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        result: Dict[str, Stream] = {}

        connector = (
            RawPostgreSQLConnector(connection=self._own_connection)
            if self._own_connection
            else None
        )

        for catalog_entry, stream_config in self.discover_raw_sql_streams():
            s = RawPostgreSQLStream(
                tap=self,
                schema=catalog_entry["schema"],
                name=catalog_entry["tap_stream_id"],
                stream_config=stream_config,
                connector=connector,
            )
            result[s.name] = s

        return result

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict:
            return self._catalog_dict

        result: Dict[str, List[dict]] = {"streams": []}
        result["streams"].extend(
            [catalog for catalog, _ in self.discover_raw_sql_streams()]
        )

        self._catalog_dict = result
        return self._catalog_dict

    @classproperty
    def capabilities(self) -> List[CapabilitiesEnum]:
        """Get tap capabilities.

        Returns:
            A list of capabilities supported by this tap.
        """
        return [
            TapCapabilities.CATALOG,
            TapCapabilities.STATE,
            TapCapabilities.DISCOVER,
            PluginCapabilities.ABOUT,
            PluginCapabilities.STREAM_MAPS,
            PluginCapabilities.FLATTENING,
            PluginCapabilities.BATCH,
        ]


if __name__ == "__main__":
    TapRawPostgreSQL.cli()
