"""SQL client handling.

This includes PostgreSQLStream and PostgreSQLConnector.
"""

import gzip
import json
import logging
from os import PathLike
from typing import IO, Any, Iterable, Optional, Union
from uuid import UUID, uuid4

import singer_sdk._singerlib as singer
import sqlalchemy  # type: ignore
from singer_sdk import SQLConnector, Stream
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig
from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.plugin_base import PluginBase as TapBaseClass


class RawPostgreSQLConnector(SQLConnector):
    """Connects to the PostgreSQL SQL source."""

    def __init__(
        self,
        config: Optional[dict] = None,
        sqlalchemy_url: Optional[str] = None,
        connection: Optional[sqlalchemy.engine.Connection] = None,
    ) -> None:
        super().__init__(config, sqlalchemy_url)
        self._own_connection = connection

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return (
            f"postgresql+psycopg2://{config['username']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )

    def create_sqlalchemy_engine(self) -> sqlalchemy.engine.Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return sqlalchemy.create_engine(self.sqlalchemy_url, echo=False)

    def table_exists(self, full_table_name: str) -> bool:
        return True

    def column_exists(self, full_table_name: str, column_name: str) -> bool:
        return True

    def get_table_columns(
        self, full_table_name: str, column_names: Optional[list[str]] = None
    ) -> dict[str, sqlalchemy.Column]:
        _, _, table_name = self.parse_full_table_name(full_table_name)
        table_config: Optional[dict] = None

        for c in self.config["streams"]:
            if c["name"] == table_name:
                table_config = c
                break

        if not table_config:
            raise Exception(f"No stream with name '{table_name} defined in config")

        return {
            col_meta["name"]: sqlalchemy.Column(
                col_meta["name"],
                col_meta["type"],
                nullable=col_meta.get("nullable", True),
            )
            for col_meta in table_config["columns"]
        }

    def get_table(
        self, full_table_name: str, column_names: Optional[list[str]] = None
    ) -> sqlalchemy.Table:
        return super().get_table(full_table_name, column_names)

    def discover_catalog_entries(self) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        result: list[dict] = []
        engine = self.create_sqlalchemy_engine()
        inspected = sqlalchemy.inspect(engine)
        for schema_name in self.get_schema_names(engine, inspected):
            # Iterate through each table and view
            for table_name, is_view in self.get_object_names(
                engine, inspected, schema_name
            ):
                catalog_entry = self.discover_catalog_entry(
                    engine, inspected, schema_name, table_name, is_view
                )
                result.append(catalog_entry.to_dict())

        return result

    @property
    def connection(self) -> sqlalchemy.engine.Connection:
        """Return or set the SQLAlchemy connection object.

        Returns:
            The active SQLAlchemy connection object.
        """
        if self._own_connection is not None:
            return self._own_connection

        if not self._connection:
            self._connection = self.create_sqlalchemy_connection()

        return self._connection

    @staticmethod
    def to_jsonschema_type(sql_type: sqlalchemy.types.TypeEngine) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_jsonschema_type(sql_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_sql_type(jsonschema_type)


def conform_record_data_types_and_uuid(  # noqa: C901
    stream_name: str, record: dict[str, Any], schema: dict, logger: logging.Logger
) -> dict[str, Any]:
    """Translate values in record dictionary to singer-compatible data types.

    Any property names not found in the schema catalog will be removed, and a
    warning will be logged exactly once per unmapped property name.
    """
    rec = conform_record_data_types(
        stream_name=stream_name, record=record, schema=schema, logger=logger
    )

    # Fix any UUIDs as well
    for property_name, elem in record.items():
        if isinstance(elem, UUID):
            rec[property_name] = str(elem)

    return rec


class RawPostgreSQLStream(Stream):
    """Stream class for PostgreSQL streams."""

    connector_class = RawPostgreSQLConnector
    stream_config: dict

    def __init__(
        self,
        tap: TapBaseClass,
        stream_config: dict,
        schema: Union[str, PathLike, dict[str, Any], singer.Schema, None] = None,
        name: Optional[str] = None,
        connector: Optional[SQLConnector] = None,
    ) -> None:
        self.stream_config = stream_config
        self.connector = connector or self.connector_class(dict(tap.config))
        super().__init__(tap, schema=schema, name=name)
        self.primary_keys = stream_config["key_properties"]
        self.replication_key = stream_config.get("replication_key", None)
        self.batch_size = self.config.get("batch_size", 100_000)

    def get_batches(
        self,
        batch_config: BatchConfig,
        context: Optional[dict] = None,
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Batch generator function.

        Developers are encouraged to override this method to customize batching
        behavior for databases, bulk APIs, etc.

        Args:
            batch_config: Batch config for this stream.
            context: Stream partition or context dictionary.

        Yields:
            A tuple of (encoding, manifest) for each batch.
        """
        sync_id = f"{self.tap_name}--{self.name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""

        i = 1
        chunk_size = 0
        filename: Optional[str] = None
        f: Optional[IO] = None
        gz: Optional[gzip.GzipFile] = None

        with batch_config.storage.fs() as fs:
            for record in self._sync_records(context, write_messages=False):
                if filename is None:
                    filename = f"{prefix}{sync_id}-{i}.json.gz"
                    f = fs.open(filename, "wb")
                    gz = gzip.GzipFile(fileobj=f, mode="wb")

                record = conform_record_data_types_and_uuid(
                    stream_name=self.name,
                    record=record,
                    schema=self.schema,
                    logger=self.logger,
                )

                gz.write((json.dumps(record) + "\n").encode())
                chunk_size += 1

                if chunk_size >= self.batch_size:
                    gz.close()
                    gz = None
                    f.close()
                    f = None
                    file_url = fs.geturl(filename)
                    yield batch_config.encoding, [file_url]

                    filename = None

                    i += 1
                    chunk_size = 0

            if chunk_size > 0:
                gz.close()
                f.close()
                file_url = fs.geturl(filename)
                yield batch_config.encoding, [file_url]

    def get_records(self, context: Optional[dict]) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Yield one dict per record.
        """
        sql: str = self.stream_config["sql"]

        if not sql:
            raise Exception("sql is empty: ")

        is_incremental = self.replication_method == "INCREMENTAL"

        rep_key = self.stream_config.get("replication_key", context)

        multiparams: list[dict] = []

        if is_incremental:
            if not rep_key:
                raise Exception(
                    "INCREMENTAL sync not possible with a specified 'replication_key'"
                )

            rep_key_val = self.get_starting_replication_key_value(context)
            if rep_key_val is None:
                rep_key_val = self.stream_config.get(
                    "replication_key_value_start", None
                )

            if rep_key_val is None:
                raise Exception(
                    "No value for replication key. INCREMENTAL sync not possible."
                )

            multiparams = [{"rep_key_val": rep_key_val}]
        else:
            rep_key_val = self.stream_config.get("replication_key_value_start", None)

            if rep_key:
                rep_key_val = self.stream_config.get(
                    "replication_key_value_start", None
                )
                if rep_key_val is None:
                    raise Exception(
                        "'replication_key' is specified but no "
                        "'replication_key_value_start'."
                        " FULL_TABLE sync not possible."
                    )

                multiparams = [{"rep_key_val": rep_key_val}]

        for record in self.connector.connection.execute(
            sqlalchemy.text(sql),
            *multiparams,
        ).mappings():
            yield dict(record)
