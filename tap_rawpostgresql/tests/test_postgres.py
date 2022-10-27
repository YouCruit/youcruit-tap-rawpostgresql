"""Tests some actual SQL"""

import gzip
import json
import os
from urllib.parse import urlparse

from sqlalchemy import create_engine

from tap_rawpostgresql.tap import TapRawPostgreSQL

CONFIG_BASE = {
    "username": "username",
    "password": "password",
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "schema": "public",
}

CONFIG_INCREMENTAL = dict(
    streams=[
        {
            "name": "test",
            "sql": "SELECT 'Ichi' as one, 2 as two, '3' as three, '4' as four",
            "key_properties": ["one"],
            "replication_key": "two",
            "replication_key_value_start": 0,
            "columns": [
                {
                    "name": "one",
                    "type": "text",
                },
                {
                    "name": "two",
                    "type": "int",
                },
                {
                    "name": "three",
                    "type": "text",
                    "nullable": True,
                },
                {
                    "name": "four",
                    "type": "text",
                    "nullable": False,
                },
            ],
        },
    ],
    **CONFIG_BASE,
)


def test_incremental():
    CONFIG = CONFIG_INCREMENTAL

    tap = TapRawPostgreSQL(CONFIG)

    d = tap.catalog.to_dict()

    for stream in d["streams"]:
        assert stream["tap_stream_id"].split("-")[-1] == "test"
        assert stream["schema"]["properties"]["one"]["type"] == ["string"]
        assert stream["schema"]["properties"]["two"]["type"] == ["integer"]
        assert stream["schema"]["properties"]["three"]["type"] == ["string", "null"]

    for name, stream in tap.streams.items():
        assert name == f"{CONFIG['database']}-{CONFIG['schema']}-test"

        records = list(stream.get_records(context=None))

        assert len(records) == 1

        assert records[0] == {
            "one": "Ichi",
            "two": 2,
            "three": "3",
            "four": "4",
        }


CONFIG_FULL = dict(
    streams=[
        {
            "name": "test",
            "sql": "SELECT 'Ichi' as one, 2 as two, '3' as three, '4' as four",
            "key_properties": ["one"],
            "columns": [
                {
                    "name": "one",
                    "type": "text",
                },
                {
                    "name": "two",
                    "type": "int",
                },
                {
                    "name": "three",
                    "type": "text",
                    "nullable": True,
                },
                {
                    "name": "four",
                    "type": "text",
                    "nullable": False,
                },
            ],
        },
    ],
    **CONFIG_BASE,
)


def test_full():
    CONFIG = CONFIG_FULL

    tap = TapRawPostgreSQL(CONFIG)

    d = tap.catalog.to_dict()

    for stream in d["streams"]:
        assert stream["tap_stream_id"].split("-")[-1] == "test"
        assert stream["schema"]["properties"]["one"]["type"] == ["string"]
        assert stream["schema"]["properties"]["two"]["type"] == ["integer", "null"]
        assert stream["schema"]["properties"]["three"]["type"] == ["string", "null"]
        assert stream["schema"]["properties"]["four"]["type"] == ["string"]

    for name, stream in tap.streams.items():
        assert name == f"{CONFIG['database']}-{CONFIG['schema']}-test"

        records = list(stream.get_records(context=None))

        assert len(records) == 1

        assert records[0] == {
            "one": "Ichi",
            "two": 2,
            "three": "3",
            "four": "4",
        }


CONFIG_BATCH = dict(
    batch_size=5,
    batch_config={
        "encoding": {"format": "jsonl", "compression": "gzip"},
        "storage": {"root": "file:///tmp"},
    },
    streams=[
        {
            "name": "test",
            "sql": """
                select
                  '1441c21d-9921-4a1d-b239-9c6ea18af234'::uuid as id,
                  timestamptz '2021-11-22T11:45:11.062824+00:00' as last_updated
            """,
            "key_properties": ["id"],
            "columns": [
                {
                    "name": "id",
                    "type": "string",
                },
                {
                    "name": "last_updated",
                    "type": "datetime",
                },
            ],
        }
    ],
    **CONFIG_BASE,
)


def test_batch():
    CONFIG = CONFIG_BATCH
    tap = TapRawPostgreSQL(CONFIG)

    for _, stream in tap.streams.items():
        batches = list(
            stream.get_batches(batch_config=stream.get_batch_config(stream.config))
        )

        assert len(batches) == 1

        _, files = batches[0]

        assert len(files) == 1

        p = urlparse(files[0])
        batchfile = os.path.abspath(os.path.join(p.netloc, p.path))

        result = None

        with open(batchfile, "rb") as f:
            with gzip.GzipFile(fileobj=f, mode="rb") as gz:
                for line in gz:
                    result = json.loads(line)

        assert result == {
            "id": "1441c21d-9921-4a1d-b239-9c6ea18af234",
            "last_updated": "2021-11-22T11:45:11.062824+00:00",
        }
