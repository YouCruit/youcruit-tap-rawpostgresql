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
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)

    with engine.connect() as conn:
        # conn.execute(text("CREATE TABLE test_table (x int, y int)"))
        # conn.execute(
        #    text("INSERT INTO test_table (x, y) VALUES (:x, :y)"),
        #    [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
        # )
        # conn.commit()

        tap = TapRawPostgreSQL(CONFIG, connection=conn)

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
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)

    with engine.connect() as conn:
        # conn.execute(text("CREATE TABLE test_table (x int, y int)"))
        # conn.execute(
        #    text("INSERT INTO test_table (x, y) VALUES (:x, :y)"),
        #    [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
        # )
        # conn.commit()

        tap = TapRawPostgreSQL(CONFIG, connection=conn)

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
