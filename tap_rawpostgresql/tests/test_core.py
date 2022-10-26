"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_standard_tap_tests

from tap_rawpostgresql.tap import TapRawPostgreSQL

SAMPLE_CONFIG = {
    "username": "username",
    "password": "password",
    "host": "localhost",
    "port": 5432,
    "database": "testdb",
    "schema": "public",
    "streams": [
        {
            "name": "test",
            "sql": "SELECT 1 as one",
            "key_properties": ["one"],
            "columns": [
                {
                    "name": "one",
                    "type": "string",
                }
            ],
        }
    ],
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapRawPostgreSQL, config=SAMPLE_CONFIG)
    for test in tests:
        test()
