import mock
import tap_db2.discovery as d


def _update(dict_, **kwargs):
    new = dict_.copy()
    new.update(kwargs)
    return new


@mock.patch("tap_db2.discovery._query_tables")
@mock.patch("tap_db2.discovery._query_columns")
@mock.patch("tap_db2.discovery._query_primary_keys")
def test_basic_discovery(pks_mock, columns_mock, tables_mock):
    tables_mock.return_value = [
        ("a_schema", "a_table", "T"),
    ]
    columns_mock.return_value = [
        ("a_schema", "a_table", "a_column", "FLOAT", None, None, None),
        ("a_schema", "a_table", "b_column", "FLOAT", None, None, None),
        ("a_schema", "a_table", "c_column", "FLOAT", None, None, None),
    ]
    pks_mock.return_value = [
        ("a_schema", "a_table", "a_column", 2),
        ("a_schema", "a_table", "b_column", 1),
    ]
    ctx = mock.MagicMock()
    catalog = d.discover(ctx).to_dict()
    streams = catalog["streams"]
    float_schema = {"inclusion": "available", "type": ["null", "number"]}
    expected_schema = {
        "properties": {"a_column": _update(float_schema, inclusion="automatic"),
                       "b_column": _update(float_schema, inclusion="automatic"),
                       "c_column": float_schema},
        "type": "object",
    }
    streams[0]["metadata"].sort(key=lambda x: x["breadcrumb"])
    import pprint
    import sys
    pprint.pprint(streams[0])
    assert streams[0] == {
        "database_name": "a_schema",
        "table_name": "a_table",
        "tap_stream_id": "a_schema-a_table",
        "schema": expected_schema,
        "stream": "a_table",
        "is_view": False,
        "key_properties": ["b_column", "a_column"],
        "metadata": [{"breadcrumb": (),
                      "metadata": {"selected-by-default": False}},
                     {"breadcrumb": ("properties", "a_column"),
                      "metadata": {"selected-by-default": True,
                                   "sql-datatype": "float"}},
                     {"breadcrumb": ("properties", "b_column"),
                      "metadata": {"selected-by-default": True,
                                   "sql-datatype": "float"}},
                     {"breadcrumb": ("properties", "c_column"),
                      "metadata": {"selected-by-default": True,
                                   "sql-datatype": "float"}}],
    }



@mock.patch("tap_db2.discovery._query_tables")
@mock.patch("tap_db2.discovery._query_columns")
@mock.patch("tap_db2.discovery._query_primary_keys")
def test_decimal_types(pks_mock, columns_mock, tables_mock):
    tables_mock.return_value = [
        ("a_schema", "a_table", "T"),
    ]
    columns_mock.return_value = [
        ("a_schema", "a_table", "a_column", "decimal", None, 10, 4),
    ]
    ctx = mock.MagicMock()
    catalog = d.discover(ctx).to_dict()
    schema = catalog["streams"][0]["schema"]["properties"]["a_column"]
    # precision = number of digits
    # scale = how many of those are decimal
    # so number of digits for non-decimal is 10 - 4 = 6
    # largest 6-digit number is 999,999
    # so exclusive max is that + 1 = 1,000,000 = 10^6
    expected_limit = 10 ** 6
    assert schema == {"inclusion": "available",
                      "type": ["null", "number"],
                      "exclusiveMinimum": True,
                      "exclusiveMaximum": True,
                      "minimum": -expected_limit,
                      "maximum": expected_limit,
                      "multipleOf": 0.0001}
