from collections import namedtuple
from itertools import groupby
from singer.catalog import Catalog, CatalogEntry
from ..common import get_cursor
from . import schemas

Table = namedtuple("Table", [
    "table_schema",
    "table_name",
    "table_type",
])

Column = namedtuple("Column", [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
])


# Note the _query_* functions mainly exist for the sake of mocking in unit
# tests. Normally I would prefer to have integration tests than mock out this
# data, but DB2 databases aren't easy to come by and if there is a lot of data
# these queries can be quite slow.
def _query_tables(cursor):
    """Queries the qsys2 tables catalog and returns an iterator containing the
    raw results."""
    cursor.execute("""
        SELECT table_schema,
               table_name,
               table_type
          FROM qsys2.systables
         WHERE table_type in ('T', 'V')
    """)
    return cursor


def _query_columns(cursor):
    """Queries the qsys2 columns catalog and returns an iterator containing the
    raw results."""
    cursor.execute("""
        SELECT table_schema,
               table_name,
               column_name,
               data_type,
               character_maximum_length,
               numeric_precision,
               numeric_scale
          FROM qsys2.syscolumns
    """)
    return cursor


def _query_primary_keys(cursor):
    """Queries the qsys2 primary key catalog and returns an iterator containing
    the raw results."""
    cursor.execute("""
        SELECT A.table_schema,
               A.table_name,
               A.column_name,
               A.ordinal_position
          FROM qsys2.syskeycst A
          JOIN qsys2.syscst B
            ON A.constraint_schema = B.constraint_schema
           AND A.constraint_name = B.constraint_name
         WHERE B.constraint_type = 'PRIMARY KEY'
    """)
    return cursor


def _table_id(table):
    """Returns a 2-tuple that can be used to uniquely identify the table."""
    return (table.table_schema, table.table_name)


def _find_tables(config):
    with get_cursor(config) as cursor:
        # We use the qsys2.systables catalog rather than
        # information_schema.tables because it contains better information
        # about the "table_type." The information_schema table doesn't
        # distinguish between tables and data files.
        results = _query_tables(cursor)
        tbls = [Table(*rec) for rec in results]
        return {_table_id(t): t for t in tbls}


def _find_columns(config, tables):
    with get_cursor(config) as cursor:
        results = _query_columns(cursor)
        cols = (Column(*rec) for rec in results)
        return [c for c in cols
                if (c.table_schema, c.table_name) in tables]


def _find_primary_keys(config, tables):
    """Returns a dict of tuples -> list where the keys are \"table ids\" -
    ie. the (schema name, table_name) and the values are the primary key
    columns, sorted by their ordinal position."""
    with get_cursor(config) as cursor:
        results = _query_primary_keys(cursor)
        keys = {}
        for (table_schema, table_name, column_name, ordinal_pos) in results:
            table_id = (table_schema, table_name)
            if table_id not in tables:
                continue
            if table_id not in keys:
                keys[table_id] = []
            # We append a 2-tuple containing the ordinal position first so we
            # can sort the PKs by their ordinal position once this loop is
            # done.
            keys[table_id].append((ordinal_pos, column_name))
        return {
            k: [x[1] for x in sorted(v)]
            for k, v in keys.items()
        }


def discover(config):
    tables = _find_tables(config)
    columns = _find_columns(config, tables)
    pks = _find_primary_keys(config, tables)
    entries = []
    for (table_id, cols) in groupby(columns, _table_id):
        (table_schema, table_name) = table_id
        cols = list(cols)
        schema = schemas.generate(cols)
        entry = CatalogEntry(
            database=table_schema,
            table=table_name,
            stream=table_name,
            tap_stream_id="{}-{}".format(table_schema, table_name),
            schema=schema)
        if table_id in pks:
            entry.key_properties = pks[table_id]
        # if table_schema in table_info and table_name in table_info[table_schema]:
        #     entry.row_count = table_info[table_schema][table_name]["row_count"]
        entry.is_view = tables[table_id].table_type == "V"
        entries.append(entry)
    return Catalog(entries)
