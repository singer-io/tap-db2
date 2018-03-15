import csv
from collections import namedtuple
from singer.catalog import Catalog, CatalogEntry
import singer
from singer import metadata
from ..common import get_cursor
from . import schemas
LOGGER = singer.get_logger()

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
    "ccsid",
])

SUPPORTED_TYPES = {"T", "V", "P"}


def _question_marks(lst):
    return ",".join("?" * len(lst))

# Note the _query_* functions mainly exist for the sake of mocking in unit
# tests. Normally I would prefer to have integration tests than mock out this
# data, but DB2 databases aren't easy to come by and if there is a lot of data
# these queries can be quite slow.
def _query_tables(config):
    """Queries the qsys2 tables catalog and returns an iterator containing the
    raw results."""
    sql = """
        SELECT table_schema,
               table_name,
               table_type
          FROM qsys2.systables
         WHERE table_type IN ({})
    """.format(_question_marks(SUPPORTED_TYPES))
    bindings = list(SUPPORTED_TYPES)
    schema_csv = config.get("filter_schemas", "")
    schemas_ = [s.strip() for s in next(csv.reader([schema_csv]))]
    if schemas_:
        sql += "AND table_schema IN ({})".format(_question_marks(schemas_))
        bindings += schemas_
    with get_cursor(config) as cursor:
        cursor.execute(sql, bindings)
        yield from cursor

def _query_columns(config):
    """Queries the qsys2 columns catalog and returns an iterator containing the
    raw results."""
    with get_cursor(config) as cursor:
        sql = """
        """
        schema_csv = config.get("filter_schemas", "")
        binds = [s.strip() for s in next(csv.reader([schema_csv]))]
        if len(binds) != 0:
            sql = """
            SELECT table_schema,
                   table_name,
                   column_name,
                   data_type,
                   character_maximum_length,
                   numeric_precision,
                   numeric_scale,
                   ccsid
              FROM qsys2.syscolumns
             WHERE table_schema IN ({})
            """.format(_question_marks(binds))
            LOGGER.info("sql: %s, binds: %s", sql, binds)
            cursor.execute(sql, binds)
        else:
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
        yield from cursor

def _query_primary_keys(config):
    """Queries the qsys2 primary key catalog and returns an iterator containing
    the raw results."""
    with get_cursor(config) as cursor:
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
        yield from cursor


def _table_id(table):
    """Returns a 2-tuple that can be used to uniquely identify the table."""
    return (table.table_schema, table.table_name)


def _find_tables(config):
    # We use the qsys2.systables catalog rather than
    # information_schema.tables because it contains better information
    # about the "table_type." The information_schema table doesn't
    # distinguish between tables and data files.
    tbls = [Table(*rec) for rec in _query_tables(config)]
    return {_table_id(t): t for t in tbls}


def _col_table_id(column):
    """Returns a 2-tuple representing the column's table ID. See also
    _table_id."""
    return (column.table_schema, column.table_name)


def _find_columns(config, tables):
    cols = (Column(*rec) for rec in _query_columns(config))
    ret = {}
    for col in cols:
        table_id = _col_table_id(col)
        if table_id not in tables:
            continue
        if tables[table_id].table_type not in SUPPORTED_TYPES:
            continue
        if table_id not in ret:
            ret[table_id] = []
        ret[table_id].append(col)
    return ret


def _find_primary_keys(config, tables):
    """Returns a dict of tuples -> list where the keys are \"table ids\" -
    ie. the (schema name, table_name) and the values are the primary key
    columns, sorted by their ordinal position."""
    results = _query_primary_keys(config)
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


def _create_column_metadata(cols, schema, pk_columns):
    mdata = {() : {'table-key-properties' : pk_columns}}
    mdata = metadata.write(mdata, (), "selected-by-default", False)
    for col in cols:
        col_schema = schema.properties[col.column_name]
        mdata = metadata.write(mdata,
                               breadcrumb=("properties", col.column_name),
                               k="selected-by-default",
                               val=(col_schema.inclusion != "unsupported"))
        mdata = metadata.write(mdata,
                               breadcrumb=("properties", col.column_name),
                               k="sql-datatype",
                               val=col.data_type.lower())

    mdata = metadata.write(mdata, breadcrumb=(), k="valid-replication-keys",
                           val=schemas.valid_replication_keys(cols))
    return metadata.to_list(mdata)


def _update_entry_for_table_type(catalog_entry, table_type):
    catalog_entry.is_view = table_type == "V"
    # https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_71/db2/rbafzcatsystbls.htm
    known_types = {
        "A": "Alias",
        "L": "Logical file",
        "M": "Materialized query table",
        "P": "Physical file",
    }
    if table_type not in SUPPORTED_TYPES:
        catalog_entry.schema.inclusion = "unsupported"
        hint = known_types.get(table_type, "Unknown")
        err = "Unsupported table type {} ({})".format(table_type, hint)
        catalog_entry.schema.description = err


def discover(config):
    tables = _find_tables(config)
    columns = _find_columns(config, tables)
    pks = _find_primary_keys(config, tables)
    entries = []
    for table_id in tables:
        table_schema, table_name = table_id
        cols = columns.get(table_id, [])
        pk_columns = pks.get(table_id, [])
        schema = schemas.generate(cols, pk_columns)
        entry = CatalogEntry(
            database=table_schema,
            table=table_name,
            stream=table_name,
            metadata=_create_column_metadata(cols, schema, pk_columns),
            tap_stream_id="{}-{}".format(table_schema, table_name),
            schema=schema)

        _update_entry_for_table_type(entry, tables[table_id].table_type)
        entries.append(entry)
    return Catalog(entries)
