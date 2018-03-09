from datetime import datetime, date, time
from time import time as time_
from collections import namedtuple
import pendulum
import singer
import singer.metrics as metrics
from singer.catalog import CatalogEntry
from singer import write_message as _emit
from singer import metadata
from .common import get_cursor

LOGGER = singer.get_logger()
_get_bk = singer.get_bookmark
_set_bk = singer.write_bookmark

ReplicationKey = namedtuple("ReplicationKey", ["column", "value"])


def _quote(x):
    return '"{}"'.format(x.replace('"', '""'))


def _get_stream_version(tap_stream_id, state):
    return _get_bk(state, tap_stream_id, "version") or int(time_() * 1000)


def _is_datetime_col(catalog_entry, column):
    return catalog_entry.schema.properties[column].format == "date-time"


def _get_replication_key(state: dict, catalog_entry: CatalogEntry):
    tap_stream_id = catalog_entry.tap_stream_id
    column = _get_bk(state, tap_stream_id, "replication_key")
    if not column:
        return None
    value = _get_bk(state, tap_stream_id, "replication_key_value")
    if value and _is_datetime_col(catalog_entry, column):
        value = pendulum.parse(value)
    return ReplicationKey(column, value)


def _column_sql(catalog_entry: CatalogEntry, column: str):
    data_type = metadata.get(metadata.to_map(catalog_entry.metadata),
                             breadcrumb=("properties", column),
                             k="sql-datatype")
    if data_type == "timestmp":  # No that's not a typo
        return "{} - CURRENT TIMEZONE".format(_quote(column))
    return _quote(column)


def _create_sql(catalog_entry: CatalogEntry, columns, rep_key: ReplicationKey):
    escaped_columns = [_column_sql(catalog_entry, c) for c in columns]
    select = "SELECT {} FROM {}.{}".format(
        ",".join(escaped_columns),
        _quote(catalog_entry.database),
        _quote(catalog_entry.table))
    params = ()
    if not rep_key:
        return select, params
    if rep_key.value:
        select += " WHERE {} >= ?".format(_quote(rep_key.column))
        params = (rep_key.value,)
    select += " ORDER BY {} ASC".format(_quote(rep_key.column))
    return select, params


def _row_to_record(catalog_entry, version, row, columns):
    row_to_persist = ()
    for elem in row:
        if isinstance(elem, datetime):
            row_to_persist += (elem.isoformat() + "+00:00",)
        elif isinstance(elem, date):
            row_to_persist += (elem.isoformat() + "T00:00:00+00:00",)
        elif isinstance(elem, time):
            epoch = datetime.utcfromtimestamp(0)
            epoch_with_time = datetime.combine(epoch.date(), elem)
            row_to_persist += (epoch_with_time.isoformat() + "+00:00",)
        else:
            row_to_persist += (elem,)
    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=dict(zip(columns, row_to_persist)),
        version=version)


def _write_metrics(catalog_entry, rows_saved):
    with metrics.record_counter(None) as counter:
        counter.tags["database"] = catalog_entry.database
        counter.tags["table"] = catalog_entry.table
        counter.increment(rows_saved)


def _activate_version(catalog_entry: CatalogEntry, version):
    _emit(singer.ActivateVersionMessage(stream=catalog_entry.stream,
                                        version=version))


def _maybe_activate_before_sync(state, catalog_entry, rep_key, stream_version):
    # If there's a replication key, we want to emit an ACTIVATE_VERSION
    # message at the beginning so the records show up right away. If
    # there's no bookmark at all for this stream, assume it's the very
    # first replication. That is, clients have never seen rows for this
    # stream before, so they can immediately acknowledge the present
    # version.
    tap_stream_id = catalog_entry.tap_stream_id
    bookmark_is_empty = not state.get("bookmarks", {}).get(tap_stream_id)
    if rep_key or bookmark_is_empty:
        _activate_version(catalog_entry, stream_version)


def _maybe_activate_after_sync(state, catalog_entry, rep_key, stream_version):
    # If there is no replication key, we"re doing "full table" replication,
    # and we need to activate this version at the end. Also clear the
    # stream's version from the state so that subsequent invocations will
    # emit a distinct stream version.
    if not rep_key:
        _activate_version(catalog_entry, stream_version)
        tap_stream_id = catalog_entry.tap_stream_id
        state = _set_bk(state, tap_stream_id, "version", None)
    return state


def _sync_table(config, state, catalog_entry):
    columns = list(catalog_entry.schema.properties)
    if not columns:
        LOGGER.warning(
            "There are no columns selected for table %s, skipping it",
            catalog_entry.table)
        return
    tap_stream_id = catalog_entry.tap_stream_id
    rep_key = _get_replication_key(state, catalog_entry)
    stream_version = _get_stream_version(tap_stream_id, state)
    state = _set_bk(state, tap_stream_id, "version", stream_version)
    _maybe_activate_before_sync(state, catalog_entry, rep_key, stream_version)
    select, params = _create_sql(catalog_entry, columns, rep_key)
    with get_cursor(config) as cursor:
        LOGGER.info("Running %s PARAMS (%s)", select, params)
        cursor.execute(select, params)
        rows_saved = 0
        for row in cursor:
            rows_saved += 1
            record_message = _row_to_record(catalog_entry, stream_version, row,
                                            columns)
            _emit(record_message)
            if rep_key:
                state = _set_bk(state, tap_stream_id, "replication_key_value",
                                record_message.record[rep_key.column])
            if rows_saved % 1000 == 0:
                _emit(singer.StateMessage(value=state))
        _write_metrics(catalog_entry, rows_saved)
    state = _maybe_activate_after_sync(state, catalog_entry, rep_key, stream_version)
    _emit(singer.StateMessage(value=state))


def sync(config, state, catalog):
    for catalog_entry in catalog.streams:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        state = singer.set_currently_syncing(state, catalog_entry.tap_stream_id)
        _emit(singer.StateMessage(value=state))
        if catalog_entry.is_view:
            key_properties = metadata.to_map(catalog_entry.metadata).get((), {}).get('view-key-properties')
        else:
            key_properties = metadata.to_map(catalog_entry.metadata).get((), {}).get('table-key-properties')


        _emit(singer.SchemaMessage(
            stream=catalog_entry.stream,
            schema=catalog_entry.schema.to_dict(),
            key_properties=key_properties,
            bookmark_properties=replication_key
          ))
        with metrics.job_timer("sync_table") as timer:
            timer.tags["schema"] = catalog_entry.database
            timer.tags["table"] = catalog_entry.table
            _sync_table(config, state, catalog_entry)
    state = singer.set_currently_syncing(state, None)
    _emit(singer.StateMessage(value=state))
