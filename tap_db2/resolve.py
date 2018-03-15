"""Functions which accept user input, like a state or catalog, and modifies
them for use during the sync process.

The functions herein were taken nearly verbatim from
https://github.com/singer-io/tap-mysql/"""
from itertools import dropwhile
import singer
from singer import metadata
import singer.schema
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

LOGGER = singer.get_logger()


def _desired_columns(selected, table_schema):

    '''Return the set of column names we need to include in the SELECT.
    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    '''
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in table_schema.properties.items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
        if inclusion == 'automatic':
            automatic.add(column)
        elif inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported.add(column)
        else:
            raise Exception('Unknown inclusion ' + inclusion)

    selected_but_unsupported = selected.intersection(unsupported)
    if selected_but_unsupported:
        LOGGER.warning(
            'Columns %s were selected but are not supported. Skipping them.',
            selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning(
            'Columns %s were selected but do not exist.',
            selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning(
            'Columns %s are primary keys but were not selected. Adding them.',
            not_selected_but_automatic)

    return selected.intersection(available).union(automatic)


def resolve_catalog(catalog, discovered, state):
    '''Returns the Catalog of data we're going to sync.
    Takes the Catalog we read from the input file and turns it into a
    Catalog representing exactly which tables and columns we're going to
    emit in this process. Compares the input Catalog to a freshly
    discovered Catalog to determine the resulting Catalog. Returns a new
    instance. The result may differ from the input Catalog in the
    following ways:
      * It will only include streams marked as "selected".
      * We will remove any streams and columns that were selected but do
        not actually exist in the database right now.
      * If the state has a currently_syncing, we will skip to that stream and
        drop all streams appearing before it in the catalog.
      * We will add any columns that were not selected but should be
        automatically included. For example, primary key columns and
        columns used as replication keys.
    '''

    # Filter catalog to include only selected streams
    streams = list(filter(lambda stream: stream.is_selected(), catalog.streams))

    # If the state says we were in the middle of processing a stream, skip
    # to that stream.
    currently_syncing = singer.get_currently_syncing(state)
    if currently_syncing:
        streams = dropwhile(lambda s: s.tap_stream_id != currently_syncing, streams)

    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        discovered_table = discovered.get_stream(catalog_entry.tap_stream_id)
        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist',
                           catalog_entry.database, catalog_entry.table)
            continue
        selected = set([k for k, v in catalog_entry.schema.properties.items()
                        if v.selected or k == replication_key])

        # These are the columns we need to select
        columns = _desired_columns(selected, discovered_table.schema)

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            stream=catalog_entry.stream,
            metadata=catalog_entry.metadata,
            database=catalog_entry.database,
            table=catalog_entry.table,
            is_view=catalog_entry.is_view,
            schema=Schema(
                type='object',
                properties={col: discovered_table.schema.properties[col]
                            for col in columns}
            )
        ))

    return result


def build_state(raw_state, catalog):
    state = {}

    currently_syncing = singer.get_currently_syncing(raw_state)
    if currently_syncing:
        state = singer.set_currently_syncing(state, currently_syncing)

    for catalog_entry in catalog.streams:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get('replication-key')
        if replication_key:
            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'replication_key',
                                          replication_key)

            # Only keep the existing replication_key_value if the
            # replication_key hasn't changed.
            raw_replication_key = singer.get_bookmark(raw_state,
                                                      catalog_entry.tap_stream_id,
                                                      'replication_key')
            if raw_replication_key == replication_key:
                raw_replication_key_value = singer.get_bookmark(raw_state,
                                                                catalog_entry.tap_stream_id,
                                                                'replication_key_value')
                state = singer.write_bookmark(state,
                                              catalog_entry.tap_stream_id,
                                              'replication_key_value',
                                              raw_replication_key_value)

        # Persist any existing version, even if it's None
        if raw_state.get('bookmarks', {}).get(catalog_entry.tap_stream_id):
            raw_stream_version = singer.get_bookmark(raw_state,
                                                     catalog_entry.tap_stream_id,
                                                     'version')

            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'version',
                                          raw_stream_version)

    return state
