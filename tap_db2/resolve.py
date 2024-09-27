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


def resolve_catalog(catalog, discovered, state):
    '''Returns the Catalog of data we're going to sync.
    Takes the Catalog we read from the input file and turns it into a
    Catalog representing exactly which tables and columns we're going to
    emit in this process. Compares the input Catalog to a freshly
    discovered Catalog to determine the resulting Catalog. Returns a new
    instance. The result may differ from the input Catalog in the
    following ways:
      * It will only include streams marked as "selected".
      * If the state has a currently_syncing, we will skip to that stream and
        drop all streams appearing before it in the catalog.
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

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            stream=catalog_entry.stream,
            metadata=catalog_entry.metadata,
            database=catalog_entry.database,
            table=catalog_entry.table,
            is_view=catalog_entry.is_view,
            schema=discovered_table.schema,
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
