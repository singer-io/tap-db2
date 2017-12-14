#!/usr/bin/env python3
import singer
from singer import utils
from singer.catalog import Catalog
from . import resolve, sync, discovery

REQUIRED_CONFIG_KEYS = ["db2_system", "db2_uid", "db2_pwd"]
LOGGER = singer.get_logger()


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        discovery.discover(args.config).dump()
        print()
    else:
        input_catalog = Catalog.from_dict(args.properties)
        catalog = resolve.resolve(input_catalog, input_catalog, args.state)
        sync.sync(args.config, args.state, catalog)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical("unknown top-level tap exception", exc_info=exc)
        raise
