#!/usr/bin/env python3
import singer
from singer import utils
from singer.catalog import Catalog
from . import resolve, sync, discovery, common

REQUIRED_CONFIG_KEYS = ["host", "user", "password"]
LOGGER = singer.get_logger()


def do_sync(args, input_catalog):
    state = resolve.build_state(args.state, input_catalog)
    catalog = resolve.resolve_catalog(input_catalog, input_catalog, state)
    sync.sync(args.config, state, catalog)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    common.setup_port_configuration(args.config)
    if args.discover:
        discovery.discover(args.config).dump()
        print()
    elif args.catalog:
        do_sync(args, args.catalog)
    elif args.properties:
        do_sync(args, Catalog.from_dict(args.properties))
    else:
        LOGGER.info("No properties were selected")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical("unknown top-level tap exception", exc_info=exc)
        raise
