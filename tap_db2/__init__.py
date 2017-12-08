#!/usr/bin/env python3
import os
import sys
import json
from collections import namedtuple
import singer
from singer import utils
from singer.catalog import Catalog
from .discovery import discover
from . import catalogs, sync

REQUIRED_CONFIG_KEYS = ["start_date", "db2_system", "db2_uid", "db2_pwd"]
LOGGER = singer.get_logger()


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        discover(args.config).dump()
        print()
    else:
        if not os.getenv("DEVELOPMENT_FLAG"):
            return
        input_catalog = Catalog.from_dict(args.properties)
        catalog = catalogs.resolve(input_catalog, input_catalog, args.state)
        sync.sync(args.config, args.state, catalog)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical("unknown top-level tap exception", exc_info=exc)
        raise
