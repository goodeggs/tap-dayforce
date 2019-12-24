import logging
import os
import json
from typing import Dict

import rollbar
import singer
from rollbar.logger import RollbarHandler

from .streams import EmployeePunchesStream, EmployeeRawPunchesStream, EmployeesStream, PaySummaryReportStream
from .utils import get_abs_path, load_schema, parse_args

AVAILABLE_STREAMS = {
    EmployeePunchesStream,
    EmployeeRawPunchesStream,
    EmployeesStream,
    PaySummaryReportStream
}

LOGGER = singer.get_logger()

try:
    ROLLBAR_ACCESS_TOKEN = os.environ["ROLLBAR_ACCESS_TOKEN"]
    ROLLBAR_ENVIRONMENT = os.environ["ROLLBAR_ENVIRONMENT"]
except KeyError:
    LOGGER.info("Required env vars for Rollbar logging not found. Rollbar logging disabled..")
    log_to_rollbar = False
else:
    rollbar.init(ROLLBAR_ACCESS_TOKEN, ROLLBAR_ENVIRONMENT)
    log_to_rollbar = True


def discover(args, select_all=False):
    LOGGER.info('Starting discovery..')

    catalog = {"streams": []}
    for stream in AVAILABLE_STREAMS:
        schema = load_schema(stream.tap_stream_id)
        catalog_entry = {
            "stream": stream.tap_stream_id,
            "tap_stream_id": stream.tap_stream_id,
            "schema": schema,
            "metadata": singer.metadata.get_standard_metadata(schema=schema,
                                                              key_properties=stream.key_properties,
                                                              valid_replication_keys=stream.bookmark_properties,
                                                              replication_method=stream.replication_method)
        }

        if select_all is True:
            catalog_entry["metadata"][0]["metadata"]["selected"] = True
        catalog["streams"].append(catalog_entry)

    print(json.dumps(catalog, indent=2))
    LOGGER.info('Finished discovery..')


def sync(args):
    LOGGER.info('Starting sync..')

    selected_streams = {catalog_entry.stream for catalog_entry in args.catalog.get_selected_streams(args.state)}
    LOGGER.info(f"Selected Streams: {selected_streams}")

    for available_stream in AVAILABLE_STREAMS:
        stream = available_stream.from_args(args)
        if stream.tap_stream_id in selected_streams:
            print(stream.catalog.get_stream(stream.tap_stream_id).schema.to_dict())
            LOGGER.info(f"Starting sync for Stream {stream.tap_stream_id}..")
            singer.bookmarks.set_currently_syncing(state=args.state, tap_stream_id=stream.tap_stream_id)
            stream.write_state_message()
            stream.write_schema_message()
            stream.sync()
            singer.bookmarks.set_currently_syncing(state=args.state, tap_stream_id=None)
            stream.write_state_message()


def main():
    args = parse_args(required_config_keys={"username", "password", "client_namespace", "start_date"})
    if args.discover:
        try:
            discover(args, select_all=args.select_all)
        except:
            LOGGER.exception('Caught exception during Discovery..')
            if log_to_rollbar is True:
                rollbar.report_exc_info()
    else:
        try:
            sync(args)
        except:
            LOGGER.exception('Caught exception during Sync..')
            if log_to_rollbar is True:
                rollbar.report_exc_info()


if __name__ == "__main__":
    main()
