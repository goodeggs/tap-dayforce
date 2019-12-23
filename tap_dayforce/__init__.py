import logging
import os
import json

import rollbar
import singer
from rollbar.logger import RollbarHandler

from typing import Dict

from .streams import EmployeePunchesStream

AVAILABLE_STREAMS = {
    EmployeePunchesStream
}

ROLLBAR_ACCESS_TOKEN = os.environ["ROLLBAR_ACCESS_TOKEN"]
ROLLBAR_ENVIRONMENT = os.environ["ROLLBAR_ENVIRONMENT"]

LOGGER = singer.get_logger()

def get_abs_path(path: str) -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(tap_stream_id: str, schema_path: str = 'schemas') -> Dict:
    path = get_abs_path(schema_path)
    return singer.utils.load_json(f"{path}/{tap_stream_id}.json")

def discover(args, select_all=False):
    LOGGER.info('Starting discovery..')

    catalog = {"streams": []}
    for available_stream in AVAILABLE_STREAMS:
        stream = available_stream.from_args(args)
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
    args = singer.utils.parse_args(required_config_keys=["username", "password", "client_namespace"])
    if args.discover:
        try:
            discover(args, select_all=True)
        except:
            LOGGER.exception('Caught exception during Discovery..')
            rollbar.report_exc_info()
    else:
        try:
            sync(args)
        except:
            LOGGER.exception('Caught exception during Sync..')
            rollbar.report_exc_info()


if __name__ == "__main__":
    main()
