import logging
import os

import rollbar
import singer
from rollbar.logger import RollbarHandler

from .streams import AVAILABLE_STREAMS, ReportStream

ROLLBAR_ACCESS_TOKEN = os.environ["ROLLBAR_ACCESS_TOKEN"]
ROLLBAR_ENVIRONMENT = os.environ["ROLLBAR_ENVIRONMENT"]

LOGGER = singer.get_logger()

rollbar.init(ROLLBAR_ACCESS_TOKEN, ROLLBAR_ENVIRONMENT)
rollbar_handler = RollbarHandler()
# Only send level WARNING and above to Rollbar.
rollbar_handler.setLevel(logging.WARNING)
LOGGER.addHandler(rollbar_handler)


def discover(config, state={}):
    LOGGER.info('Starting discovery..')
    data = {}
    data['streams'] = []
    for available_stream in AVAILABLE_STREAMS:
        if available_stream == ReportStream:
            if config.get("streams", {}).get("reports") is None:
                LOGGER.info('No Reports to replicate. Moving on..')
                continue
            else:
                for report in config.get("streams").get("reports").keys():
                    data['streams'].append(available_stream(config=config, state=state, xrefcode=report))
        else:
            data['streams'].append(available_stream(config=config, state=state))
    catalog = singer.catalog.Catalog.from_dict(data=data)
    singer.catalog.write_catalog(catalog)
    LOGGER.info('Finished discovery..')


def sync(config, catalog, state):
    LOGGER.info('Starting sync..')
    selected_streams = {catalog_entry.stream for catalog_entry in catalog.get_selected_streams(state)}

    streams_to_sync = set()
    for available_stream in AVAILABLE_STREAMS:
        if available_stream == ReportStream:
            if config.get("streams", {}).get("reports") is None:
                LOGGER.info('No Reports to replicate. Moving on..')
                continue
            else:
                for report in config.get("streams").get("reports").keys():
                    stream = available_stream(config=config, state=state, xrefcode=report)
                    if stream.stream in selected_streams:
                        streams_to_sync.add(stream)

        elif available_stream.stream in selected_streams:
            streams_to_sync.add(available_stream(config=config, state=state))

    for stream in streams_to_sync:
        singer.bookmarks.set_currently_syncing(state=stream.state, tap_stream_id=stream.tap_stream_id)
        stream.write_state_message()
        stream.write_schema_message()
        stream.sync()
        singer.bookmarks.set_currently_syncing(state=stream.state, tap_stream_id=None)
        stream.write_state_message()


def main():
    args = singer.utils.parse_args(required_config_keys=["username", "password", "client_name", "email"])
    if args.discover:
        try:
            discover(config=args.config)
        except:
            LOGGER.exception('Caught exception during Discovery..')
            rollbar.report_exc_info()
    else:
        try:
            sync(config=args.config, catalog=args.catalog, state=args.state)
        except:
            LOGGER.exception('Caught exception during Sync..')
            rollbar.report_exc_info()


if __name__ == "__main__":
    main()
