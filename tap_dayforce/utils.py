import argparse
import json
import logging
import os
import time
from typing import Callable, Dict, Set

import requests
import singer
from dayforce_client.response import DayforceResponse


def get_abs_path(path: str) -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id: str, schema_path: str = "schemas") -> Dict:
    path = get_abs_path(schema_path)
    return singer.utils.load_json(f"{path}/{tap_stream_id}.json")


def is_fatal_code(e: requests.exceptions.RequestException) -> bool:
    """Helper function to determine if a Requests reponse status code
    is a "fatal" status code. If it is, the backoff decorator will giveup
    instead of attemtping to backoff."""
    return 400 <= e.response.status_code < 500 and e.response.status_code != 429


def load_json(path: str) -> Dict:
    with open(path) as fil:
        return json.load(fil)


def parse_args(required_config_keys: Set[str]) -> argparse.Namespace:
    """Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    -a,--select-all Selects all streams in the Catalog for replication.
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", help="Config file", required=True)

    parser.add_argument("-s", "--state", help="State file")

    parser.add_argument("--catalog", help="Catalog file")

    parser.add_argument("-d", "--discover", action="store_true", help="Do schema discovery")

    parser.add_argument(
        "-a", "--select-all", action="store_true", help="Selects all streams in the Catalog for replication."
    )

    args = parser.parse_args()
    if args.config:
        setattr(args, "config_path", args.config)
        args.config = load_json(args.config)
    if args.state:
        setattr(args, "state_path", args.state)
        args.state = load_json(args.state)
    else:
        args.state = {}
    if args.catalog:
        setattr(args, "catalog_path", args.catalog)
        args.catalog = singer.Catalog.load(args.catalog)

    check_config(args.config, required_config_keys)

    return args


def check_config(config: Dict, required_config_keys: Set[str]):
    missing_keys = [key for key in required_config_keys if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))


def handle_rate_limit(func: Callable, logger: logging.Logger) -> DayforceResponse:
    try:
        response = func
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            sleep_time = int(e.response.headers["Retry-After"]) + 1
            logger.info(f"Rate limit reached. Retrying in {sleep_time} seconds..")
            time.sleep(sleep_time)
            response = func
        else:
            raise
    finally:
        return response
