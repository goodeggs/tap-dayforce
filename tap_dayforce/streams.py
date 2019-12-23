import hashlib
import inspect
import os
import time
from datetime import timedelta
from typing import Dict, Generator, Iterable, List, Optional

import attr
from dayforce_client import Dayforce
import requests
import rollbar
import singer

from .version import __version__

LOGGER = singer.get_logger()

BOOKMARK_DATETIME_STR_FMT = "%Y-%m-%dT%H:%M:%SZ"

WHITELISTED_COLLECTIONS = {
    'CompensationSummary',
    'EmploymentStatuses'
}

WHITELISTED_FIELDS = {
    'BaseRate',
    'BaseSalary',
    'PreviousBaseRate',
    'PreviousBaseSalary',
    'ChangeValue',
    'ChangePercent'
}

WHITELISTED_PAY_POLICY_CODES = {
    'USA_CA_HNE',
    'USA_CA_HNE_4',
    'USA_CA_HNEWHSE',
    'USA_CA_HNEDRIVER'
}

def is_fatal_code(e: requests.exceptions.RequestException) -> bool:
    '''Helper function to determine if a Requests reponse status code
    is a "fatal" status code. If it is, the backoff decorator will giveup
    instead of attemtping to backoff.'''
    return 400 <= e.response.status_code < 500 and e.response.status_code != 429


@attr.s
class DayforceStream(object):

    client: Dayforce = attr.ib(validator=attr.validators.instance_of(Dayforce))
    config: Dict = attr.ib(repr=False,
                           validator=attr.validators.instance_of(Dict))
    config_path: str = attr.ib(validator=attr.validators.instance_of(str))
    catalog: Optional[singer.catalog.Catalog] = attr.ib(validator=attr.validators.optional(attr.validators.instance_of(singer.catalog.Catalog)),
                                                        repr=False,
                                                        default=None)
    catalog_path: Optional[str] = attr.ib(validator=attr.validators.optional(attr.validators.instance_of(str)),
                                          default=None)
    state: Dict = attr.ib(validator=attr.validators.instance_of(Dict), factory=dict)
    schema: Dict = attr.ib(repr=False, factory=dict, validator=attr.validators.instance_of(Dict))

    @classmethod
    def from_args(cls, args, **kwargs):
        return cls(client=Dayforce(username=args.config.get("username"),
                                   password=args.config.get("password"),
                                   client_namespace=args.config.get("client_namespace")),
                   config=args.config,
                   config_path=args.config_path,
                   catalog=getattr(args, 'catalog', None),
                   catalog_path=getattr(args, 'catalog_path', None),
                   state=args.state,
                   **kwargs)

    def get_schema(self):
        return self.catalog.get_stream(self.tap_stream_id).schema.to_dict()

    def write_schema_message(self):
        '''Writes a Singer schema message.'''
        return singer.write_schema(stream_name=self.tap_stream_id, schema=self.get_schema(), key_properties=self.key_properties)

    def write_state_message(self):
        '''Writes a Singer state message.'''
        return singer.write_state(self.state)


@attr.s
class EmployeePunchesStream(DayforceStream):

    tap_stream_id: str = attr.ib(default='employee_punches')
    key_properties: List[str] = attr.ib(default=['PunchXRefCode'])
    bookmark_properties: str = attr.ib(default='SyncTimestampUtc')
    replication_method: str = attr.ib(default='INCREMENTAL')

    def sync(self):
        with singer.metrics.job_timer(job_type=f"sync_{self.tap_stream_id}"):
            with singer.metrics.record_counter(endpoint=self.tap_stream_id) as counter:
                for _, record in self.client.get_employee_punches().yield_records():
                    with singer.Transformer() as transformer:
                        transformed_record = transformer.transform(data=record, schema=self.get_schema())
                        singer.write_record(stream_name=self.tap_stream_id, time_extracted=singer.utils.now(), record=transformed_record)
                        counter.increment()
