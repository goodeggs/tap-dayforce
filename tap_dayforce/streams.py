import hashlib
import os
from datetime import timedelta
from typing import ClassVar, Dict, Iterable, List, Optional, Union

import attr
import backoff
import requests
import singer
from dayforce_client import Dayforce

from .utils import is_fatal_code
from .whitelisting import (WHITELISTED_COLLECTIONS, WHITELISTED_FIELDS,
                           WHITELISTED_PAY_POLICY_CODES)

LOGGER = singer.get_logger()


@attr.s
class DayforceStream(object):

    client: Dayforce = attr.ib(validator=attr.validators.instance_of(Dayforce))
    config: Dict = attr.ib(repr=False, validator=attr.validators.instance_of(Dict))
    config_path: Union[os.PathLike, str] = attr.ib()
    state: Dict = attr.ib(validator=attr.validators.instance_of(Dict))
    catalog: Optional[singer.catalog.Catalog] = attr.ib(validator=attr.validators.optional(attr.validators.instance_of(singer.catalog.Catalog)),
                                                        repr=False,
                                                        default=None)
    catalog_path: Optional[Union[os.PathLike, str]] = attr.ib(default=None)

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

    @staticmethod
    def get_schema(tap_stream_id: str, catalog: singer.catalog.Catalog) -> Dict:
        return catalog.get_stream(tap_stream_id).schema.to_dict()

    @staticmethod
    def get_bookmark(config: Dict, tap_stream_id: str, state: Dict, bookmark_properties: str) -> Optional[str]:
        bookmark = singer.bookmarks.get_bookmark(state, tap_stream_id, key=bookmark_properties)
        if bookmark is not None:
            return bookmark
        else:
            return config.get("start_date")


@attr.s
class DayforcePunchStream(DayforceStream):

    def sync(self):
        with singer.metrics.job_timer(job_type=f"sync_{self.tap_stream_id}"):
            with singer.metrics.record_counter(endpoint=self.tap_stream_id) as counter:
                start = singer.utils.strptime_to_utc(self.get_bookmark(self.config, self.tap_stream_id, self.state, self.bookmark_properties))
                new_bookmark = singer.utils.now()
                singer.bookmarks.write_bookmark(state=self.state, tap_stream_id=self.tap_stream_id, key=self.bookmark_properties, val=singer.utils.strftime(new_bookmark))
                step = timedelta(days=6)
                while start < new_bookmark:
                    end = start + step - timedelta(seconds=1)
                    self._transform_records(start, end, counter)
                    start += step


@attr.s
class EmployeePunchesStream(DayforcePunchStream):
    tap_stream_id: ClassVar[str] = 'employee_punches'
    key_properties: ClassVar[List[str]] = ['PunchXRefCode']
    bookmark_properties: ClassVar[str] = 'SyncTimestampUtc'
    replication_method: ClassVar[str] = 'INCREMENTAL'

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.HTTPError,
                          max_time=120,
                          giveup=is_fatal_code,
                          logger=LOGGER)
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError,
                           requests.exceptions.Timeout),
                          max_time=120,
                          logger=LOGGER)
    def _transform_records(self, start, end, counter):
        for _, record in self.client.get_employee_punches(filterTransactionStartTimeUTC=singer.utils.strftime(start), filterTransactionEndTimeUTC=singer.utils.strftime(end)).yield_records():
            if record:
                record["SyncTimestampUtc"] = self.get_bookmark(self.config, self.tap_stream_id, self.state, self.bookmark_properties)
                with singer.Transformer() as transformer:
                    transformed_record = transformer.transform(data=record, schema=self.get_schema(self.tap_stream_id, self.catalog))
                    singer.write_record(stream_name=self.tap_stream_id, time_extracted=singer.utils.now(), record=transformed_record)
                    counter.increment()


@attr.s
class EmployeeRawPunchesStream(DayforcePunchStream):
    tap_stream_id: ClassVar[str] = 'employee_raw_punches'
    key_properties: ClassVar[List[str]] = ['RawPunchXRefCode']
    bookmark_properties: ClassVar[str] = 'SyncTimestampUtc'
    replication_method: ClassVar[str] = 'INCREMENTAL'

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.HTTPError,
                          max_time=120,
                          giveup=is_fatal_code,
                          logger=LOGGER)
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError,
                           requests.exceptions.Timeout),
                          max_time=120,
                          logger=LOGGER)
    def _transform_records(self, start, end, counter):
        for _, record in self.client.get_employee_raw_punches(filterTransactionStartTimeUTC=singer.utils.strftime(start), filterTransactionEndTimeUTC=singer.utils.strftime(end)).yield_records():
            if record:
                record["SyncTimestampUtc"] = self.get_bookmark(self.config, self.tap_stream_id, self.state, self.bookmark_properties)
                with singer.Transformer() as transformer:
                    transformed_record = transformer.transform(data=record, schema=self.get_schema(self.tap_stream_id, self.catalog))
                    singer.write_record(stream_name=self.tap_stream_id, time_extracted=singer.utils.now(), record=transformed_record)
                    counter.increment()


@attr.s
class EmployeesStream(DayforceStream):
    tap_stream_id: ClassVar[str] = 'employees'
    key_properties: ClassVar[List[str]] = ['XRefCode']
    bookmark_properties: ClassVar[str] = 'SyncTimestampUtc'
    replication_method: ClassVar[str] = 'INCREMENTAL'

    def whitelist_sensitive_info(self, data: Dict) -> Dict:
        for collection in WHITELISTED_COLLECTIONS:
            if data.get(collection, {}).get("Items") is not None:
                for i, item in enumerate(data.get(collection, {}).get("Items")):
                    if item.get("PayPolicy") is None or item.get("PayPolicy").get("XRefCode") not in WHITELISTED_PAY_POLICY_CODES:
                        for field in WHITELISTED_FIELDS:
                            data[collection]["Items"][i].pop(field, None)

        return data

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.HTTPError,
                          max_time=120,
                          giveup=is_fatal_code,
                          logger=LOGGER)
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError,
                           requests.exceptions.Timeout),
                          max_time=120,
                          logger=LOGGER)
    def _transform_records(self, start, end, counter):
        for _, record in self.client.get_employees(filterUpdatedStartDate=singer.utils.strftime(start), filterUpdatedEndDate=singer.utils.strftime(end)).yield_records():
            if record:
                details = self.client.get_employee_details(xrefcode=record.get("XRefCode"), expand="WorkAssignments,Contacts,EmploymentStatuses,Roles,EmployeeManagers,CompensationSummary,Locations,LastActiveManagers").get("Data")
                if details:
                    details["SyncTimestampUtc"] = self.get_bookmark(self.config, self.tap_stream_id, self.state, self.bookmark_properties)
                    details = self.whitelist_sensitive_info(data=details)
                    with singer.Transformer() as transformer:
                        transformed_record = transformer.transform(data=details, schema=self.get_schema(self.tap_stream_id, self.catalog))
                        singer.write_record(stream_name=self.tap_stream_id, time_extracted=singer.utils.now(), record=transformed_record)
                        counter.increment()

    def sync(self):
        with singer.metrics.job_timer(job_type=f"sync_{self.tap_stream_id}"):
            with singer.metrics.record_counter(endpoint=self.tap_stream_id) as counter:
                start = singer.utils.strptime_to_utc(self.get_bookmark(self.config, self.tap_stream_id, self.state, self.bookmark_properties))
                new_bookmark = singer.utils.now()
                singer.bookmarks.write_bookmark(state=self.state, tap_stream_id=self.tap_stream_id, key=self.bookmark_properties, val=singer.utils.strftime(new_bookmark))
                self._transform_records(start, new_bookmark, counter)


@attr.s
class PaySummaryReportStream(DayforceStream):
    tap_stream_id: ClassVar[str] = 'pay_summary_report'
    key_properties: ClassVar[List[str]] = ['HashKey']
    bookmark_properties: ClassVar[str] = 'SyncTimestampUtc'
    replication_method: ClassVar[str] = 'INCREMENTAL'

    def _generate_md5_hash(self, input_sequence: Iterable[Optional[str]]) -> str:
        m = hashlib.md5()
        for input in input_sequence:
            m.update(str(input).encode('utf-8'))
        return m.hexdigest()

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.HTTPError,
                          max_time=120,
                          giveup=is_fatal_code,
                          logger=LOGGER)
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError,
                           requests.exceptions.Timeout),
                          max_time=120,
                          logger=LOGGER)
    def _transform_records(self, start, end, counter):
        report_params = {
            "003cd1ea-5f11-4fe8-ae9c-d7af1e3a95d6": singer.utils.strftime(start),
            "b03cd1ea-5f11-4fe8-ae9c-d7af1e3a95d6": singer.utils.strftime(end)
        }
        for _, row in self.client.get_report(xrefcode=self.tap_stream_id, **report_params).yield_report_rows():
            if row:
                row["HashKey"] = self._generate_md5_hash(row.values())
                row["SyncTimestampUtc"] = self.get_bookmark(self.config, self.tap_stream_id, self.state, self.bookmark_properties)
                with singer.Transformer() as transformer:
                    transformed_record = transformer.transform(data=row, schema=self.get_schema(self.tap_stream_id, self.catalog))
                    singer.write_record(stream_name=self.tap_stream_id, time_extracted=singer.utils.now(), record=transformed_record)
                    counter.increment()

    def sync(self):
        with singer.metrics.job_timer(job_type=f"sync_{self.tap_stream_id}"):
            with singer.metrics.record_counter(endpoint=self.tap_stream_id) as counter:
                start = singer.utils.strptime_to_utc(self.get_bookmark(self.config, self.tap_stream_id, self.state, self.bookmark_properties))
                new_bookmark = singer.utils.now()
                singer.bookmarks.write_bookmark(state=self.state, tap_stream_id=self.tap_stream_id, key=self.bookmark_properties, val=singer.utils.strftime(new_bookmark))
                step = timedelta(days=1)
                while start < new_bookmark:
                    end = start + step - timedelta(seconds=1)
                    self._transform_records(start, end, counter)
                    start += step
