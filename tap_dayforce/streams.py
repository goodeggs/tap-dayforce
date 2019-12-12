import hashlib
import inspect
import os
import time
from datetime import timedelta
from typing import Dict, Generator, Iterable

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


class DayforceStream:
    BASE_URL = "https://usr57-services.dayforcehcm.com/Api"

    def __init__(self, config: Dict, state: Dict):
        self.username = config.get('username')
        self.password = config.get('password')
        self.client_name = config.get('client_name')
        self.email = config.get('email')
        self.params = {}
        self.state = state
        self.schema = self._load_schema()
        self.metadata = singer.metadata.get_standard_metadata(schema=self._load_schema(),
                                                              key_properties=self.key_properties,
                                                              valid_replication_keys=self.valid_replication_keys,
                                                              replication_method=self.replication_method)

        config_stream_params = config.get('streams', {}).get(self.tap_stream_id)

        if config_stream_params is not None:
            for key in config_stream_params.keys():
                if key not in self.valid_params:
                    raise DayforceException(f"/{self.tap_stream_id} endpoint does not support '{key}' parameter.")

            self.params.update(config_stream_params)

        for param in self.required_params:
            if param not in self.params.keys():
                raise DayforceException(f"Parameter '{param}' required but not supplied for /{self.tap_stream_id} endpoint.")

    def _get_abs_path(self, path: str) -> str:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def _load_schema(self) -> Dict:
        '''Loads a JSON schema file for a given
        Dayforce resource into a dict representation.
        '''
        schema_path = self._get_abs_path("schemas")
        return singer.utils.load_json(f"{schema_path}/{self.tap_stream_id}.json")

    def _construct_headers(self) -> Dict:
        '''Constructs a standard set of headers for GET requests.'''
        headers = requests.utils.default_headers()
        headers["User-Agent"] = f"python-dayforce-tap/{__version__}"
        headers["Content-Type"] = "application/json"
        headers["From"] = self.email
        headers["Date"] = singer.utils.strftime(singer.utils.now(), '%a, %d %b %Y %H:%M:%S %Z')
        return headers

    def _get(self, resource: str, params: Dict = None) -> Dict:
        '''Constructs a standard way of making
        a GET request to the Dayforce REST API.
        '''
        url = f"{self.BASE_URL}/{self.client_name}/V1/{resource}"
        headers = self._construct_headers()
        response = requests.get(url, auth=(self.username, self.password), headers=headers, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After')) + 1
            LOGGER.warn(f"Rate limit reached. Trying again in {retry_after} seconds: {response.text}")
            time.sleep(retry_after)
            response = requests.get(url, auth=(self.username, self.password), headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def _get_records(self, resource: str, params: Dict = None) -> Generator[Dict, None, None]:
        '''Yields a Dict representing a record of the requested resource.'''
        resp = DayforceResponse(client=self,
                                resource=resource,
                                params=params,
                                response=self._get(resource=resource, params=params))

        return resp

    def get(self, key: str):
        '''Custom get method so that Singer can
        access Class attributes using dict syntax.
        '''
        return inspect.getattr_static(self, key, default=None)

    def write_schema_message(self):
        '''Writes a Singer schema message.'''
        return singer.write_schema(stream_name=self.stream, schema=self.schema, key_properties=self.key_properties)

    def write_state_message(self):
        '''Writes a Singer state message.'''
        return singer.write_state(self.state)


class DayforceResponse:
    """
    Class to enable response pagination via iteration.
    """
    def __init__(self, client, resource, params, response):
        self.client = client
        self.resource = resource
        self.params = params
        self.response = response

    def __iter__(self):
        self._iteration = 0
        return self

    def __next__(self):
        self._iteration += 1
        if self._iteration == 1:
            return self

        # If Pagination is not available on endpoint.
        if self.response.get("Paging") is None:
            raise StopIteration
        # If Pagination section has a blank "Next" value.
        elif self.response.get("Paging").get("Next") == "":
            raise StopIteration
        else:
            next_page = self.response.get("Paging").get("Next")
            headers = self.client._construct_headers()
            response = requests.get(next_page, auth=(self.client.username, self.client.password), headers=headers)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After')) + 1
                LOGGER.warn(f"Rate limit reached. Trying again in {retry_after} seconds: {response.text}")
                time.sleep(retry_after)
                response = requests.get(next_page, auth=(self.client.username, self.client.password), headers=headers)
            response.raise_for_status()
            self.response = response.json()
            return self

    def get(self, key, default=None):
        return self.response.get(key, default)


class DayforceException(BaseException):
    pass


class EmployeesStream(DayforceStream):
    tap_stream_id = 'employees'
    stream = 'employees'
    replication_key = 'SyncTimestampUtc'
    valid_replication_keys = ['SyncTimestampUtc']
    key_properties = 'XRefCode'
    replication_method = 'INCREMENTAL'
    required_params = []
    valid_params = ['contextDate', 'expand']

    def __init__(self, config: Dict, state: Dict):
        super().__init__(config, state)

    def sync(self):

        new_bookmark = singer.utils.strftime(singer.utils.now(), BOOKMARK_DATETIME_STR_FMT)
        current_bookmark = singer.bookmarks.get_bookmark(state=self.state,
                                                         tap_stream_id=self.tap_stream_id,
                                                         key=self.replication_key)

        if current_bookmark is not None:
            self.params.update({
                "filterUpdatedStartDate": current_bookmark,
                "filterUpdatedEndDate": new_bookmark
            })

        self.state = singer.bookmarks.write_bookmark(state=self.state,
                                                     tap_stream_id=self.tap_stream_id,
                                                     key=self.replication_key,
                                                     val=new_bookmark)

        with singer.metrics.job_timer(job_type=f"sync_{self.tap_stream_id}"):
            with singer.metrics.record_counter(endpoint=self.tap_stream_id) as counter:
                for page in self._get_records(resource='Employees', params=self.params):
                    for employee in page.get("Data"):
                        if not employee:
                            msg = f"Dayforce returned an empty {self.tap_stream_id} record. Skipping it.."
                            LOGGER.warning(msg)
                            rollbar.report_message(msg, 'warning')
                            continue
                        else:
                            resp = self._get(resource=f"Employees/{employee.get('XRefCode')}", params=self.params)
                            if resp.get('Data') is None:
                                msg = f"Dayforce returned an empty record for employee {employee.get('XRefCode')}. Skipping it.."
                                LOGGER.warning(msg)
                                rollbar.report_message(msg, 'warning')
                                continue
                            else:
                                data = resp.get('Data')
                                # Custom blacklisting for sensitive pay information.
                                try:
                                    for collection in WHITELISTED_COLLECTIONS:
                                        for i, item in enumerate(data.get(collection).get("Items")):
                                            if item.get("PayPolicy") is None:
                                                for field in WHITELISTED_FIELDS:
                                                    data[collection]["Items"][i].pop(field, None)
                                            elif item.get("PayPolicy").get("XRefCode") not in WHITELISTED_PAY_POLICY_CODES:
                                                for field in WHITELISTED_FIELDS:
                                                    data[collection]["Items"][i].pop(field, None)
                                except Exception as e:
                                    msg = f"""
                                    Sensitive information blacklist failed for Employee {employee.get('XRefCode')}. Skipping it..
                                    Exception: {e}
                                    """
                                    LOGGER.warning(msg)
                                    rollbar.report_message(msg, 'warning')
                                    continue
                                data['SyncTimestampUtc'] = new_bookmark
                                with singer.Transformer() as transformer:
                                    transformed_record = transformer.transform(data=data, schema=self.schema)
                                    singer.write_record(stream_name=self.stream, time_extracted=singer.utils.now(), record=transformed_record)
                                    counter.increment()


class EmployeePunchesStream(DayforceStream):
    tap_stream_id = 'employee_punches'
    stream = 'employee_punches'
    replication_key = 'SyncTimestampUtc'
    valid_replication_keys = ['SyncTimestampUtc']
    key_properties = 'PunchXRefCode'
    replication_method = 'INCREMENTAL'
    required_params = ['filterTransactionStartTimeUTC']
    valid_params = [
        'filterTransactionStartTimeUTC',
        'filterTransactionEndTimeUTC',
        'employeeXRefCode',
        'locationXRefCode',
        'positionXRefCode',
        'departmentXRefCode',
        'jobXRefCode',
        'shiftStatus',
        'filterShiftTimeStart',
        'filterShiftTimeEnd',
        'businessDate',
        'pageSize'
    ]

    def __init__(self, config: Dict, state: Dict):
        super().__init__(config, state)

    def sync(self):

        new_bookmark = singer.utils.strftime(singer.utils.now(), BOOKMARK_DATETIME_STR_FMT)

        self.params.update({
            "filterTransactionEndTimeUTC": new_bookmark
        })

        current_bookmark = singer.bookmarks.get_bookmark(state=self.state,
                                                         tap_stream_id=self.tap_stream_id,
                                                         key=self.replication_key)

        if current_bookmark is not None:
            self.params.update({
                "filterTransactionStartTimeUTC": current_bookmark
            })

        self.state = singer.bookmarks.write_bookmark(state=self.state,
                                                     tap_stream_id=self.tap_stream_id,
                                                     key=self.replication_key,
                                                     val=new_bookmark)

        start = singer.utils.strptime_to_utc(self.params.get('filterTransactionStartTimeUTC'))
        end = singer.utils.strptime_to_utc(self.params.get('filterTransactionEndTimeUTC'))
        step = timedelta(days=7)

        with singer.metrics.job_timer(job_type=f"sync_{self.tap_stream_id}"):
            with singer.metrics.record_counter(endpoint=self.tap_stream_id) as counter:
                while start < end:
                    range = {
                        "filterTransactionStartTimeUTC": singer.utils.strftime(start),
                        "filterTransactionEndTimeUTC": min(singer.utils.strftime(start + step), singer.utils.strftime(end))
                    }
                    self.params.update(range)
                    for page in self._get_records(resource='EmployeePunches', params=self.params):
                        for punch in page.get("Data"):
                            if not punch:
                                msg = f"Dayforce returned an empty {self.tap_stream_id} record. Skipping it.."
                                LOGGER.warning(msg)
                                rollbar.report_message(msg, 'warning')
                                continue
                            else:
                                punch["SyncTimestampUtc"] = new_bookmark
                                with singer.Transformer() as transformer:
                                    transformed_record = transformer.transform(data=punch, schema=self.schema)
                                    singer.write_record(stream_name=self.stream, time_extracted=singer.utils.now(), record=transformed_record)
                                    counter.increment()
                    start += step


class EmployeeRawPunchesStream(DayforceStream):
    tap_stream_id = 'employee_raw_punches'
    stream = 'employee_raw_punches'
    replication_key = 'SyncTimestampUtc'
    valid_replication_keys = ['SyncTimestampUtc']
    key_properties = 'RawPunchXRefCode'
    replication_method = 'INCREMENTAL'
    required_params = ['filterTransactionStartTimeUTC']
    valid_params = [
        'filterTransactionStartTimeUTC',
        'filterTransactionEndTimeUTC',
        'employeeXRefCode',
        'employeeBadge',
        'punchState',
        'punchTypes',
        'pageSize'
    ]

    def __init__(self, config: Dict, state: Dict):
        super().__init__(config, state)

    def sync(self):

        new_bookmark = singer.utils.strftime(singer.utils.now(), BOOKMARK_DATETIME_STR_FMT)

        self.params.update({
            "filterTransactionEndTimeUTC": new_bookmark
        })

        current_bookmark = singer.bookmarks.get_bookmark(state=self.state,
                                                         tap_stream_id=self.tap_stream_id,
                                                         key=self.replication_key)

        if current_bookmark is not None:
            self.params.update({
                "filterTransactionStartTimeUTC": current_bookmark
            })

        self.state = singer.bookmarks.write_bookmark(state=self.state,
                                                     tap_stream_id=self.tap_stream_id,
                                                     key=self.replication_key,
                                                     val=new_bookmark)

        start = singer.utils.strptime_to_utc(self.params.get('filterTransactionStartTimeUTC'))
        end = singer.utils.strptime_to_utc(self.params.get('filterTransactionEndTimeUTC'))
        step = timedelta(days=7)

        with singer.metrics.job_timer(job_type=f"sync_{self.tap_stream_id}"):
            with singer.metrics.record_counter(endpoint=self.tap_stream_id) as counter:
                while start < end:
                    range = {
                        "filterTransactionStartTimeUTC": singer.utils.strftime(start),
                        "filterTransactionEndTimeUTC": min(singer.utils.strftime(start + step), singer.utils.strftime(end))
                    }
                    self.params.update(range)
                    for page in self._get_records(resource='EmployeeRawPunches', params=self.params):
                        for punch in page.get("Data"):
                            if not punch:
                                msg = f"Dayforce returned an empty {self.tap_stream_id} record. Skipping it.."
                                LOGGER.warning(msg)
                                rollbar.report_message(msg, 'warning')
                                continue
                            else:
                                punch["SyncTimestampUtc"] = new_bookmark
                                with singer.Transformer() as transformer:
                                    transformed_record = transformer.transform(data=punch, schema=self.schema)
                                    singer.write_record(stream_name=self.stream, time_extracted=singer.utils.now(), record=transformed_record)
                                    counter.increment()
                    start += step


class ReportStream(DayforceStream):
    DATA_TYPE_MAPPING = {
        "String": {"type": ["null", "string"]},
        "Integer": {"type": ["null", "integer"]},
        "Decimal": {"type": ["null", "number"]},
        "DateTime": {
            "type": ["null", "string"],
            "format": "date-time"
        },
        "Time": {
            "type": ["null", "string"]
        },
        "Date": {
            "type": ["null", "string"],
            "format": "date-time"
        }
    }
    key_properties = []
    valid_replication_keys = []
    replication_method = 'FULL_TABLE'
    required_params = []

    def __init__(self, config: Dict, state: Dict, xrefcode: str):
        self.tap_stream_id = f"report_{xrefcode}"
        self.stream = f"report_{xrefcode}"
        self.xrefcode = xrefcode
        self.key_properties = 'hash_pk'
        self.valid_replication_keys = []
        super().__init__(config, state)

    def _generate_schema(self, report_xrefcode: str) -> Dict:
        '''Dynamically generates a schema for a Report by inspecting the ReportMetadata endpoint.'''
        resp = self._get(resource=f"ReportMetadata/{report_xrefcode}")
        column_metadata = resp.get("Data")[0].get("ColumnMetadata")
        schema = {
            "type": ["null", "object"],
            "additionalProperties": False,
            "properties": {
                "hash_pk": {
                    "type": ["null", "string"]
                }
            }
        }
        for column in column_metadata:
            field = column.get("CodeName").replace(".", "_")
            if column.get("DataType") not in self.DATA_TYPE_MAPPING.keys():
                raise TypeError(f"Column {column.get('DisplayName')} has data type {column.get('DataType')} which is not implemented.")
            else:
                schema.get("properties").update({field: self.DATA_TYPE_MAPPING.get(column.get("DataType"))})

        return schema

    def _load_schema(self) -> Dict:
        return self._generate_schema(report_xrefcode=self.xrefcode)

    def _generate_md5_hash(self, input_sequence: Iterable[str]) -> str:
        m = hashlib.md5()
        for input in input_sequence:
            m.update(input.encode('utf-8'))
        return m.hexdigest()

    def sync(self):
        with singer.metrics.job_timer(job_type=f"sync_{self.tap_stream_id}"):
            with singer.metrics.record_counter(endpoint=self.tap_stream_id) as counter:
                for page in self._get_records(resource=f"Reports/{self.xrefcode}", params=self.params):
                    for row in page.get("Data").get("Rows"):
                        row["hash_pk"] = self._generate_md5_hash(row.values())
                        with singer.Transformer() as transformer:
                            transformed_record = transformer.transform(data=row, schema=self.schema)
                            singer.write_message(singer.RecordMessage(stream=self.stream,
                                                                      record=transformed_record,
                                                                      time_extracted=singer.utils.now()))
                            counter.increment()


AVAILABLE_STREAMS = {EmployeesStream, EmployeePunchesStream, EmployeeRawPunchesStream, ReportStream}
