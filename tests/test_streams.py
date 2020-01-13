import copy

import pytest

from tap_dayforce import (EmployeePunchesStream, EmployeeRawPunchesStream,
                          EmployeesStream, PaySummaryReportStream)
from tap_dayforce.whitelisting import (WHITELISTED_COLLECTIONS,
                                       WHITELISTED_FIELDS)


@pytest.mark.parametrize('pt_stream', [EmployeesStream, EmployeeRawPunchesStream, EmployeePunchesStream, PaySummaryReportStream])
def test_stream_from_args_class_method(pt_stream, args, dayforce_client, config, state, catalog, shared_datadir):
    stream = pt_stream.from_args(args)
    assert stream.client == dayforce_client
    assert stream.config == config
    assert stream.config_path == shared_datadir / 'test.config.json'
    assert stream.state == state
    assert stream.catalog == catalog
    assert stream.catalog_path == shared_datadir / 'test.catalog.json'


@pytest.mark.parametrize('pt_stream', [EmployeesStream, EmployeeRawPunchesStream, EmployeePunchesStream, PaySummaryReportStream])
def test_get_schema_static_method(pt_stream, catalog):
    schema = pt_stream.get_schema(tap_stream_id=pt_stream.tap_stream_id, catalog=catalog)
    assert isinstance(schema, dict)


@pytest.mark.parametrize('pt_stream', [EmployeesStream, EmployeeRawPunchesStream, EmployeePunchesStream])
def test_get_bookmark_static_method(pt_stream, config, state):
    expected = state.get("bookmarks").get(pt_stream.tap_stream_id).get(pt_stream.bookmark_properties)
    bookmark = pt_stream.get_bookmark(config, pt_stream.tap_stream_id, state, pt_stream.bookmark_properties)
    assert expected == bookmark


@pytest.mark.parametrize('pay_policy_xrefcode', ['SALARIED', 'CONTRACTOR', None,
                                                 pytest.param('USA_CA_HNE', marks=pytest.mark.xfail),
                                                 pytest.param('USA_CA_HNE_4', marks=pytest.mark.xfail),
                                                 pytest.param('USA_CA_HNEWHSE', marks=pytest.mark.xfail),
                                                 pytest.param('USA_CA_HNEDRIVER', marks=pytest.mark.xfail)])
def test_employee_stream_whitelist(args, employee_record, pay_policy_xrefcode):
    stream = EmployeesStream.from_args(args)
    for collection in WHITELISTED_COLLECTIONS:
        employee_record[collection]["Items"][0]["PayPolicy"]["XRefCode"] = pay_policy_xrefcode

    expected = copy.deepcopy(employee_record)
    for collection in WHITELISTED_COLLECTIONS:
        for field in WHITELISTED_FIELDS:
            expected[collection]["Items"][0].pop(field, None)

    output = stream.whitelist_sensitive_info(data=employee_record)

    assert expected == output
