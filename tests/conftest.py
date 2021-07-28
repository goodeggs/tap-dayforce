import argparse
import json

import pytest

from dayforce_client import Dayforce
from singer.catalog import Catalog
from tap_dayforce.streams import (
    EmployeePunchesStream,
    EmployeeRawPunchesStream,
    EmployeesStream,
    PaySummaryReportStream,
)


@pytest.fixture(scope="function")
def config(shared_datadir):
    with open(shared_datadir / "test.config.json") as f:
        return json.load(f)


@pytest.fixture(scope="function")
def catalog(shared_datadir):
    return Catalog.load(shared_datadir / "test.catalog.json")


@pytest.fixture(scope="function")
def state(shared_datadir):
    with open(shared_datadir / "test.state.json") as f:
        return json.load(f)


@pytest.fixture(scope="function")
def employee_record(shared_datadir):
    with open(shared_datadir / "test.record.employee.json") as f:
        return json.load(f)


@pytest.fixture(scope="function")
def dayforce_client(config):
    return Dayforce(
        username=config.get("username"),
        password=config.get("password"),
        client_namespace=config.get("client_namespace"),
    )


@pytest.fixture(scope="function")
def args(dayforce_client, config, state, catalog, shared_datadir):
    args = argparse.Namespace()
    setattr(args, "client", dayforce_client)
    setattr(args, "config", config)
    setattr(args, "config_path", shared_datadir / "test.config.json")
    setattr(args, "state", state)
    setattr(args, "catalog", catalog)
    setattr(args, "catalog_path", shared_datadir / "test.catalog.json")
    return args


@pytest.fixture(
    scope="function", params={EmployeesStream, EmployeePunchesStream, EmployeeRawPunchesStream, PaySummaryReportStream}
)
def stream(dayforce_client, config, state, shared_datadir, request):
    return request.param(
        client=dayforce_client,
        config=config,
        config_path=shared_datadir / "test.config.json",
        state=state,
        catalog=catalog,
        catalog_path=shared_datadir / "test.catalog.json",
    )
