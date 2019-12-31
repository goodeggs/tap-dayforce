# tap-dayforce
[![PyPI version](https://badge.fury.io/py/tap-dayforce.svg)](https://badge.fury.io/py/tap-dayforce)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python Versions](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)](https://pypi.python.org/pypi/ansicolortags/)
[![Build Status](https://travis-ci.com/goodeggs/tap-pagerduty.svg?branch=master)](https://travis-ci.com/goodeggs/tap-dayforce.svg?branch=master)
[![Maintainability](https://api.codeclimate.com/v1/badges/7225a0fe514477edc17c/maintainability)](https://codeclimate.com/github/goodeggs/tap-dayforce/maintainability)

A [Singer](https://www.singer.io/) tap for extracting data from the [Dayforce REST API v1](https://developers.dayforce.com/Build/RESTful-general-URL-structure.aspx).

## Installation

Since package dependencies tend to conflict between various taps and targets, Singer [recommends](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-singer-with-python) installing taps and targets into their own isolated virtual environments:

### Install Dayforce Tap

```bash
$ python3 -m venv ~/.venvs/tap-dayforce
$ source ~/.venvs/tap-dayforce/bin/activate
$ pip3 install tap-dayforce
$ deactivate
```

### Install Stitch Target (optional)

```bash
$ python3 -m venv ~/.venvs/target-stitch
$ source ~/.venvs/target-stitch/bin/activate
$ pip3 install target-stitch
$ deactivate
```

## Configuration

The tap accepts a JSON-formatted configuration file as arguments. This configuration file has four required fields:

1. `username`: A valid Dayforce web services user account username.
2. `password`: A valid Dayforce web services user account password.
3. `client_namespace`: A valid client name (e.g. Company Name) that will be inserted into the request URL.
4. `start_date`: A date to fall back on when no `state.json` is provided. This determines how far back the tap looks for data within the Dayforce platform.

It's important to note that the Role attached to the User Account used in the configuration file must have at minimum the "Web Services" feature, as well as the "Read Data" sub-feature enabled.

An bare-bones Dayforce configuration may file may look like the following:

```json
{
  "username": "foo",
  "password": "bar",
  "client_namespace": "foo_bar",
  "start_date": "2019-01-01T00:00:00Z"
}
```

## Streams

The current version of the tap syncs four distinct [Streams](https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md#streams):
1. `Employees`: [Endpoint Documentation](https://developers.dayforce.com/Build/API-Explorer/Employee/GET-Employee-Details.aspx)
2. `EmployeePunches`: [Endpoint Documentation](https://developers.dayforce.com/Build/API-Explorer/Time-Management/Employee-Punches.aspx)
3. `EmployeeRawPunches` [Endpoint Documentation](https://developers.dayforce.com/Build/API-Explorer/Time-Management/Employee-Raw-Punches.aspx)
4. `PaySummaryReport` [Endpoint Documentation](https://developers.dayforce.com/Build/API-Explorer/Reporting/GET-Reports.aspx)

## Discovery

Singer taps describe the data that a stream supports via a [Discovery](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode) process. You can run the Dayforce tap in Discovery mode by passing the `--discover` flag at runtime:

```bash
$ ~/.venvs/tap-dayforce/bin/tap-dayforce --config=config/dayforce.config.json --discover
```

The tap will generate a [Catalog](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#the-catalog) to stdout. To pass the Catalog to a file instead, simply redirect it to a file:

```bash
$ ~/.venvs/tap-dayforce/bin/tap-dayforce --config=config/dayforce.config.json --discover > catalog.json
```

Discovery mode will not select all streams for replication by default. To instruct Discovery mode to select all streams for replication, use the `--select-all` flag:

```bash
$ ~/.venvs/tap-dayforce/bin/tap-dayforce --config=config/dayforce.config.json --discover --select-all > catalog.json
```

## Sync to stdout

Running a tap in [Sync mode](https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md#sync-mode) will extract data from the various selected Streams. In order to run a tap in Sync mode and have messages emitted to stdout, pass a valid configuration file and catalog file:

```bash
$ ~/.venvs/tap-dayforce/bin/tap-dayforce --config=config/dayforce.config.json --catalog=catalog.json
```

The tap will emit occasional [Metric](https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md#metric-messages), [Schema](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#schema-message), [Record](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#record-message), and [State messages](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#state-message). You can persist State between runs by redirecting messages to a file:

```bash
$ ~/.venvs/tap-dayforce/bin/tap-dayforce --config=config/dayforce.config.json --catalog=catalog.json >> state.json
$ tail -1 state.json > state.json.tmp
$ mv state.json.tmp state.json
```

## Sync to Stitch

You can also send the output of the tap to [Stitch Data](https://www.stitchdata.com/) for loading into the data warehouse. To do this, first create a JSON-formatted configuration for Stitch. This configuration file has two required fields:
1. `client_id`: The ID associated with the Stitch Data account you'll be sending data to.
2. `token`: The token associated with the specific [Import API integration](https://www.stitchdata.com/docs/integrations/import-api/) within the Stitch Data account.
3. `small_batch_url`: Default to "https://api.stitchdata.com/v2/import/batch"
4. `big_batch_url`: Default to "https://api.stitchdata.com/v2/import/batch"
5. `batch_size_preferences`: Default to `{}`

An example configuration file will look as follows:

```json
{
  "token": "foobar",
  "client_id": 12345,
  "small_batch_url": "https://api.stitchdata.com/v2/import/batch",
  "big_batch_url": "https://api.stitchdata.com/v2/import/batch",
  "batch_size_preferences": {}
}
```

Once the configuration file is created, simply pipe the output of the tap to the Stitch Data target and supply the target with the newly created configuration file:

```bash
$ ~/.venvs/tap-dayforce/bin/tap-dayforce --config=config/dayforce.config.json --catalog=catalog.json --state=state.json | ~/.venvs/target-stitch/bin/target-stitch --config=config/stitch.config.json >> state.json
$ tail -1 state.json > state.json.tmp
$ mv state.json.tmp state.json
```

## Contributing

1. The first step to contributing is getting a copy of the source code. First, [fork `tap-dayforce` on GitHub](https://github.com/goodeggs/tap-dayforce/fork). Then, `cd` into the directory where you want your copy of the source code to live and clone the source code:

```bash
$ cd repos
$ git clone git@github.com:YourGitHubName/tap-dayforce.git
```

2. Now that you have a copy of the source code on your machine, create and activate a virtual envionment for `tap-dayforce`:

```bash
$ python3 -mvenv ~/.venvs/tap-dayforce
$ source ~/.venvs/tap-dayforce/bin/activate
```

2. Once inside the virtual environment, run `make dev_install` at the root of the repository:

```bash
$ (tap-dayforce) make dev_install
```

3. Run the [tox](https://tox.readthedocs.io/en/latest/) testing suite in the appropriate python environment to ensure things are working properly:
```bash
$ (tap-dayforce) tox -e py37
```

To format your code using [isort](https://github.com/timothycrosley/isort) and [flake8](http://flake8.pycqa.org/en/latest/index.html) before commiting changes, run the following commands:

```bash
$ (tap-dayforce) make isort
$ (tap-dayforce) make flake8
```

Once you've confirmed that your changes work and the testing suite passes, feel free to put out a PR!
