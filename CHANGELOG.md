## 0.3.2

 - Added ability to target a Dayforce test environment via config file. Bumped `dayforce-client` requirement from `0.2.0` to `0.2.1`.

## 0.3.1

 - Added optional `dayforce_release` and `api_version` parameters to the `DayforceStream` class method.

## 0.3.0

 - Added data from the [`/Schedules`](https://usr57-services.dayforcehcm.com/api/goodeggs/Swagger/) endpoint to `EmployeesStream`.
 - Reformatted library according to [black](https://black.readthedocs.io/en/stable/) code style and added tox style checks.

## 0.2.0

 - Refactor to make tap more robust.
 - Implement iterative sync for Pay Summary Report to get around 20,000 row max return limit.
 - Added fibonaccial decay backoff to the `_get()` method on all Dayforce streams.
 - Made Rollbar logging more explicitly optional.

## 0.1.1

 - Depracate `activate_version()` for `ReportStream` and instead generate a surrogate MD5 hash primary key for each received row.

## 0.1.0

 - Added in functionality to log and skip empty Employee records returned by Dayforce.
