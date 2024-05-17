## 4.1.1

 - Fix breaking changes

## 4.1.0

 - Added helper to handle 401 responses so entire call will not break.

## 3.0.0

## 2.0.0

 - If an Item does not pass the whitelist, the entire Item will be omitted (where previously we only omitted certain fields).

## 1.0.0

 - Whitelist PayClass

## 0.3.1

 - Added optional `dayforce_release`, `api_version`, and `test` parameters to the `DayforceStream` class method.
 - Bumped `dayforce-client` requirement from `0.2.0` to `0.2.1`.

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
