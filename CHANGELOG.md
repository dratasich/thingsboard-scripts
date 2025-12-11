## 0.3.0 (2025-12-11)

### Feat

- **telemetry**: add interval to metadata
- **telemetry**: append metadata to log multiple runs
- **telemetry**: support export of single device telemetry

## 0.2.0 (2025-12-11)

### Feat

- add TB host and device profile to device's metadata
- add script to download timeseries of all devices of a profile
- add rpc demo
- add script to export attributes by first column
- add demo to listen to attribute changes

### Fix

- add pyarrow for parquet export
- write *all* metadata
- avoid error when no telemetry data is returned by TB
- ruff complaints
- ruff rules for too long lines
