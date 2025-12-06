"""Script to get telemetry of a specific device or all devices of a profile.

Exports the telemetry data in the specified output format (csv or parquet).
Additionally creates a metadata.json file with device and attribute information.

Example usage (get the token from ThingsBoard UI / account / security):
```
# get telemetry for devices listed in test.csv
python telemetry.py \
    --host https://<hostname> \
    --token <token> \
    --device-profile <device_profile_name> \
    --start-time 2023-01-01T00:00:00Z \
    --end-time 2023-01-31T23:59:59Z \
    --keys temperature,humidity

# see usage help
python telemetry.py -h
```

Prepend `LOGURU_LEVEL=TRACE` to the command to enable *all* logs
(be careful, even TB responses).

Check the number of data points per device and time range.
Adapt the time window size in the code if you get too many data points
for a single request (default is 24 hours with max 1000 points).
Maximum number of devices in a profile is 1000 (adapt if needed).
"""

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
from loguru import logger
from tb_rest_client.models.models_ce.entity_id import EntityId
from tb_rest_client.rest_client_ce import RestClientCE

# --- constants ---

device_limit = 1000  # max number of devices to query per profile
points_limit = 1000  # max number of telemetry points to query per request
batch_window = timedelta(hours=24)  # time window per request

# --- helpers ---


def device_query(profile_name: str) -> dict:
    """Template for device query to get all devices of a profile."""
    return {
        "entityFilter": {
            "type": "deviceType",
            "deviceType": profile_name,
        },
        "pageLink": {
            "pageSize": device_limit,
            "page": 0,
        },
    }


def ts_to_datetime(ts: int) -> datetime:
    """Convert milliseconds since epoch to datetime."""
    return datetime.fromtimestamp(ts / 1000, tz=UTC)


# --- main ---

if __name__ == "__main__":
    # arguments
    argparser = argparse.ArgumentParser(description="Export Attribute")
    argparser.add_argument(
        "--host",
        type=str,
        help="ThingsBoard host, e.g. https://demo.thingsboard.io",
        required=True,
    )
    argparser.add_argument(
        "--token",
        type=str,
        help="ThingsBoard token (may include 'Bearer ' prefix)",
        required=True,
    )
    argparser.add_argument(
        "--output-format",
        type=str,
        choices=["csv", "parquet"],
        default="csv",
        help="Output format for the telemetry data",
    )
    argparser.add_argument(
        "--device-profile",
        type=str,
        help="ThingsBoard device profile name to get telemetry for",
        required=True,
    )
    argparser.add_argument(
        "--start-time",
        type=str,
        help="Start time for telemetry data (ISO format)",
        default="1970-01-01T00:00:00Z",
    )
    argparser.add_argument(
        "--end-time",
        type=str,
        help="End time for telemetry data (ISO format)",
        default=datetime.now(UTC).isoformat(),
    )
    argparser.add_argument(
        "--keys",
        type=str,
        help="Comma-separated list of telemetry keys to export"
        "(if not specified, all keys are exported)",
    )
    argparser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )
    args = argparser.parse_args()
    logger.trace(f"Arguments: {args}")  # note this logs sensitive info!
    if not args.verbose:
        logger.remove()
        logger.add(sys.stderr, level="INFO")

    # tb rest/http client
    tb = RestClientCE(
        base_url=args.host,
    )
    logger.info(f"Connecting to ThingsBoard {args.host}")
    token = args.token.removeprefix("Bearer ").strip()
    tb.token_login(token)

    # get all devices of the profile
    logger.info(f"Get devices of profile {args.device_profile}...")
    res = tb.entity_query_controller.find_entity_data_by_query_using_post(
        async_req=False,
        body=device_query(args.device_profile),
    )
    device_ids = [r.entity_id.id for r in res.data]
    logger.trace(f"Device IDs: {device_ids}")
    logger.info(f"Found {len(device_ids)} devices of profile {args.device_profile}")

    # collect device data
    all_meta: list[dict] = []
    for device_id in device_ids:
        meta = {
            "device_id": device_id,
            "telemetry": {},
        }

        # --- attributes ---
        logger.debug(f"Get attributes for device {device_id}...")

        attribute_keys = tb.get_attribute_keys(EntityId(device_id, "DEVICE"))
        logger.debug(f"Attribute keys: {attribute_keys}")

        res = tb.get_attributes(EntityId(device_id, "DEVICE"), ",".join(attribute_keys))
        meta["attributes"] = {a["key"]: a["value"] for a in res}
        logger.trace(f"Attributes for device {device_id}: {meta['attributes']}")

        # --- telemetry ---
        logger.debug(f"Get telemetry for device {device_id}...")

        telemetry_keys = tb.telemetry_controller.get_timeseries_keys_using_get1(
            async_req=False,
            entity_type="DEVICE",
            entity_id=device_id,
        )
        logger.debug(f"Telemetry keys: {telemetry_keys}")
        meta["telemetry"]["keys"] = telemetry_keys
        if args.keys:
            telemetry_keys = [
                key for key in telemetry_keys if key in args.keys.split(",")
            ]
            logger.debug(f"Filtered telemetry keys: {telemetry_keys}")
        meta["telemetry"]["exported_keys"] = telemetry_keys

        dt_start = datetime.fromisoformat(args.start_time)
        dt_end = datetime.fromisoformat(args.end_time)
        dt_current = dt_start

        df = pd.DataFrame(columns=["ts", *telemetry_keys])
        while dt_current < dt_end:
            # specify time window in milliseconds since epoch
            ts = int(dt_current.timestamp() * 1000)
            te = int(min(dt_current + batch_window, dt_end).timestamp() * 1000)

            logger.debug(f"Device {device_id}, keys {telemetry_keys} from {ts} to {te}")
            res = tb.telemetry_controller.get_timeseries_using_get(
                async_req=False,
                entity_type="DEVICE",
                entity_id=device_id,
                keys=",".join(telemetry_keys),
                start_ts=ts,
                end_ts=te,
                limit=points_limit,
            )
            logger.trace(res)

            # collect values per timestamp
            # {"key": [{"ts": 123456789, "value": 123}, ...], ...}
            records: dict[int, dict[str, Any]] = {}
            for key, samples in res.items():
                for sample in samples:
                    ts = sample["ts"]
                    value = sample["value"]
                    row = records.setdefault(ts, {"ts": ts})
                    row[key] = value

            # append to dataframe
            if len(records) > 0:
                df_part = pd.DataFrame.from_records(list(records.values()))
                df = pd.concat([df, df_part], ignore_index=True)

            # increment time window
            dt_current += batch_window

        if len(df) == 0:
            logger.warning(f"No telemetry data for device {device_id}")
            continue

        df = df.sort_values("ts")
        logger.debug(f"Telemetry for {device_id} from {dt_start} to {dt_end}\n{df}")

        # save to file
        output_file = (
            f"{dt_start.date()}_{dt_end.date()}_{device_id}.{args.output_format}"
        )
        if args.output_format == "csv":
            df.to_csv(output_file, index=False)
        elif args.output_format == "parquet":
            df.to_parquet(output_file, index=False)
        logger.info(f"Saved telemetry data for device {device_id} to {output_file}")
        meta["telemetry"]["file"] = output_file
        meta["telemetry"]["size"] = len(df)

    # save metadata
    all_meta.append(meta)
    meta_file = f"{dt_start.date()}_{dt_end.date()}_metadata.json"
    with open(meta_file, "w") as f:  # noqa: PTH123
        json.dump(all_meta, f, indent=2)
    logger.info(f"Saved metadata to {meta_file}")
