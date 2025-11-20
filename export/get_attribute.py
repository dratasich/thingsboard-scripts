"""Script to get an attribute for a list of devices from ThingsBoard.

The first column of the CSV file is the attribute to match on (e.g., `serialNumber`).

Example usage:
```
# get attribute `<attribute_key>` for devices listed in test.csv
python get_attribute.py \
    --host https://<hostname> \
    --username "<email>" --password "***" \
    --csv <test.csv> \
    --attribute <attribute_key>
```
"""

import argparse
import logging

import pandas as pd
from tb_rest_client.rest_client_pe import (
    RestClientPE,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_attribute(
    client: RestClientPE,
    match_attribute_key: str,
    match_attribute_value: str,
    get_attribute_key: str,
) -> str:
    """Query an attribute given another's value per device.

    Searches for devices where `match_attribute_key == match_attribute_value`
    and returns the value of `get_attribute_key` attribute for that device.

    Uses the entities query API of ThingsBoard.

    Limitations: Works only for match_attribute_value of type STRING.
    Adapt the query if you need other types.
    """
    query = {
        "entityFilter": {
            "type": "entityType",
            "resolveMultiple": True,
            "entityType": "DEVICE",
        },
        "entityFields": [
            {"type": "ENTITY_FIELD", "key": "name"},
        ],
        "latestValues": [
            {"type": "ATTRIBUTE", "key": match_attribute_key},
            {"type": "ATTRIBUTE", "key": get_attribute_key},
        ],
        "keyFilters": [
            {
                "key": {"type": "ATTRIBUTE", "key": match_attribute_key},
                "valueType": "STRING",
                "predicate": {
                    "operation": "EQUAL",
                    "value": {
                        "defaultValue": match_attribute_value,
                        "dynamicValue": None,
                    },
                    "type": "STRING",
                },
            },
        ],
        "pageLink": {"page": 0, "pageSize": 2},
    }
    logger.debug("Querying device: %s", query)

    res = client.entity_query_controller.find_entity_data_by_query_using_post(
        body=query,
    )
    if len(res.data) != 1:
        msg = (
            f"Expected exactly one device with {match_attribute_key} "
            + match_attribute_value
            + f", found {len(res.data)}"
        )
        raise ValueError(msg)

    logger.debug(
        "Found device %s: %s",
        res.data[0].entity_id.id,
        res.data[0].latest["ATTRIBUTE"],
    )
    logger.debug(
        "%s -> %s",
        match_attribute_value,
        res.data[0].latest["ATTRIBUTE"][get_attribute_key].value,
    )
    return res.data[0].latest["ATTRIBUTE"][get_attribute_key].value


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
        "--username",
        type=str,
        help="ThingsBoard username",
        required=True,
    )
    argparser.add_argument(
        "--password",
        type=str,
        help="ThingsBoard password",
        required=True,
    )
    argparser.add_argument(
        "--csv",
        type=str,
        required=True,
        help="Path to the CSV file (requires column '<')",
    )
    argparser.add_argument(
        "--attribute",
        type=str,
        help="Attribute to search and append per row",
        required=True,
    )
    argparser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )
    args = argparser.parse_args()

    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    logger.debug("Arguments: %s", args)

    # tb rest/http client
    tb = RestClientPE(
        base_url=args.host,
    )
    logger.info("Connecting to ThingsBoard %s as user %s", args.host, args.username)
    tb.login(args.username, args.password)

    # read csv file
    df = pd.read_csv(args.csv)

    # first column is the attribute to match on
    match_attribute_key = df.columns[0]
    logger.info("Using '%s' as matching attribute.", match_attribute_key)

    # drop rows with missing match attribute
    df = df.dropna(subset=[match_attribute_key])
    logger.info("Loaded %d rows from CSV file.", len(df))
    logger.debug("Dataframe:\n%s", df)

    # get attribute for each row
    df[args.attribute] = df[match_attribute_key].apply(
        lambda row: get_attribute(
            tb,
            match_attribute_key,
            str(row),
            args.attribute,
        ),
    )
    logger.info("Enriched dataframe with attribute '%s'.", args.attribute)
    logger.debug("Enriched Dataframe:\n%s", df)

    # save to csv
    output_csv = args.csv.replace(".csv", f"_with_{args.attribute}.csv")
    df.to_csv(output_csv, index=False)
    logger.info("Saved enriched data to '%s'.", output_csv)
