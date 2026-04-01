"""Delete telemetries (timeseries) for devices or assets in an entity group."""

import argparse
import logging

import requests

parser = argparse.ArgumentParser(
    description=(
        "Delete telemetries (timeseries) for devices or assets in an entity group."
    ),
)
parser.add_argument(
    "--host",
    type=str,
    required=True,
    help="ThingsBoard host, e.g. https://demo.thingsboard.io",
)
parser.add_argument(
    "--token",
    type=str,
    required=True,
    help="ThingsBoard bearer token (may include 'Bearer ' prefix).",
)
parser.add_argument(
    "-g",
    "--group-id",
    required=True,
    help="The ID of the entity group (devices or assets) to delete telemetries from.",
)
parser.add_argument(
    "-n",
    "--names",
    required=False,
    help="Comma-separated list of entity names to delete timeseries from.",
)

keys_group = parser.add_mutually_exclusive_group(required=True)
keys_group.add_argument(
    "--all",
    dest="delete_all",
    action="store_true",
    help="Delete all telemetry keys for each entity.",
)
keys_group.add_argument(
    "--keys",
    dest="keys",
    help="Comma-separated list of telemetry keys to delete.",
)

parser.add_argument(
    "-d",
    "--dry-run",
    action="store_true",
    help="Disable actual deletion, only logs what would be deleted.",
)
parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
args = parser.parse_args()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

logger.info("Using host: %s", args.host)

names = args.names.split(",") if args.names else []
keys_to_delete = args.keys.split(",") if args.keys else []

_TIMEOUT = 30


class TBClient:
    """ThingsBoard REST API client."""

    def __init__(self, url: str, token: str) -> None:
        """Initialize client with base URL and bearer token."""
        self._tb_base_url = url
        self._tb_token = token.removeprefix("Bearer ").strip()

    def _get_headers(self) -> dict:
        """Return headers for TB API requests."""
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._tb_token}",
        }

    def get_entities(
        self,
        group_id: str,
    ) -> list[dict]:
        """Return entities of a group, optionally filtered by profile."""
        headers = self._get_headers()
        url = (
            f"{self._tb_base_url}/api/entityGroup/{group_id}/entities"
            "?pageSize=10000&page=0"
        )
        response = requests.get(url, headers=headers, timeout=_TIMEOUT)
        response.raise_for_status()

        entities = response.json()["data"]
        logger.debug("Found %d entities in group %s.", len(entities), group_id)
        return [
            {
                "id": e["id"]["id"],
                "name": e["name"],
                "type": e["id"]["entityType"],
                "profile": e.get(f"{e['id']['entityType'].lower()}_profile"),
            }
            for e in entities
        ]

    def get_timeseries_keys(self, entity_id: str, entity_type: str) -> list[str]:
        """Return all timeseries keys for an entity."""
        headers = self._get_headers()
        url = (
            f"{self._tb_base_url}/api/plugins/telemetry"
            f"/{entity_type}/{entity_id}/keys/timeseries"
        )
        response = requests.get(url, headers=headers, timeout=_TIMEOUT)
        response.raise_for_status()
        return sorted(response.json())

    def delete_timeseries(
        self,
        entity_id: str,
        entity_type: str,
        keys: list[str],
        *,
        dry_run: bool = False,
    ) -> None:
        """Delete the given timeseries keys for an entity."""
        if not keys:
            logger.debug("No timeseries to delete for %s %s.", entity_type, entity_id)
            return

        url = (
            f"{self._tb_base_url}/api/plugins/telemetry"
            f"/{entity_type}/{entity_id}/timeseries/delete"
        )
        if dry_run:
            logger.info(
                "Dry run: would delete %d timeseries for %s %s: %s.",
                len(keys),
                entity_type,
                entity_id,
                keys,
            )
        else:
            headers = self._get_headers()
            response = requests.delete(
                url,
                headers=headers,
                params={
                    "keys": ",".join(keys),
                    "deleteAllDataForKeys": "true",
                },
                timeout=_TIMEOUT,
            )
            response.raise_for_status()
            logger.info(
                "Deleted %d timeseries from %s %s: %s.",
                len(keys),
                entity_type,
                entity_id,
                keys,
            )


tb = TBClient(
    url=args.host,
    token=args.token,
)

if args.group_id:
    group = tb.get_entities(group_id=args.group_id)

if names:
    logger.info("Filtering to entity names: %s", names)
    entities = [e for e in group if e["name"] in names]
else:
    entities = group

logger.info("Processing %d entities...", len(entities))

for entity in entities:
    logger.debug("Processing entity %s (%s)...", entity["name"], entity["type"])
    if args.delete_all:
        keys = tb.get_timeseries_keys(entity["id"], entity["type"])
        logger.debug("All keys for %s: %s", entity["name"], keys)
    else:
        keys = keys_to_delete

    tb.delete_timeseries(
        entity_id=entity["id"],
        entity_type=entity["type"],
        keys=keys,
        dry_run=args.dry_run,
    )

logger.info("Done.")
