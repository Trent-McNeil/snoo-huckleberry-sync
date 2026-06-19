"""SNOO data source: authenticate and fetch PubNub ActivityState history."""

import logging
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

import aiohttp
from python_snoo.snoo import Snoo
from python_snoo.containers import SnooData

log = logging.getLogger(__name__)

PUBNUB_SUB_KEY = "sub-c-97bade2a-483d-11e6-8b3b-02ee2ddab7fe"
PUBNUB_ORIGIN = "happiestbaby.pubnubapi.com"
PUBNUB_HISTORY_COUNT = 100


def event_time(sd: SnooData) -> datetime:
    return datetime.utcfromtimestamp(sd.event_time_ms / 1000).replace(tzinfo=timezone.utc)


async def fetch_events(
    websession: aiohttp.ClientSession,
    username: str,
    password: str,
    lookback_hours: float,
) -> List[SnooData]:
    """Authenticate to SNOO and return ActivityState events within the lookback window."""
    snoo = Snoo(username, password, websession)
    auth_info = await snoo.authorize()

    snoo_token = getattr(auth_info, "snoo", None)
    if not snoo_token:
        raise RuntimeError("No PubNub snoo token in auth response")

    devices = await snoo.get_devices()
    if not devices:
        raise RuntimeError("No SNOO devices found on this account")

    serial = (
        getattr(devices[0], "serialNumber", None)
        or getattr(devices[0], "serial_number", None)
    )
    if not serial:
        raise RuntimeError("Could not resolve device serial number")

    log.debug("Fetching PubNub history for channel ActivityState.%s", serial)
    events = await _fetch_pubnub_history(websession, serial, snoo_token)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    in_window = [e for e in events if event_time(e) >= cutoff]

    log.info(
        "PubNub: fetched %d events, %d within %.0f-hour lookback window",
        len(events),
        len(in_window),
        lookback_hours,
    )
    return sorted(in_window, key=event_time)


async def _fetch_pubnub_history(
    websession: aiohttp.ClientSession,
    serial: str,
    snoo_token: str,
) -> List[SnooData]:
    req_uuid = uuid.uuid1()
    dev_uuid = uuid.uuid1()
    app_dev_id = secrets.token_urlsafe(18)

    url = (
        f"https://{PUBNUB_ORIGIN}/v2/history"
        f"/sub-key/{PUBNUB_SUB_KEY}"
        f"/channel/ActivityState.{serial}"
        f"?pnsdk=PubNub-Kotlin%2F7.4.0"
        f"&auth={snoo_token}"
        f"&requestid={req_uuid}"
        f"&include_token=true"
        f"&count={PUBNUB_HISTORY_COUNT}"
        f"&include_meta=false"
        f"&reverse=false"
        f"&uuid=android_{app_dev_id}_{dev_uuid}"
    )

    async with websession.get(url) as resp:
        resp.raise_for_status()
        raw = await resp.json(content_type=None)

    if not isinstance(raw, list) or not raw:
        log.warning("Unexpected PubNub history response shape")
        return []

    messages_raw = raw[0]
    events: List[SnooData] = []
    for item in messages_raw:
        try:
            msg_dict = item["message"] if isinstance(item, dict) and "message" in item else item
            if isinstance(msg_dict, dict) and "system_state" in msg_dict:
                events.append(SnooData.from_dict(msg_dict))
        except Exception:
            log.debug("Failed to parse PubNub message: %r", item, exc_info=True)

    if len(messages_raw) == PUBNUB_HISTORY_COUNT:
        log.warning(
            "Received full %d-event page from PubNub — history may extend further. "
            "Consider adding timetoken pagination if lookback window is not fully covered.",
            PUBNUB_HISTORY_COUNT,
        )

    return events
