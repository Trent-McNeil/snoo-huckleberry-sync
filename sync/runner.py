"""
One sync pass: SNOO → session builder → dedupe → Huckleberry (or dry-run log).

Run once:
    python -m sync.runner

Or in a loop (Docker entrypoint):
    python -m sync.runner --loop
"""

import argparse
import asyncio
import logging
import sys

import aiohttp

from . import config
from .dedupe import DedupeStore
from .huckleberry_sink import make_huckleberry_client, resolve_child_uid, write_sleep_interval
from .session_builder import build_sleep_intervals
from .snoo_source import fetch_events

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("sync.runner")


async def run_once() -> None:
    dry = config.DRY_RUN
    log.info(
        "Starting sync pass (DRY_RUN=%s, lookback=%.0fh, merge_gap=%.0fmin)",
        dry,
        config.LOOKBACK_HOURS,
        config.MERGE_GAP_MINUTES,
    )

    async with aiohttp.ClientSession() as session:
        # ---- Fetch SNOO events ----
        events = await fetch_events(
            session,
            config.SNOO_USERNAME,
            config.SNOO_PASSWORD,
            config.LOOKBACK_HOURS,
        )

        if not events:
            log.info("No SNOO events in lookback window — nothing to do.")
            return

        # ---- Build sleep intervals ----
        intervals = build_sleep_intervals(
            events,
            merge_gap_minutes=config.MERGE_GAP_MINUTES,
            min_asleep_minutes=config.MIN_ASLEEP_MINUTES,
            asleep_ratio=config.ASLEEP_RATIO,
        )

        if not intervals:
            log.info("No qualifying sleep intervals found in SNOO events.")
            return

        log.info("Found %d qualifying sleep interval(s).", len(intervals))

        # ---- Dry-run: log and stop ----
        if dry:
            log.info("DRY_RUN=true — logging intended writes, nothing will be written.")
            for ivl in intervals:
                log.info(
                    "  WOULD WRITE: %s → %s  (asleep %.1f min, %.0f%% of %.1f min total)",
                    ivl.start.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    ivl.end.strftime("%H:%M:%S UTC"),
                    ivl.asleep_seconds / 60,
                    ivl.asleep_fraction * 100,
                    ivl.total_seconds / 60,
                )
            log.info(
                "Set DRY_RUN=false in .env when the above intervals look correct."
            )
            return

        # ---- Real mode: dedupe + write ----
        store = DedupeStore(config.DB_PATH)
        hb = await make_huckleberry_client(
            session,
            config.HUCKLEBERRY_EMAIL,
            config.HUCKLEBERRY_PASSWORD,
            config.HUCKLEBERRY_TIMEZONE,
        )
        child_uid = await resolve_child_uid(hb, config.HUCKLEBERRY_CHILD_UID)

        written = 0
        skipped = 0
        for ivl in intervals:
            if store.seen(ivl.session_id):
                log.debug("Session %s already written — skipping.", ivl.session_id)
                skipped += 1
                continue
            await write_sleep_interval(hb, child_uid, ivl)
            store.mark(ivl.session_id, ivl.start, ivl.end)
            written += 1

        await hb.stop_all_listeners()
        store.close()
        log.info("Pass complete: %d written, %d skipped (already in dedupe store).", written, skipped)


async def run_loop() -> None:
    interval_s = config.INTERVAL_MINUTES * 60
    log.info("Starting sync loop (interval=%.0f min).", config.INTERVAL_MINUTES)
    while True:
        try:
            await run_once()
        except Exception as exc:
            log.error("Sync pass failed: %s", exc, exc_info=True)
        log.info("Sleeping %.0f seconds until next pass.", interval_s)
        await asyncio.sleep(interval_s)


def main() -> None:
    parser = argparse.ArgumentParser(description="SNOO → Huckleberry sync")
    parser.add_argument("--loop", action="store_true", help="Run continuously on INTERVAL_MINUTES schedule")
    args = parser.parse_args()

    try:
        if args.loop:
            asyncio.run(run_loop())
        else:
            asyncio.run(run_once())
    except KeyboardInterrupt:
        log.info("Interrupted.")
        sys.exit(0)


if __name__ == "__main__":
    main()
