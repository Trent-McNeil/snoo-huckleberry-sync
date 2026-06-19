"""
Build completed sleep intervals from raw SNOO PubNub ActivityState events.

Each ActivityState event is a *state transition*. The state holds from
event_time[i] until event_time[i+1]. The last state holds until... either
the next event (which we may not have yet) or it's the current state.

A SNOO session is a run identified by a common session_id. Within a session:
  - BASELINE / WEANING_BASELINE  → baby is asleep (SNOO at baseline motion)
  - LEVEL1 … LEVEL4             → SNOO is soothing (escalating response)
  - anything else (stop/none/pretimeout/timeout/…) → awake / inactive

We merge consecutive asleep+soothing segments across awake gaps shorter than
MERGE_GAP_MINUTES into one sleep interval. An interval qualifies as sleep iff:
  - asleep_fraction ≥ ASLEEP_RATIO  (e.g. 50 % of interval must be true asleep)
  - asleep_minutes  ≥ MIN_ASLEEP_MINUTES (absolute floor)

We only emit intervals whose session is definitively closed — i.e. we have
observed is_active_session → False for that session_id. An in-progress session
is never emitted, so we never write a partial nap.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

from python_snoo.containers import SnooData, SnooStates

from .snoo_source import event_time

log = logging.getLogger(__name__)

# States that count as the baby being asleep
_ASLEEP_STATES = {SnooStates.baseline, SnooStates.weaning_baseline}
# States that count as the SNOO soothing (motor running but not baseline)
_SOOTHING_STATES = {SnooStates.level1, SnooStates.level2, SnooStates.level3, SnooStates.level4}


def _classify(state: SnooStates) -> str:
    if state in _ASLEEP_STATES:
        return "asleep"
    if state in _SOOTHING_STATES:
        return "soothing"
    return "awake"


@dataclass
class Segment:
    start: datetime
    end: datetime
    kind: str  # "asleep" | "soothing" | "awake"

    @property
    def duration_s(self) -> float:
        return (self.end - self.start).total_seconds()


@dataclass
class SleepInterval:
    session_id: str
    start: datetime
    end: datetime
    asleep_seconds: float
    total_seconds: float

    @property
    def asleep_fraction(self) -> float:
        return self.asleep_seconds / self.total_seconds if self.total_seconds > 0 else 0.0

    def __str__(self) -> str:
        return (
            f"SleepInterval(session={self.session_id!r}, "
            f"start={self.start.isoformat()}, end={self.end.isoformat()}, "
            f"asleep={self.asleep_seconds/60:.1f}min/{self.total_seconds/60:.1f}min "
            f"[{self.asleep_fraction*100:.0f}%])"
        )


def build_sleep_intervals(
    events: List[SnooData],
    *,
    merge_gap_minutes: float,
    min_asleep_minutes: float,
    asleep_ratio: float,
) -> List[SleepInterval]:
    """Convert raw PubNub events into qualified, closed sleep intervals."""
    merge_gap = timedelta(minutes=merge_gap_minutes)
    min_asleep_s = min_asleep_minutes * 60

    # Group by session_id; skip events with no session_id
    sessions: dict[str, List[SnooData]] = {}
    for ev in events:
        sid = getattr(ev.state_machine, "session_id", None)
        if sid:
            sessions.setdefault(sid, []).append(ev)

    results: List[SleepInterval] = []
    for session_id, evs in sessions.items():
        evs_sorted = sorted(evs, key=event_time)

        # Only process sessions we've seen close (is_active_session went False)
        closed = any(not getattr(e.state_machine, "is_active_session", True) for e in evs_sorted)
        if not closed:
            log.debug("Session %s still active — skipping", session_id)
            continue

        segments = _build_segments(evs_sorted)
        interval = _merge_and_classify(
            session_id, segments, merge_gap, min_asleep_s, asleep_ratio
        )
        if interval is not None:
            results.append(interval)

    return sorted(results, key=lambda i: i.start)


def _build_segments(evs: List[SnooData]) -> List[Segment]:
    """Convert event list to segments (state i holds from evs[i].time to evs[i+1].time)."""
    segments: List[Segment] = []
    for i, ev in enumerate(evs):
        start = event_time(ev)
        if i + 1 < len(evs):
            end = event_time(evs[i + 1])
        else:
            # Last event: the state held until... we don't know. For a closed session
            # the final event is the inactive one; assign 0-duration so it doesn't skew totals.
            end = start

        if end <= start:
            continue  # skip zero-duration or out-of-order

        try:
            kind = _classify(ev.state_machine.state)
        except Exception:
            kind = "awake"

        if segments and segments[-1].kind == kind:
            # Extend the previous same-kind segment rather than creating a new one
            segments[-1] = Segment(segments[-1].start, end, kind)
        else:
            segments.append(Segment(start, end, kind))

    return segments


def _merge_and_classify(
    session_id: str,
    segments: List[Segment],
    merge_gap: timedelta,
    min_asleep_s: float,
    asleep_ratio: float,
) -> SleepInterval | None:
    """
    Merge awake gaps shorter than merge_gap into one interval, then classify.
    Returns a SleepInterval if it qualifies, else None.
    """
    if not segments:
        return None

    # Walk segments, collapsing short awake gaps between active (asleep/soothing) stretches
    # into one sleep interval. Each time we find a long awake gap (or end of data), we
    # evaluate the accumulated interval.
    intervals: List[tuple[datetime, datetime, float, float]] = []  # (start, end, asleep_s, total_s)

    ivl_start: datetime | None = None
    ivl_asleep_s = 0.0
    ivl_total_s = 0.0
    last_active_end: datetime | None = None  # end of last asleep/soothing segment

    for seg in segments:
        if seg.kind in ("asleep", "soothing"):
            if ivl_start is None:
                ivl_start = seg.start
            elif last_active_end is not None:
                gap = seg.start - last_active_end
                if gap > merge_gap:
                    # Gap too long — close the current interval and start fresh
                    if ivl_start is not None and last_active_end is not None:
                        intervals.append((ivl_start, last_active_end, ivl_asleep_s, ivl_total_s))
                    ivl_start = seg.start
                    ivl_asleep_s = 0.0
                    ivl_total_s = 0.0
                # else: gap is short enough, bridge it (add gap to total but not asleep)
                else:
                    ivl_total_s += gap.total_seconds()

            if seg.kind == "asleep":
                ivl_asleep_s += seg.duration_s
            ivl_total_s += seg.duration_s
            last_active_end = seg.end

        else:  # awake segment — don't add to totals, just track for gap detection
            pass

    # Close final interval
    if ivl_start is not None and last_active_end is not None:
        intervals.append((ivl_start, last_active_end, ivl_asleep_s, ivl_total_s))

    # Pick the best interval (most asleep time) that meets the thresholds.
    # For most sessions there will be only one merged interval.
    best = None
    for start, end, asleep_s, total_s in intervals:
        frac = asleep_s / total_s if total_s > 0 else 0.0
        if frac >= asleep_ratio and asleep_s >= min_asleep_s:
            if best is None or asleep_s > best.asleep_seconds:
                best = SleepInterval(
                    session_id=session_id,
                    start=start,
                    end=end,
                    asleep_seconds=asleep_s,
                    total_seconds=total_s,
                )
        else:
            log.debug(
                "Session %s interval [%s→%s] rejected: asleep=%.1fmin (%.0f%%) < "
                "threshold (%.1fmin, %.0f%%)",
                session_id,
                start.isoformat(),
                end.isoformat(),
                asleep_s / 60,
                frac * 100,
                min_asleep_s / 60,
                asleep_ratio * 100,
            )

    return best
