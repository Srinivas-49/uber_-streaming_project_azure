"""Deterministic replay simulator for Uber-shaped synthetic events.

This is a contract/CDC validation harness, NOT an Azure Event Hubs consumer.
All samples intentionally omit passenger/driver PII and precise coordinates.
"""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

STATES = frozenset({"requested", "accepted", "completed", "cancelled"})

def normalize(event):
    """Validate typed CDC contract; never retain unsafe raw rejected payload."""
    required = ("event_id", "ride_id", "event_time", "status", "fare_gbp")
    if any(not isinstance(event.get(k), str) or not event[k].strip() for k in required):
        raise ValueError("missing required event field")
    try:
        stamp = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid event_time") from exc
    if stamp.tzinfo is None:
        raise ValueError("event_time must include a timezone")
    if event["status"] not in STATES:
        raise ValueError("invalid ride status")
    try:
        fare = Decimal(event["fare_gbp"])
        if not fare.is_finite() or fare < 0 or fare.as_tuple().exponent < -2:
            raise ValueError("invalid ride fare")
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("invalid ride fare") from exc
    return {"event_id": event["event_id"], "ride_id": event["ride_id"],
            "event_time": stamp.astimezone(timezone.utc),
            "status": event["status"], "fare_gbp": fare}

def replay(events):
    """Latest-event-time ride state with deterministic equal-timestamp tie breaker.

    Uses O(number of distinct rides + event IDs) memory for small local fixtures.
    Event ID replay is idempotent within this run; true Kafka exactly-once delivery
    requires durable checkpoints and transactional sinks beyond this simulator.
    """
    seen, latest, quarantine = set(), {}, []
    metrics = {"received": 0, "accepted_events": 0, "duplicate_events": 0,
               "invalid_events": 0, "stale_events": 0, "updated_rides": 0}
    for index, raw in enumerate(events):
        metrics["received"] += 1
        try:
            event = normalize(raw)
        except (ValueError, TypeError) as exc:
            metrics["invalid_events"] += 1
            quarantine.append({"position": index, "reason": str(exc)})
            continue
        if event["event_id"] in seen:
            metrics["duplicate_events"] += 1
            continue
        seen.add(event["event_id"])
        metrics["accepted_events"] += 1
        key = event["ride_id"]
        previous = latest.get(key)
        if previous:
            if (event["event_time"], event["event_id"]) <= (previous["event_time"], previous["event_id"]):
                metrics["stale_events"] += 1
                continue
            metrics["updated_rides"] += 1
        latest[key] = event
    metrics["current_rides"] = len(latest)
    metrics["completed_rides"] = sum(v["status"] == "completed" for v in latest.values())
    metrics["completed_revenue_gbp"] = str(sum(
        (v["fare_gbp"] for v in latest.values() if v["status"] == "completed"),
        Decimal("0")))
    public_state = {
        key: {"event_id": value["event_id"], "event_time": value["event_time"].isoformat(),
              "status": value["status"], "fare_gbp": str(value["fare_gbp"])}
        for key, value in sorted(latest.items())
    }
    return {"metrics": metrics, "rides": public_state, "quarantine": quarantine}

def fixture(n=30):
    if not 1 <= n <= 100000:
        raise ValueError("synthetic ride count must be between 1 and 100000")
    events = [
        {"event_id": f"start-{i}", "ride_id": f"ride-{i}", "event_time": "2026-11-14T09:00:00Z",
         "status": "requested", "fare_gbp": f"{i % 30 + 5}.50"}
        for i in range(n)
    ]
    events.append(dict(events[0]))  # duplicate delivery
    events.extend([
        {"event_id": f"finish-{i}", "ride_id": f"ride-{i}", "event_time": "2026-11-14T09:30:00Z",
         "status": "completed", "fare_gbp": f"{i % 30 + 5}.50"}
        for i in range(0, n, 5)
    ])
    events.append({"event_id": "late", "ride_id": "ride-0",
                   "event_time": "2026-11-14T08:00:00Z", "status": "cancelled",
                   "fare_gbp": "0.00"})
    events.append({"event_id": "bad", "ride_id": "ride-0", "event_time": "not-a-time",
                   "status": "requested", "fare_gbp": "9.99"})
    return events
