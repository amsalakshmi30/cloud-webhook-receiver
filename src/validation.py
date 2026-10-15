"""Input validation for incoming webhook events (pure Python, no AWS dependency)."""
import re

EVENT_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
MAX_BODY_BYTES = 200_000  # keep well below the 256 KB SQS message limit
MAX_SIMULATED_FAILURES = 10


def validate_event(payload):
    """Return (clean_event, errors). clean_event is None when errors is non-empty."""
    if not isinstance(payload, dict):
        return None, ["body must be a JSON object"]

    errors = []

    event_id = payload.get("event_id")
    if not isinstance(event_id, str) or not EVENT_ID_RE.match(event_id):
        errors.append("event_id is required: string of 1-128 chars (letters, digits, . _ : -)")

    event_type = payload.get("event_type")
    if not isinstance(event_type, str) or not event_type.strip() or len(event_type) > 64:
        errors.append("event_type is required: non-empty string, max 64 chars")

    data = payload.get("data")
    if not isinstance(data, dict):
        errors.append("data is required and must be a JSON object")

    timestamp = payload.get("timestamp")
    if timestamp is not None and not isinstance(timestamp, str):
        errors.append("timestamp, when given, must be a string")

    simulate = payload.get("simulate")
    if simulate is not None:
        fail_times = simulate.get("fail_times") if isinstance(simulate, dict) else None
        valid = (
            isinstance(fail_times, int)
            and not isinstance(fail_times, bool)
            and 0 <= fail_times <= MAX_SIMULATED_FAILURES
        )
        if not valid:
            errors.append(
                f"simulate, when given, must be an object like {{\"fail_times\": 0-{MAX_SIMULATED_FAILURES}}}"
            )

    if errors:
        return None, errors

    clean = {"event_id": event_id, "event_type": event_type.strip(), "data": data}
    if timestamp is not None:
        clean["timestamp"] = timestamp
    if simulate is not None:
        clean["simulate"] = {"fail_times": simulate["fail_times"]}
    return clean, []
