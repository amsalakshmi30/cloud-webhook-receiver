"""API Gateway (HTTP API, payload v2) -> Lambda: the public webhook endpoint.

Routes
    POST /webhook             validate -> atomically register event id -> enqueue to SQS
    GET  /events/{event_id}   processing status of one event (for demos / debugging)
    GET  /health              liveness probe (no auth)

Responses to POST /webhook
    202 accepted     new event, queued for processing
    200 duplicate    this event_id was already received - nothing is queued again
    400 rejected     invalid JSON or missing/invalid fields (details in "errors")
    401              missing/wrong x-api-key (only when API_KEY is configured)
    413              body too large
    503              could not queue the event - nothing was stored, sender should retry
"""
import base64
import hmac
import json
import os

from obs import log, metric
from validation import MAX_BODY_BYTES, validate_event

_deps = {"store": None, "queue": None}


def configure(store=None, queue=None):
    """Inject dependencies (used by tests)."""
    _deps["store"] = store
    _deps["queue"] = queue


def _store():
    if _deps["store"] is None:
        from store import DynamoStore

        _deps["store"] = DynamoStore(os.environ["EVENTS_TABLE"], os.environ["EFFECTS_TABLE"])
    return _deps["store"]


def _queue():
    if _deps["queue"] is None:
        from sqs import SqsQueue

        _deps["queue"] = SqsQueue(os.environ["QUEUE_URL"])
    return _deps["queue"]


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }


def _authorized(event):
    expected = os.environ.get("API_KEY", "")
    if not expected:
        return True
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    supplied = headers.get("x-api-key", "")
    return hmac.compare_digest(supplied.encode(), expected.encode())


def handler(event, context=None):
    route = event.get("routeKey", "")

    if route == "GET /health":
        return _response(200, {"status": "ok"})

    if not _authorized(event):
        metric("Unauthorized")
        return _response(401, {"status": "unauthorized", "message": "missing or invalid x-api-key"})

    if route == "POST /webhook":
        return _post_webhook(event)
    if route == "GET /events/{event_id}":
        return _get_event(event)
    return _response(404, {"status": "not_found"})


def _post_webhook(event):
    raw = event.get("body") or ""
    try:
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw).decode("utf-8")
    except Exception:
        metric("Rejected")
        return _response(400, {"status": "rejected", "errors": ["body could not be decoded"]})

    if len(raw.encode("utf-8")) > MAX_BODY_BYTES:
        metric("Rejected")
        return _response(413, {"status": "rejected", "errors": ["body too large"]})

    try:
        payload = json.loads(raw)
    except ValueError:
        metric("Rejected")
        log("WARNING", "rejected: invalid JSON")
        return _response(400, {"status": "rejected", "errors": ["body is not valid JSON"]})

    clean, errors = validate_event(payload)
    if errors:
        metric("Rejected")
        log("WARNING", "rejected: validation failed", errors=errors)
        return _response(400, {"status": "rejected", "errors": errors})

    event_id = clean["event_id"]
    store = _store()

    # Atomic duplicate protection: conditional put (attribute_not_exists) on the event id.
    if not store.register(clean):
        existing = store.get_event(event_id) or {}
        metric("Duplicate")
        log("INFO", "duplicate event ignored", event_id=event_id, current_status=existing.get("status"))
        return _response(
            200,
            {
                "status": "duplicate",
                "event_id": event_id,
                "current_status": existing.get("status"),
                "message": "event_id already received; no new processing was started",
            },
        )

    try:
        _queue().send(json.dumps(clean))
    except Exception as exc:  # queue unavailable: undo the registration so the sender can retry
        store.release(event_id)
        metric("EnqueueError")
        log("ERROR", "failed to enqueue event", event_id=event_id, error=str(exc))
        return _response(
            503, {"status": "unavailable", "event_id": event_id, "message": "could not queue event, retry later"}
        )

    metric("Accepted")
    log("INFO", "event accepted", event_id=event_id, event_type=clean["event_type"])
    return _response(202, {"status": "accepted", "event_id": event_id})


def _get_event(event):
    event_id = (event.get("pathParameters") or {}).get("event_id", "")
    store = _store()
    record = store.get_event(event_id)
    if record is None:
        return _response(404, {"status": "not_found", "event_id": event_id})
    record["side_effect_count"] = store.count_side_effects(event_id)
    return _response(200, record)
