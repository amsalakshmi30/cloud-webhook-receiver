"""SQS -> Lambda: processes queued events with bounded retries and idempotency.

Retry model (provider-native):
  * SQS redelivers a message whose Lambda invocation failed, after the visibility timeout.
  * The queue's redrive policy has maxReceiveCount = 3, so a message is delivered at most
    3 times and is then moved to the dead-letter queue (DLQ) by SQS itself.
  * On the 3rd failed attempt this code also records FAILED in DynamoDB so the final
    failure is visible via the status endpoint, not only in the DLQ.

Idempotency: store.claim() is an atomic conditional update; a duplicate copy of an
event can never claim it again, and the side effect + COMPLETED mark are written in one
DynamoDB transaction, so the side effect happens at most once per event_id.
"""
import json
import os

from obs import log, metric

MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))

_deps = {"store": None}


class TransientError(Exception):
    """Stand-in for a temporary downstream failure (timeout, 503, throttling...)."""


def configure(store=None):
    _deps["store"] = store


def _store():
    if _deps["store"] is None:
        from store import DynamoStore

        _deps["store"] = DynamoStore(os.environ["EVENTS_TABLE"], os.environ["EFFECTS_TABLE"])
    return _deps["store"]


def handler(event, context=None):
    """Return SQS partial-batch response: only failed messages are retried."""
    failures = []
    for record in event.get("Records", []):
        try:
            _process_record(record)
        except Exception:
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def _simulate_failure(event, attempt):
    """Controlled transient failure for demos: fail the first N attempts (N = simulate.fail_times)."""
    if os.environ.get("ALLOW_SIMULATION", "false").lower() != "true":
        return
    fail_times = (event.get("simulate") or {}).get("fail_times", 0)
    if attempt <= fail_times:
        raise TransientError(f"simulated transient failure on attempt {attempt}")


def _do_side_effect(event):
    """The business action. Here: persist a result record (stands in for e-mail, payment, etc.)."""
    return {
        "event_type": event["event_type"],
        "summary": f"processed {event['event_type']} event",
        "data": event["data"],
    }


def _process_record(record):
    attempt = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1"))
    try:
        event = json.loads(record["body"])
        event_id = event["event_id"]
    except (ValueError, KeyError, TypeError) as exc:
        metric("PoisonMessage")
        log("ERROR", "unreadable queue message, sending towards DLQ", message_id=record.get("messageId"))
        raise ValueError("unreadable queue message") from exc

    store = _store()

    if not store.claim(event_id, attempt):
        metric("DuplicateSkipped")
        log("INFO", "duplicate delivery skipped", event_id=event_id, attempt=attempt)
        return  # success -> SQS deletes this copy, side effect is NOT repeated

    log("INFO", "processing event", event_id=event_id, attempt=attempt)
    try:
        _simulate_failure(event, attempt)
        effect = _do_side_effect(event)
        if store.complete(event_id, effect):
            metric("Completed")
            log("INFO", "event completed", event_id=event_id, attempt=attempt)
        else:
            log("WARNING", "event was no longer PROCESSING; nothing written", event_id=event_id)
    except Exception as exc:
        if attempt >= MAX_ATTEMPTS:
            store.mark_failed(event_id, exc)
            metric("FailedFinal")
            log("ERROR", "retry limit reached, event FAILED (SQS will move it to the DLQ)",
                event_id=event_id, attempt=attempt, error=str(exc))
        else:
            store.mark_retry(event_id, exc)
            metric("Retried")
            log("WARNING", "attempt failed, will be retried by SQS",
                event_id=event_id, attempt=attempt, error=str(exc))
        raise
