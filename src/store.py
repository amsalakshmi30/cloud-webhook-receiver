"""Persistence layer.

Two interchangeable implementations of the same interface:

* DynamoStore  - production, DynamoDB conditional writes give the atomic guarantees.
* InMemoryStore - used by the local unit tests (a lock stands in for DynamoDB atomicity).

Event lifecycle (status field of the events table):

    RECEIVED --claim--> PROCESSING --complete--> COMPLETED
                            |
                            +--mark_retry--> RETRY --claim--> PROCESSING ...
                            |
                            +--mark_failed (after the 3rd attempt)--> FAILED

`attempts` holds the SQS receive count of the delivery that last claimed the event.
A claim only succeeds when the incoming receive count is HIGHER than the stored
attempts, so a second copy of the same event (receive count 1 again) can never
re-run the work, while a genuine SQS redelivery (count 2, 3) can.
"""
import json
import threading
import time
import uuid
from datetime import datetime, timezone

RECEIVED = "RECEIVED"
PROCESSING = "PROCESSING"
RETRY = "RETRY"
COMPLETED = "COMPLETED"
FAILED = "FAILED"
CLAIMABLE = (RECEIVED, RETRY, PROCESSING)

RECORD_TTL_SECONDS = 30 * 24 * 3600  # idempotency records expire after 30 days


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class InMemoryStore:
    def __init__(self):
        self._lock = threading.Lock()
        self.events = {}
        self.side_effects = {}

    def register(self, event):
        """Atomically record a new event id. False if it already exists."""
        with self._lock:
            if event["event_id"] in self.events:
                return False
            ts = now_iso()
            self.events[event["event_id"]] = {
                "event_id": event["event_id"],
                "event_type": event["event_type"],
                "status": RECEIVED,
                "attempts": 0,
                "created_at": ts,
                "updated_at": ts,
            }
            return True

    def release(self, event_id):
        """Undo register() when the event could not be queued. Only removes RECEIVED records."""
        with self._lock:
            rec = self.events.get(event_id)
            if rec and rec["status"] == RECEIVED:
                del self.events[event_id]
                return True
            return False

    def claim(self, event_id, receive_count):
        with self._lock:
            rec = self.events.get(event_id)
            if not rec or rec["status"] not in CLAIMABLE or rec["attempts"] >= receive_count:
                return False
            rec.update(status=PROCESSING, attempts=receive_count, updated_at=now_iso())
            return True

    def complete(self, event_id, effect):
        """Atomically write the side effect AND mark the event COMPLETED."""
        with self._lock:
            rec = self.events.get(event_id)
            if not rec or rec["status"] != PROCESSING:
                return False
            self.side_effects.setdefault(event_id, []).append(
                {"execution_id": str(uuid.uuid4()), **effect}
            )
            rec.update(status=COMPLETED, updated_at=now_iso(), completed_at=now_iso())
            return True

    def mark_retry(self, event_id, error):
        return self._transition(event_id, RETRY, error)

    def mark_failed(self, event_id, error):
        return self._transition(event_id, FAILED, error)

    def _transition(self, event_id, status, error):
        with self._lock:
            rec = self.events.get(event_id)
            if not rec or rec["status"] != PROCESSING:
                return False
            rec.update(status=status, last_error=str(error)[:500], updated_at=now_iso())
            return True

    def get_event(self, event_id):
        with self._lock:
            rec = self.events.get(event_id)
            return dict(rec) if rec else None

    def count_side_effects(self, event_id):
        with self._lock:
            return len(self.side_effects.get(event_id, []))


class DynamoStore:
    """DynamoDB implementation using the low-level client (always available in Lambda)."""

    def __init__(self, events_table, effects_table, client=None):
        if client is None:
            import boto3  # imported lazily so the unit tests need no AWS SDK

            client = boto3.client("dynamodb")
        self.client = client
        self.events_table = events_table
        self.effects_table = effects_table

    # ---- helpers -------------------------------------------------------------
    @staticmethod
    def _error_code(exc):
        return getattr(exc, "response", {}).get("Error", {}).get("Code")

    @classmethod
    def _is_condition_failure(cls, exc):
        return cls._error_code(exc) == "ConditionalCheckFailedException"

    @staticmethod
    def _s(value):
        return {"S": str(value)}

    @staticmethod
    def _n(value):
        return {"N": str(value)}

    # ---- interface -----------------------------------------------------------
    def register(self, event):
        ts = now_iso()
        item = {
            "event_id": self._s(event["event_id"]),
            "event_type": self._s(event["event_type"]),
            "status": self._s(RECEIVED),
            "attempts": self._n(0),
            "created_at": self._s(ts),
            "updated_at": self._s(ts),
            "expires_at": self._n(int(time.time()) + RECORD_TTL_SECONDS),
        }
        try:
            self.client.put_item(
                TableName=self.events_table,
                Item=item,
                ConditionExpression="attribute_not_exists(event_id)",
            )
            return True
        except Exception as exc:
            if self._is_condition_failure(exc):
                return False
            raise

    def release(self, event_id):
        try:
            self.client.delete_item(
                TableName=self.events_table,
                Key={"event_id": self._s(event_id)},
                ConditionExpression="#s = :received",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":received": self._s(RECEIVED)},
            )
            return True
        except Exception as exc:
            if self._is_condition_failure(exc):
                return False
            raise

    def claim(self, event_id, receive_count):
        try:
            self.client.update_item(
                TableName=self.events_table,
                Key={"event_id": self._s(event_id)},
                UpdateExpression="SET #s = :processing, attempts = :rc, updated_at = :now",
                ConditionExpression=(
                    "attribute_exists(event_id) AND #s IN (:received, :retry, :processing) "
                    "AND attempts < :rc"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":processing": self._s(PROCESSING),
                    ":received": self._s(RECEIVED),
                    ":retry": self._s(RETRY),
                    ":rc": self._n(receive_count),
                    ":now": self._s(now_iso()),
                },
            )
            return True
        except Exception as exc:
            if self._is_condition_failure(exc):
                return False
            raise

    def complete(self, event_id, effect):
        ts = now_iso()
        effect_item = {
            "event_id": self._s(event_id),
            "execution_id": self._s(uuid.uuid4()),
            "created_at": self._s(ts),
        }
        for key, value in effect.items():
            effect_item[key] = self._s(value if isinstance(value, str) else json.dumps(value))
        try:
            self.client.transact_write_items(
                TransactItems=[
                    {"Put": {"TableName": self.effects_table, "Item": effect_item}},
                    {
                        "Update": {
                            "TableName": self.events_table,
                            "Key": {"event_id": self._s(event_id)},
                            "UpdateExpression": "SET #s = :completed, updated_at = :now, completed_at = :now",
                            "ConditionExpression": "#s = :processing",
                            "ExpressionAttributeNames": {"#s": "status"},
                            "ExpressionAttributeValues": {
                                ":completed": self._s(COMPLETED),
                                ":processing": self._s(PROCESSING),
                                ":now": self._s(ts),
                            },
                        }
                    },
                ]
            )
            return True
        except Exception as exc:
            if self._error_code(exc) == "TransactionCanceledException":
                reasons = getattr(exc, "response", {}).get("CancellationReasons", [])
                if any(r.get("Code") == "ConditionalCheckFailed" for r in reasons):
                    return False
            raise

    def mark_retry(self, event_id, error):
        return self._transition(event_id, RETRY, error)

    def mark_failed(self, event_id, error):
        return self._transition(event_id, FAILED, error)

    def _transition(self, event_id, status, error):
        try:
            self.client.update_item(
                TableName=self.events_table,
                Key={"event_id": self._s(event_id)},
                UpdateExpression="SET #s = :new, last_error = :err, updated_at = :now",
                ConditionExpression="#s = :processing",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":new": self._s(status),
                    ":err": self._s(str(error)[:500]),
                    ":processing": self._s(PROCESSING),
                    ":now": self._s(now_iso()),
                },
            )
            return True
        except Exception as exc:
            if self._is_condition_failure(exc):
                return False
            raise

    def get_event(self, event_id):
        resp = self.client.get_item(
            TableName=self.events_table,
            Key={"event_id": self._s(event_id)},
            ConsistentRead=True,
        )
        item = resp.get("Item")
        if not item:
            return None
        return {
            key: (int(val["N"]) if "N" in val else val.get("S"))
            for key, val in item.items()
            if key != "expires_at"
        }

    def count_side_effects(self, event_id):
        resp = self.client.query(
            TableName=self.effects_table,
            KeyConditionExpression="event_id = :e",
            ExpressionAttributeValues={":e": self._s(event_id)},
            Select="COUNT",
            ConsistentRead=True,
        )
        return resp["Count"]
