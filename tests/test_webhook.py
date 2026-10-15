"""Local tests (stdlib unittest, no AWS needed).

Run from the project root:   python3 -B -m unittest discover -s tests -v -b
"""
import json
import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import processor  # noqa: E402
import receiver  # noqa: E402
from store import COMPLETED, FAILED, InMemoryStore  # noqa: E402


class FakeQueue:
    """Mimics SQS standard queue + redrive policy (maxReceiveCount=3) + the Lambda poller."""

    def __init__(self, max_receive=3):
        self.max_receive = max_receive
        self.messages = []  # each: {"id", "body", "received"}
        self.dlq = []
        self.fail_next_send = False
        self._n = 0

    def send(self, body):
        if self.fail_next_send:
            self.fail_next_send = False
            raise RuntimeError("SQS unavailable")
        self._n += 1
        self.messages.append({"id": f"m{self._n}", "body": body, "received": 0})

    def drain(self):
        """Deliver messages to the processor until the queue is empty. Returns #deliveries."""
        deliveries = 0
        while self.messages:
            msg = self.messages.pop(0)
            if msg["received"] >= self.max_receive:
                self.dlq.append(msg)  # SQS moves it to the DLQ instead of delivering a 4th time
                continue
            msg["received"] += 1
            deliveries += 1
            record = {
                "messageId": msg["id"],
                "body": msg["body"],
                "attributes": {"ApproximateReceiveCount": str(msg["received"])},
            }
            result = processor.handler({"Records": [record]})
            if result["batchItemFailures"]:
                self.messages.append(msg)  # visible again after the visibility timeout
        return deliveries


def api_event(route, body=None, headers=None, path_params=None):
    if body is not None and not isinstance(body, str):
        body = json.dumps(body)
    return {"routeKey": route, "headers": headers or {}, "body": body, "pathParameters": path_params}


def valid_event(event_id="evt-1", **extra):
    return {"event_id": event_id, "event_type": "order.created", "data": {"amount": 499}, **extra}


class WebhookTests(unittest.TestCase):
    def setUp(self):
        os.environ["ALLOW_SIMULATION"] = "true"
        os.environ.pop("API_KEY", None)
        self.store = InMemoryStore()
        self.queue = FakeQueue()
        receiver.configure(store=self.store, queue=self.queue)
        processor.configure(store=self.store)

    # helpers
    def post(self, body, **kw):
        resp = receiver.handler(api_event("POST /webhook", body, **kw))
        return resp["statusCode"], json.loads(resp["body"])

    def status(self, event_id):
        resp = receiver.handler(api_event("GET /events/{event_id}", path_params={"event_id": event_id}))
        return resp["statusCode"], json.loads(resp["body"])

    # ---- Scenario 1: successful processing -------------------------------------------------
    def test_1_successful_processing(self):
        code, body = self.post(valid_event("ok-1"))
        self.assertEqual((code, body["status"]), (202, "accepted"))
        self.assertEqual(self.queue.drain(), 1)
        code, rec = self.status("ok-1")
        self.assertEqual(code, 200)
        self.assertEqual(rec["status"], COMPLETED)
        self.assertEqual(rec["attempts"], 1)
        self.assertEqual(rec["side_effect_count"], 1)

    # ---- Scenario 2: invalid input ---------------------------------------------------------
    def test_2_invalid_input_is_rejected_and_never_queued(self):
        bad_bodies = [
            {},  # everything missing
            {"event_type": "x", "data": {}},  # no event_id
            {"event_id": "a b", "event_type": "x", "data": {}},  # illegal characters
            {"event_id": "e1", "event_type": "", "data": {}},  # empty type
            {"event_id": "e1", "event_type": "x", "data": "nope"},  # data not an object
            {"event_id": 123, "event_type": "x", "data": {}},  # id not a string
            {"event_id": "e1", "event_type": "x", "data": {}, "simulate": {"fail_times": 99}},
            [1, 2, 3],  # not an object
            "not json at all {",
        ]
        for body in bad_bodies:
            with self.subTest(body=body):
                code, resp = self.post(body)
                self.assertEqual(code, 400)
                self.assertEqual(resp["status"], "rejected")
                self.assertTrue(resp["errors"])
        self.assertEqual(self.queue.messages, [])
        self.assertEqual(self.store.events, {})

    def test_2b_oversized_body(self):
        code, _ = self.post(valid_event("big", data_blob="x" * 300_000))
        self.assertEqual(code, 413)

    # ---- Scenario 3: transient failure followed by retry -----------------------------------
    def test_3_transient_failure_then_retry_succeeds(self):
        self.post(valid_event("flaky-1", simulate={"fail_times": 2}))
        deliveries = self.queue.drain()
        self.assertEqual(deliveries, 3)  # attempts 1 and 2 fail, attempt 3 succeeds
        _, rec = self.status("flaky-1")
        self.assertEqual(rec["status"], COMPLETED)
        self.assertEqual(rec["attempts"], 3)
        self.assertEqual(rec["side_effect_count"], 1)  # side effect ran once, not once per attempt
        self.assertIn("simulated transient failure", rec["last_error"])
        self.assertEqual(self.queue.dlq, [])

    # ---- Scenario 4: repeated submission of the same event id ------------------------------
    def test_4_duplicate_submission_has_no_extra_side_effects(self):
        self.assertEqual(self.post(valid_event("dup-1"))[0], 202)
        self.queue.drain()
        for _ in range(3):
            code, body = self.post(valid_event("dup-1"))
            self.assertEqual(code, 200)
            self.assertEqual(body["status"], "duplicate")
            self.assertEqual(body["current_status"], COMPLETED)
        self.assertEqual(self.queue.drain(), 0)  # nothing was queued again
        self.assertEqual(self.status("dup-1")[1]["side_effect_count"], 1)

    def test_4b_duplicate_message_inside_the_queue_is_skipped(self):
        """Even if two copies of one event reach SQS (at-least-once delivery), work runs once."""
        self.post(valid_event("dup-2"))
        self.queue.send(self.queue.messages[0]["body"])  # same event queued a second time
        self.queue.drain()
        _, rec = self.status("dup-2")
        self.assertEqual(rec["status"], COMPLETED)
        self.assertEqual(rec["side_effect_count"], 1)

    def test_4c_concurrent_submissions_register_exactly_once(self):
        results = []
        barrier = threading.Barrier(25)

        def worker():
            barrier.wait()
            results.append(self.post(valid_event("race-1"))[0])

        threads = [threading.Thread(target=worker) for _ in range(25)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(sorted(results), [200] * 24 + [202])
        self.assertEqual(len(self.queue.messages), 1)
        self.queue.drain()
        self.assertEqual(self.status("race-1")[1]["side_effect_count"], 1)

    # ---- Scenario 5: retry limit reached -> final failure recorded + DLQ -------------------
    def test_5_retry_limit_reached_goes_to_dlq_and_is_recorded(self):
        self.post(valid_event("doomed-1", simulate={"fail_times": 10}))
        deliveries = self.queue.drain()
        self.assertEqual(deliveries, 3)  # exactly three processing attempts, no more
        _, rec = self.status("doomed-1")
        self.assertEqual(rec["status"], FAILED)
        self.assertEqual(rec["attempts"], 3)
        self.assertEqual(rec["side_effect_count"], 0)
        self.assertEqual(len(self.queue.dlq), 1)
        self.assertEqual(json.loads(self.queue.dlq[0]["body"])["event_id"], "doomed-1")
        # a later re-submission of the failed event id does not silently re-run it
        self.assertEqual(self.post(valid_event("doomed-1"))[1]["status"], "duplicate")

    # ---- extras ----------------------------------------------------------------------------
    def test_enqueue_failure_returns_503_and_allows_a_clean_retry(self):
        self.queue.fail_next_send = True
        code, body = self.post(valid_event("q-down"))
        self.assertEqual((code, body["status"]), (503, "unavailable"))
        self.assertNotIn("q-down", self.store.events)  # registration was rolled back
        self.assertEqual(self.post(valid_event("q-down"))[0], 202)
        self.queue.drain()
        self.assertEqual(self.status("q-down")[1]["status"], COMPLETED)

    def test_poison_message_is_retried_then_dead_lettered(self):
        self.queue.send("{{ not json")
        self.assertEqual(self.queue.drain(), 3)
        self.assertEqual(len(self.queue.dlq), 1)

    def test_simulation_is_ignored_when_disabled(self):
        os.environ["ALLOW_SIMULATION"] = "false"
        self.post(valid_event("prod-1", simulate={"fail_times": 2}))
        self.assertEqual(self.queue.drain(), 1)
        self.assertEqual(self.status("prod-1")[1]["status"], COMPLETED)

    def test_api_key_is_enforced_when_configured(self):
        os.environ["API_KEY"] = "s3cret"
        try:
            self.assertEqual(self.post(valid_event("k-1"))[0], 401)
            self.assertEqual(self.post(valid_event("k-1"), headers={"x-api-key": "wrong"})[0], 401)
            self.assertEqual(self.post(valid_event("k-1"), headers={"X-Api-Key": "s3cret"})[0], 202)
            health = receiver.handler(api_event("GET /health"))
            self.assertEqual(health["statusCode"], 200)  # health needs no key
        finally:
            os.environ.pop("API_KEY", None)

    def test_unknown_event_status_is_404(self):
        self.assertEqual(self.status("nope")[0], 404)


if __name__ == "__main__":
    unittest.main()
