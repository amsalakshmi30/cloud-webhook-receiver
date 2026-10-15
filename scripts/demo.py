#!/usr/bin/env python3
"""Live demo against the deployed endpoint (Python standard library only).

    python3 scripts/demo.py --url https://xxxx.execute-api.ap-south-1.amazonaws.com --api-key KEY

Runs the five required scenarios and prints a pass/fail report you can screenshot.
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import uuid

GREEN, RED, DIM, RESET = "\033[32m", "\033[31m", "\033[2m", "\033[0m"
results = []


def call(method, url, api_key, body=None, raw=None):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("content-type", "application/json")
    req.add_header("x-api-key", api_key)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def check(name, condition, detail=""):
    results.append((name, condition))
    mark = f"{GREEN}PASS{RESET}" if condition else f"{RED}FAIL{RESET}"
    print(f"  [{mark}] {name} {DIM}{detail}{RESET}")


def wait_for(base, key, event_id, wanted, timeout=120):
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        code, last = call("GET", f"{base}/events/{event_id}", key)
        if code == 200 and last.get("status") in wanted:
            return last
        time.sleep(3)
    return last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="api_url from `terraform output`")
    ap.add_argument("--api-key", required=True, help="terraform output -raw api_key")
    args = ap.parse_args()
    base, key = args.url.rstrip("/"), args.api_key
    run = uuid.uuid4().hex[:8]

    print("\n== 1. Successful processing ==")
    ok_id = f"demo-ok-{run}"
    code, body = call("POST", f"{base}/webhook", key, {"event_id": ok_id, "event_type": "order.created", "data": {"amount": 499}})
    check("POST valid event -> 202 accepted", code == 202, f"{code} {body}")
    rec = wait_for(base, key, ok_id, {"COMPLETED", "FAILED"})
    check("event reaches COMPLETED on attempt 1", rec.get("status") == "COMPLETED" and rec.get("attempts") == 1, str(rec))
    check("exactly 1 side effect recorded", rec.get("side_effect_count") == 1)

    print("\n== 2. Invalid input ==")
    code, body = call("POST", f"{base}/webhook", key, {"event_type": "order.created"})
    check("missing event_id/data -> 400 rejected", code == 400 and body.get("status") == "rejected", f"{code} {body}")
    code, body = call("POST", f"{base}/webhook", key, raw=b"{ this is not json")
    check("malformed JSON -> 400 rejected", code == 400, f"{code} {body}")
    code, body = call("POST", f"{base}/webhook", "wrong-key", {"event_id": "x", "event_type": "t", "data": {}})
    check("wrong API key -> 401", code == 401, f"{code}")

    print("\n== 3. Transient failure followed by retry (fails twice, 3rd attempt succeeds) ==")
    flaky_id = f"demo-flaky-{run}"
    code, _ = call("POST", f"{base}/webhook", key, {"event_id": flaky_id, "event_type": "order.created", "data": {}, "simulate": {"fail_times": 2}})
    check("POST -> 202 accepted", code == 202)
    print(f"  {DIM}waiting for retries (about 30-40 s)...{RESET}")
    rec = wait_for(base, key, flaky_id, {"COMPLETED", "FAILED"})
    check("COMPLETED after exactly 3 attempts", rec.get("status") == "COMPLETED" and rec.get("attempts") == 3, str(rec))
    check("side effect executed only once despite 3 attempts", rec.get("side_effect_count") == 1)

    print("\n== 4. Repeated submission of the same event_id ==")
    for i in range(3):
        code, body = call("POST", f"{base}/webhook", key, {"event_id": ok_id, "event_type": "order.created", "data": {"amount": 499}})
        check(f"re-send #{i + 1} -> 200 duplicate", code == 200 and body.get("status") == "duplicate", f"{code} {body}")
    code, rec = call("GET", f"{base}/events/{ok_id}", key)
    check("still exactly 1 side effect", rec.get("side_effect_count") == 1, str(rec))

    print("\n== 5. Retry limit reached -> failure recorded + dead-letter queue ==")
    dead_id = f"demo-dead-{run}"
    code, _ = call("POST", f"{base}/webhook", key, {"event_id": dead_id, "event_type": "order.created", "data": {}, "simulate": {"fail_times": 10}})
    check("POST -> 202 accepted", code == 202)
    print(f"  {DIM}waiting for 3 failed attempts (about 30-40 s)...{RESET}")
    rec = wait_for(base, key, dead_id, {"COMPLETED", "FAILED"})
    check("status FAILED after exactly 3 attempts", rec.get("status") == "FAILED" and rec.get("attempts") == 3, str(rec))
    check("no side effect was executed", rec.get("side_effect_count") == 0)
    print(f"  {DIM}final error recorded: {rec.get('last_error')}{RESET}")
    print(f"  {DIM}Also show the message in the DLQ: aws sqs receive-message --queue-url <dlq_url> --region ap-south-1{RESET}")

    passed = sum(1 for _, ok in results if ok)
    print(f"\n{passed}/{len(results)} checks passed")
    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()
