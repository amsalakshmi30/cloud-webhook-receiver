"""Observability helpers: structured JSON logs + CloudWatch Embedded Metric Format (EMF).

Anything printed to stdout inside Lambda lands in CloudWatch Logs. Lines shaped like
EMF are additionally turned into CloudWatch *metrics* (namespace WebhookReceiver),
so no extra API calls or permissions are needed.
"""
import json
import time

NAMESPACE = "WebhookReceiver"


def log(level, message, **fields):
    print(json.dumps({"level": level, "message": message, **fields}, default=str), flush=True)


def metric(name, value=1):
    print(
        json.dumps(
            {
                "_aws": {
                    "Timestamp": int(time.time() * 1000),
                    "CloudWatchMetrics": [
                        {
                            "Namespace": NAMESPACE,
                            "Dimensions": [[]],
                            "Metrics": [{"Name": name, "Unit": "Count"}],
                        }
                    ],
                },
                name: value,
            }
        ),
        flush=True,
    )
