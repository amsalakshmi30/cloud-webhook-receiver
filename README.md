# Cloud Webhook Receiver

A simple cloud system on AWS that receives events, checks them, retries failures (max 3 times), ignores duplicates, and saves failed events safely.

Built for the Cloud Computing Hackathon. Region: `ap-south-1` (Mumbai).

## What it does

- Accepts a JSON event at an HTTPS endpoint
- Rejects bad input (HTTP 400)
- Ignores duplicate event IDs (no repeated work)
- Retries failed events, maximum 3 attempts
- Moves events that still fail to a dead-letter queue (DLQ)
- Shows everything on a live dashboard

Easy example: a chai kadai. The counter checks the order slip, writes each order number in a notebook, the chai master tries up to 3 times, and failed orders go into a problem box.

## How it works

```
Sender -> API Gateway -> Lambda (ingest) -> DynamoDB (event ID check)
                                |
                                v
                           SQS queue -> Lambda (worker) -> DONE
                                |
                         after 3 failures
                                v
                            SQS DLQ -> CloudWatch alarm
```

1. **Ingest Lambda** checks the JSON. Missing fields give `400`.
2. It saves the `event_id` in DynamoDB. If the ID already exists, it replies `duplicate` and stops.
3. New events go to the SQS queue and the sender gets `202 accepted`.
4. **Worker Lambda** processes the event. If it fails, SQS retries it.
5. After 3 failed attempts, the event is marked `FAILED` and moves to the DLQ. A CloudWatch alarm rings.

## AWS services

| Service | Used for |
|---|---|
| API Gateway | Public HTTPS endpoint |
| Lambda | Ingest, worker and dashboard code |
| DynamoDB | Stores event ID and status (stops duplicates) |
| SQS + DLQ | Queue, retries and failed events |
| CloudWatch | Logs and alarm |
| S3 | Hosts the dashboard |
| IAM | Permissions |

## Why duplicates and retries are safe

- **Duplicates:** DynamoDB saves the event ID with the condition `attribute_not_exists(event_id)`. Only one request can win, even if two arrive together.
- **Retries:** The SQS queue is set to `maxReceiveCount = 3`, so no event is tried more than 3 times.
- **No repeated work:** The worker marks an event `DONE` only if it is not already `DONE`.

## API

**POST /webhook**

```json
{
  "event_id": "evt-001",
  "type": "order.created",
  "payload": { "item": "tea", "qty": 2 }
}
```

| Case | Response |
|---|---|
| Valid new event | `202 {"status":"accepted"}` |
| Same event ID again | `200 {"status":"duplicate"}` |
| Missing or wrong fields | `400` with an error message |

Optional field `simulate_failure`: `"transient"` (fails twice, then works) or `"permanent"` (always fails).

**GET /events** returns counts, queue size, DLQ size and the latest events.

## Demo (PowerShell)

```powershell
$API_ID = aws apigatewayv2 get-apis --query "Items[?Name=='webhook-api'].ApiId" --output text
$API_ENDPOINT = "https://$API_ID.execute-api.ap-south-1.amazonaws.com"

curl.exe -s -X POST "$API_ENDPOINT/webhook" -H "Content-Type: application/json" -d "@valid.json" -w '\nHTTP %{http_code}\n'
```

| Test | File | Result |
|---|---|---|
| Success | `valid.json` | `202`, then `DONE` |
| Invalid input | `invalid.json` | `400` |
| Duplicate | send `valid.json` again | `200 duplicate` |
| Retry | `transient.json` | Fails twice, works on try 3 |
| Final failure | `permanent.json` | 3 fails, `FAILED`, goes to DLQ |

Retry tests take about 2 to 3 minutes (60 seconds between tries).

## Test results

All tests passed: success, invalid input, duplicate (attempts stayed 1), transient retry (DONE on attempt 3), permanent failure (FAILED, 1 message in DLQ).

## Dashboard

A React app hosted on S3 that shows live status of all events and has a "Try it out" button to send test events.

```
http://webhook-dashboard-<ACCOUNT_ID>.s3-website.ap-south-1.amazonaws.com
```

## Files

```
ingest.py       Ingest Lambda
worker.py       Worker Lambda
dashboard.py    Dashboard API Lambda
*.json          Test payloads
bolt-ui2/       Dashboard source (React)
```

## Security

Never upload AWS access keys to GitHub. The demo endpoint is public. A real system should add request signature checking.

## Future work

Terraform, GitHub Actions CI/CD, HTTPS dashboard (CloudFront), exponential backoff, request signature check.

live deploy link:

http://webhook-dashboard-811720213795.s3-website.ap-south-1.amazonaws.com
