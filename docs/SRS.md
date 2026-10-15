# Software Requirements Specification

**Project:** Cloud-Hosted Webhook Receiver with Validation, Bounded Retries and Idempotency
**Event:** Cloud Computing Hackathon
**Version:** 1.0
**Cloud platform:** Amazon Web Services (region ap-south-1, Mumbai)

---

## 1. Introduction

### 1.1 Purpose
This document specifies the requirements of a cloud-hosted webhook receiver. External systems
(payment gateways, source-control hosts, SaaS tools) deliver JSON events to a public HTTPS endpoint.
The system must validate each event, process it reliably even when a downstream step fails
temporarily, and guarantee that retries or repeated deliveries never repeat a side effect.

### 1.2 Scope
In scope: public HTTPS endpoint, input validation, asynchronous processing, bounded retries,
atomic duplicate protection, dead-letter handling, monitoring/logging, infrastructure as code, CI/CD.
Out of scope: a user interface, business logic of a specific domain (the side effect is a
stand-in that writes a result record), signature schemes of specific webhook providers.

### 1.3 Definitions
| Term | Meaning |
|---|---|
| Webhook | An HTTP POST sent by an external system to notify us that something happened |
| Event ID / idempotency key | A sender-supplied unique string identifying one logical event |
| Idempotency | Processing the same event several times has the same effect as processing it once |
| Transient failure | A temporary error that is expected to succeed if tried again |
| Side effect | An externally visible action (here: a record written to the side-effects table) |
| DLQ | Dead-letter queue: holds messages that exhausted their retries |
| Attempt | One delivery of a queued event to the processor function |

---

## 2. Overall Description

### 2.1 Users
* **Webhook sender** - an external system that POSTs events.
* **Operator / developer** - inspects event status, watches dashboards and alarms, handles DLQ items.

### 2.2 Assumptions and constraints
* Senders supply a unique `event_id` per logical event and re-send the same id when they retry.
* Delivery from the queue is at-least-once (a property of Amazon SQS standard queues).
* Maximum of **three processing attempts** per event (requirement; implemented with the provider-native
  SQS redrive policy `maxReceiveCount = 3`).
* Idempotency records expire after 30 days (DynamoDB TTL); a duplicate arriving later is treated as new.

### 2.3 Architecture

```
                       +------------------+        +-------------------------------+
 Sender --HTTPS POST-->|  API Gateway     |------->|  Lambda "receiver"            |
                       |  (HTTP API)      |        |  validate -> register -> send |
                       +------------------+        +---------+------------+--------+
                                                             |            |
                                       conditional put       |            | SendMessage
                                       attribute_not_exists  v            v
                                              +--------------------+  +-------------+
                                              | DynamoDB: events   |  | SQS queue   |
                                              | (idempotency+state)|  | (retry =    |
                                              +----------^---------+  |  redelivery)|
                                                         |            +------+------+
                                       claim / complete  |                   | trigger (batch=1)
                                                         |            +------v------+
                                              +----------+----------->| Lambda      |
                                              | DynamoDB: side-effects| "processor" |
                                              +-----------------------+------+------+
                                                                             |
                                  after 3rd failed attempt (maxReceiveCount) |
                                                                      +------v------+
                                                                      |  SQS DLQ    |
                                                                      +-------------+
 CloudWatch Logs (structured JSON) + EMF metrics + dashboard + alarm on DLQ depth
 IAM: one least-privilege role per Lambda | Terraform: all of the above | GitHub Actions: test + deploy
```

### 2.4 Cloud services used
| Service | Role in the system |
|---|---|
| API Gateway (HTTP API) | Public HTTPS endpoint, throttling (10 req/s, burst 20) |
| AWS Lambda (Python 3.12) | `receiver` (API) and `processor` (queue worker) |
| Amazon SQS | Decouples receipt from processing; provides retries; DLQ |
| Amazon DynamoDB | Idempotency/state table and side-effect table; conditional writes and transactions |
| IAM | Separate least-privilege execution role per function |
| CloudWatch | Logs, custom metrics (Embedded Metric Format), dashboard, DLQ alarm |
| SNS (optional) | E-mail notification when the alarm fires |
| Terraform | Entire infrastructure as code |
| GitHub Actions | CI (tests, `terraform validate`) and CD (`terraform apply`) |

---

## 3. Functional Requirements

| ID | Requirement |
|---|---|
| **FR-1** | The system shall expose `POST /webhook` over HTTPS and accept a JSON event. |
| **FR-2** | The system shall validate: body is valid JSON and a JSON object; `event_id` is a string of 1-128 characters from `A-Za-z0-9._:-`; `event_type` is a non-empty string of at most 64 characters; `data` is a JSON object; optional `timestamp` is a string. Body size shall not exceed 200 KB. |
| **FR-3** | Invalid requests shall be answered with HTTP 400 (413 if too large) and a list of error messages; they shall not be stored or queued. |
| **FR-4** | A valid new event shall be recorded and queued, and answered with HTTP 202. |
| **FR-5** | The event ID shall be recorded with an atomic conditional write (`attribute_not_exists`). If the ID already exists the system shall answer HTTP 200 `duplicate` and shall not queue the event again. |
| **FR-6** | The processor shall claim an event with an atomic conditional update before doing any work. A claim shall succeed only for events in state RECEIVED, RETRY or PROCESSING **and** only when the delivery's receive count is greater than the stored attempt count, so a second copy of an event can never repeat the work. |
| **FR-7** | If processing fails, the event shall be retried automatically. The maximum number of processing attempts is **three**. |
| **FR-8** | The side effect and the transition to COMPLETED shall be written in one DynamoDB transaction, so the side effect occurs at most once per event ID. |
| **FR-9** | After the third failed attempt the final failure shall be recorded (status FAILED, attempt count, last error) **and** the message shall be moved to the dead-letter queue. |
| **FR-10** | `GET /events/{event_id}` shall return status, attempt count, last error and the number of executed side effects. |
| **FR-11** | `GET /health` shall return HTTP 200 without authentication. |
| **FR-12** | If the queue is unavailable the system shall answer HTTP 503, shall remove the just-created registration, and shall leave the sender free to retry the same event ID. |
| **FR-13** | For demonstration, an event may carry `simulate.fail_times = N (0-10)`; the processor then fails its first N attempts with a transient error. This hook shall be disabled by configuration (`allow_simulation = false`). |

### 3.1 Event states
`RECEIVED -> PROCESSING -> COMPLETED`; on failure `PROCESSING -> RETRY -> PROCESSING ...`; after the third failed attempt `PROCESSING -> FAILED`.

### 3.2 API specification

**POST /webhook** - header `x-api-key: <key>`, `content-type: application/json`

Request:
```json
{ "event_id": "evt-1001", "event_type": "order.created", "data": { "amount": 499 } }
```

| Status | Body (example) | Meaning |
|---|---|---|
| 202 | `{"status":"accepted","event_id":"evt-1001"}` | New event queued |
| 200 | `{"status":"duplicate","event_id":"evt-1001","current_status":"COMPLETED"}` | Already received |
| 400 | `{"status":"rejected","errors":["event_id is required: ..."]}` | Invalid input |
| 401 | `{"status":"unauthorized"}` | Missing/wrong API key |
| 413 | `{"status":"rejected","errors":["body too large"]}` | Body over 200 KB |
| 503 | `{"status":"unavailable"}` | Could not queue; retry |

**GET /events/{event_id}** -> `200` with `status`, `attempts`, `last_error`, `side_effect_count`, timestamps; `404` if unknown.

### 3.3 Data model

Table `events` (partition key `event_id`): `event_type`, `status`, `attempts`, `last_error`, `created_at`, `updated_at`, `completed_at`, `expires_at` (TTL).
Table `side-effects` (partition key `event_id`, sort key `execution_id`): `event_type`, `summary`, `data`, `created_at`.

---

## 4. Non-Functional Requirements

| ID | Category | Requirement |
|---|---|---|
| NFR-1 | Reliability | No accepted event is lost: it is either COMPLETED, or FAILED and present in the DLQ. |
| NFR-2 | Consistency | Duplicate protection relies on DynamoDB conditional writes (atomic), not on read-then-write checks. |
| NFR-3 | Security | One IAM role per function with only the actions it needs; API key required except `/health`; the key is compared in constant time and generated by Terraform; no secrets in source control. |
| NFR-4 | Scalability | Serverless components scale automatically; API throttled to protect downstream resources. |
| NFR-5 | Observability | Structured JSON logs; metrics Accepted, Rejected, Duplicate, Completed, Retried, FailedFinal, DuplicateSkipped, EnqueueError, Unauthorized; dashboard; alarm when the DLQ holds a message. |
| NFR-6 | Maintainability | Whole stack reproducible with `terraform apply`; automated tests run on every push. |
| NFR-7 | Cost | Pay-per-request services only; idle cost is effectively zero. |
| NFR-8 | Latency | Receiver responds after one DynamoDB write and one SQS send (typically well under one second); processing is asynchronous. |

---

## 5. Test Plan

| # | Scenario | Expected result | Automated test (`tests/test_webhook.py`) | Live check (`scripts/demo.py`) |
|---|---|---|---|---|
| 1 | Successful processing | 202, then COMPLETED on attempt 1, 1 side effect | `test_1_successful_processing` | Scenario 1 |
| 2 | Invalid input | 400, nothing stored or queued | `test_2_invalid_input...`, `test_2b_oversized_body` | Scenario 2 |
| 3 | Transient failure then retry | 3 attempts, COMPLETED, 1 side effect | `test_3_transient_failure_then_retry_succeeds` | Scenario 3 |
| 4 | Same event ID repeated | 200 duplicate, still 1 side effect | `test_4_*`, `test_4b_*`, `test_4c_concurrent_*` (25 simultaneous senders: exactly one accepted) | Scenario 4 |
| 5 | Retry limit reached | exactly 3 attempts, FAILED recorded, message in DLQ | `test_5_retry_limit_reached_goes_to_dlq_and_is_recorded` | Scenario 5 |
| 6 | Queue outage | 503, registration rolled back, clean retry works | `test_enqueue_failure_returns_503...` | - |
| 7 | Access control | 401 without key, `/health` open | `test_api_key_is_enforced...` | Scenario 2 |

---

## 6. Deployment and Operations

1. `terraform apply` creates all resources and prints `api_url`; `terraform output -raw api_key` shows the key.
2. GitHub Actions workflow `ci-cd.yml` runs the tests and `terraform validate` on every push and pull request; on `main` it can run `terraform apply` (enabled by repository variable `ENABLE_DEPLOY=true`, with state in S3).
3. Operations: watch the CloudWatch dashboard; when the DLQ alarm fires, inspect the DLQ message and the event record, fix the cause, and re-submit the event with a new `event_id` (or delete the old record).
4. `terraform destroy` removes everything.

---

## 7. Limitations and Future Work

* The side effect here is a DynamoDB record. A real external side effect (e-mail, payment) cannot join a DynamoDB transaction; it should be called with the `event_id` as the downstream idempotency key.
* Sender authentication uses a shared API key; provider-specific HMAC signature verification is a natural extension.
* Re-driving DLQ messages is a manual step (SQS console "start DLQ redrive" can be used).
* The demo hook `simulate.fail_times` must be disabled in production.
