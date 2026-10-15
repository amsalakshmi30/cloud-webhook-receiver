# Cloud Webhook Receiver (AWS)

A cloud-hosted webhook receiver that **validates** events, **retries** transient failures (max 3 attempts),
and **prevents duplicate side effects** with atomic idempotency. Built for the Cloud Computing Hackathon.

```
Sender -> API Gateway -> Lambda receiver -> SQS queue -> Lambda processor -> DynamoDB
                           |  validate         |  retries     |  idempotent claim
                           |  register id      v  (max 3)     v
                           v              SQS dead-letter queue   side-effects table
                      DynamoDB events
CloudWatch (logs, metrics, dashboard, DLQ alarm) | IAM roles | Terraform | GitHub Actions
```

## How each requirement is met

| Requirement | Implementation |
|---|---|
| Cloud HTTP endpoint, validates JSON fields | API Gateway HTTP API + Lambda `receiver` (`src/validation.py`) |
| Bounded retries, max 3 attempts | SQS redrive policy `maxReceiveCount = 3` (provider-native) |
| Persistent event ID, atomic duplicate protection | DynamoDB conditional put `attribute_not_exists(event_id)`; processor claim is a conditional update; side effect + COMPLETED are one transaction |
| Demonstrate 4 scenarios | `scripts/demo.py` (live) and `tests/test_webhook.py` (local) |
| Final failure recorded / dead-letter | Status `FAILED` in DynamoDB **and** message moved to SQS DLQ; CloudWatch alarm on DLQ |

Cloud tools used: API Gateway, Lambda, SQS, DynamoDB, IAM, CloudWatch (+SNS), Terraform, GitHub Actions.

## 1. Run the tests locally (no AWS needed)

```bash
python3 -B -m unittest discover -s tests -v -b
```
13 tests cover success, invalid input, transient failure + retry, duplicates (including 25 concurrent
senders), retry exhaustion + DLQ, queue outage, API-key checks.

## 2. Deploy to AWS

Prerequisites: an AWS account, [AWS CLI](https://aws.amazon.com/cli/), [Terraform](https://developer.hashicorp.com/terraform/install).

```bash
aws configure                      # access key of an IAM user, region ap-south-1
cd terraform
terraform init
terraform apply                    # type "yes"
terraform output api_url
terraform output -raw api_key
```

## 3. Run the live demo

```bash
python3 scripts/demo.py --url "$(terraform -chdir=terraform output -raw api_url)" \
                        --api-key "$(terraform -chdir=terraform output -raw api_key)"
```
It runs all five scenarios (takes about 1-2 minutes because of the retry delay) and prints PASS/FAIL per check.
Take screenshots of this output, the CloudWatch dashboard (`terraform output dashboard`) and the DLQ in the SQS console.

Manual example:
```bash
curl -X POST "$API_URL/webhook" -H "x-api-key: $KEY" -H "content-type: application/json" \
     -d '{"event_id":"evt-1","event_type":"order.created","data":{"amount":499}}'
curl "$API_URL/events/evt-1" -H "x-api-key: $KEY"
```
Controlled failure: add `"simulate": {"fail_times": 2}` to the event (fails twice, succeeds on attempt 3);
`"fail_times": 10` always fails and ends in the DLQ.

## 4. CI/CD (GitHub Actions)

`.github/workflows/ci-cd.yml` runs the tests and `terraform validate` on every push/PR.
To enable automatic deployment from `main`:
1. Create an S3 bucket for Terraform state (once).
2. In the repo settings add secrets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `TF_STATE_BUCKET`
   and the repository variable `ENABLE_DEPLOY = true`.

## 5. Clean up
```bash
cd terraform && terraform destroy
```

## Project layout
```
src/          receiver.py  processor.py  store.py  sqs.py  validation.py  obs.py
tests/        test_webhook.py
terraform/    main.tf  variables.tf  outputs.tf
scripts/      demo.py
docs/         SRS.md
.github/      workflows/ci-cd.yml
```

## Notes
* The `simulate` failure hook is for demos; deploy with `-var allow_simulation=false` for production.
* The side effect is a DynamoDB record standing in for a real action (e-mail, payment). See `docs/SRS.md` section 7.
