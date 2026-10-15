terraform {
  required_version = ">= 1.5"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
    random  = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = { Project = var.project_name, ManagedBy = "terraform" }
  }
}

locals {
  receiver_name  = "${var.project_name}-receiver"
  processor_name = "${var.project_name}-processor"
}

# ---------------------------------------------------------------- secrets
resource "random_password" "api_key" {
  length  = 32
  special = false
}

# ---------------------------------------------------------------- storage (DynamoDB)
# Idempotency + status table. The conditional put on event_id is the atomic duplicate guard.
resource "aws_dynamodb_table" "events" {
  name         = "${var.project_name}-events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_id"

  attribute {
    name = "event_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# Side-effect records: one row per *executed* side effect (proves "exactly once").
resource "aws_dynamodb_table" "effects" {
  name         = "${var.project_name}-side-effects"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_id"
  range_key    = "execution_id"

  attribute {
    name = "event_id"
    type = "S"
  }
  attribute {
    name = "execution_id"
    type = "S"
  }
}

# ---------------------------------------------------------------- queue + dead-letter queue (SQS)
resource "aws_sqs_queue" "dlq" {
  name                      = "${var.project_name}-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "main" {
  name = "${var.project_name}-queue"
  # Must be >= the processor Lambda timeout. It is also the delay between retry attempts.
  visibility_timeout_seconds = var.retry_delay_seconds

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3 # <= three processing attempts, then SQS moves the message to the DLQ
  })
}

# ---------------------------------------------------------------- code package
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/build/lambda.zip"
  excludes    = ["__pycache__"]
}

# ---------------------------------------------------------------- logging (CloudWatch)
resource "aws_cloudwatch_log_group" "receiver" {
  name              = "/aws/lambda/${local.receiver_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "processor" {
  name              = "/aws/lambda/${local.processor_name}"
  retention_in_days = 14
}

# ---------------------------------------------------------------- IAM (least privilege, one role per function)
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "receiver" {
  name               = "${local.receiver_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role" "processor" {
  name               = "${local.processor_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "receiver" {
  statement {
    sid       = "EventsTable"
    actions   = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:DeleteItem"]
    resources = [aws_dynamodb_table.events.arn]
  }
  statement {
    sid       = "ReadSideEffectCount"
    actions   = ["dynamodb:Query"]
    resources = [aws_dynamodb_table.effects.arn]
  }
  statement {
    sid       = "EnqueueOnly"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.main.arn]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.receiver.arn}:*"]
  }
}

data "aws_iam_policy_document" "processor" {
  statement {
    sid       = "UpdateEvents"
    actions   = ["dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.events.arn]
  }
  statement {
    sid       = "WriteSideEffects"
    actions   = ["dynamodb:PutItem"]
    resources = [aws_dynamodb_table.effects.arn]
  }
  statement {
    sid       = "ConsumeQueue"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
    resources = [aws_sqs_queue.main.arn]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.processor.arn}:*"]
  }
}

resource "aws_iam_role_policy" "receiver" {
  name   = "receiver-access"
  role   = aws_iam_role.receiver.id
  policy = data.aws_iam_policy_document.receiver.json
}

resource "aws_iam_role_policy" "processor" {
  name   = "processor-access"
  role   = aws_iam_role.processor.id
  policy = data.aws_iam_policy_document.processor.json
}

# ---------------------------------------------------------------- compute (Lambda)
resource "aws_lambda_function" "receiver" {
  function_name    = local.receiver_name
  role             = aws_iam_role.receiver.arn
  runtime          = "python3.12"
  handler          = "receiver.handler"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 10
  memory_size      = 256

  environment {
    variables = {
      EVENTS_TABLE  = aws_dynamodb_table.events.name
      EFFECTS_TABLE = aws_dynamodb_table.effects.name
      QUEUE_URL     = aws_sqs_queue.main.url
      API_KEY       = random_password.api_key.result
    }
  }

  depends_on = [aws_cloudwatch_log_group.receiver, aws_iam_role_policy.receiver]
}

resource "aws_lambda_function" "processor" {
  function_name    = local.processor_name
  role             = aws_iam_role.processor.arn
  runtime          = "python3.12"
  handler          = "processor.handler"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 10
  memory_size      = 256

  environment {
    variables = {
      EVENTS_TABLE     = aws_dynamodb_table.events.name
      EFFECTS_TABLE    = aws_dynamodb_table.effects.name
      MAX_ATTEMPTS     = "3"
      ALLOW_SIMULATION = tostring(var.allow_simulation)
    }
  }

  depends_on = [aws_cloudwatch_log_group.processor, aws_iam_role_policy.processor]
}

resource "aws_lambda_event_source_mapping" "processor" {
  event_source_arn        = aws_sqs_queue.main.arn
  function_name           = aws_lambda_function.processor.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
}

# ---------------------------------------------------------------- public endpoint (API Gateway HTTP API)
resource "aws_apigatewayv2_api" "api" {
  name          = "${var.project_name}-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "receiver" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.receiver.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "post_webhook" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "POST /webhook"
  target    = "integrations/${aws_apigatewayv2_integration.receiver.id}"
}

resource "aws_apigatewayv2_route" "get_event" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "GET /events/{event_id}"
  target    = "integrations/${aws_apigatewayv2_integration.receiver.id}"
}

resource "aws_apigatewayv2_route" "health" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "GET /health"
  target    = "integrations/${aws_apigatewayv2_integration.receiver.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_burst_limit = 20
    throttling_rate_limit  = 10
  }
}

resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.receiver.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}

# ---------------------------------------------------------------- monitoring (CloudWatch)
resource "aws_sns_topic" "alerts" {
  name = "${var.project_name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.project_name}-dlq-not-empty"
  alarm_description   = "An event exhausted its 3 attempts and is sitting in the dead-letter queue"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = var.project_name
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 12, height = 6
        properties = {
          title  = "Webhook outcomes"
          region = var.aws_region
          view   = "timeSeries"
          stat   = "Sum"
          period = 60
          metrics = [
            ["WebhookReceiver", "Accepted"],
            ["WebhookReceiver", "Rejected"],
            ["WebhookReceiver", "Duplicate"],
            ["WebhookReceiver", "Completed"],
            ["WebhookReceiver", "Retried"],
            ["WebhookReceiver", "FailedFinal"],
          ]
        }
      },
      {
        type = "metric", x = 12, y = 0, width = 12, height = 6
        properties = {
          title  = "Dead-letter queue depth"
          region = var.aws_region
          view   = "timeSeries"
          stat   = "Maximum"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.dlq.name],
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.main.name],
          ]
        }
      },
    ]
  })
}
