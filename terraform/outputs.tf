output "api_url" {
  description = "Base URL of the webhook endpoint (POST <api_url>/webhook)"
  value       = aws_apigatewayv2_api.api.api_endpoint
}

output "api_key" {
  description = "Send as the x-api-key header. Show with: terraform output -raw api_key"
  value       = random_password.api_key.result
  sensitive   = true
}

output "queue_url" {
  value = aws_sqs_queue.main.url
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.url
}

output "dashboard" {
  value = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${var.project_name}"
}
