variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-south-1" # Mumbai
}

variable "project_name" {
  description = "Prefix for all resource names"
  type        = string
  default     = "webhook-receiver"
}

variable "retry_delay_seconds" {
  description = "SQS visibility timeout = delay between retry attempts (must be >= Lambda timeout of 10s)"
  type        = number
  default     = 15
}

variable "allow_simulation" {
  description = "Enable the controlled-failure demo hook (simulate.fail_times). Set false for production."
  type        = bool
  default     = true
}

variable "alarm_email" {
  description = "Optional e-mail address to notify when the dead-letter queue is not empty"
  type        = string
  default     = ""
}
