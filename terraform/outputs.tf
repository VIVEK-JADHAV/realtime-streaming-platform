output "msk_cluster_arn" {
  description = "ARN of the MSK Serverless cluster"
  value       = aws_msk_serverless_cluster.main.arn
}

output "msk_cluster_uuid" {
  description = "MSK cluster UUID (use with aws kafka get-bootstrap-brokers)"
  value       = aws_msk_serverless_cluster.main.cluster_uuid
}

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs where MSK lives"
  value       = aws_subnet.private[*].id
}

output "msk_security_group_id" {
  description = "Security group for MSK access"
  value       = aws_security_group.msk.id
}

output "producer_lambda_role_arn" {
  description = "IAM role ARN for the producer Lambda"
  value       = aws_iam_role.producer_lambda.arn
}

output "cloudwatch_log_group" {
  description = "CloudWatch log group for streaming services"
  value       = aws_cloudwatch_log_group.msk_app.name
}

output "schema_registry_name" {
  description = "Glue Schema Registry name"
  value       = aws_glue_registry.shopstream.registry_name
}

output "schema_arn" {
  description = "ARN of the ClickstreamEvent schema"
  value       = aws_glue_schema.clickstream_event.arn
}

output "sns_topic_arn" {
  description = "SNS topic for streaming alerts"
  value       = aws_sns_topic.alerts.arn
}

output "dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.producer.dashboard_name}"
}
