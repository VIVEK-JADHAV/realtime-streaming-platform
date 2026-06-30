terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      Component   = "streaming"
    }
  }
}

data "aws_caller_identity" "current" {}

# -----------------------------------------------------
# NETWORKING (self-contained for sandbox resets)
# -----------------------------------------------------

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs            = slice(data.aws_availability_zones.available.names, 0, 3)
  account_id     = data.aws_caller_identity.current.account_id
  cluster_name   = "${var.project_name}-${var.environment}-msk"
  topic_name     = "shopstream.clickstream"
  partition_count = 12
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.project_name}-${var.environment}-streaming-vpc"
  }
}

resource "aws_subnet" "public" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index}.0/24"
  availability_zone = local.azs[count.index]

  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-${var.environment}-public-${local.azs[count.index]}"
  }
}

resource "aws_subnet" "private" {
  count             = 3
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 10}.0/24"
  availability_zone = local.azs[count.index]

  tags = {
    Name = "${var.project_name}-${var.environment}-private-${local.azs[count.index]}"
  }
}

# NAT Gateway — allows Lambda in private subnets to reach STS/internet for IAM auth
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-${var.environment}-igw"
  }
}

resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "${var.project_name}-${var.environment}-nat-eip"
  }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  depends_on = [aws_internet_gateway.main]

  tags = {
    Name = "${var.project_name}-${var.environment}-nat"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-public-rt"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-private-rt"
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = 3
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

resource "aws_security_group" "msk" {
  name        = "${local.cluster_name}-sg"
  description = "Security group for MSK Serverless cluster"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Kafka from within VPC"
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  ingress {
    description = "Self-referencing rule for MSK Serverless ENIs"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.cluster_name}-sg"
  }
}

# -----------------------------------------------------
# MSK SERVERLESS CLUSTER
# -----------------------------------------------------

resource "aws_msk_serverless_cluster" "main" {
  cluster_name = local.cluster_name

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.msk.id]
  }

  tags = {
    Name = local.cluster_name
  }
}

# -----------------------------------------------------
# GLUE SCHEMA REGISTRY
# -----------------------------------------------------

resource "aws_glue_registry" "shopstream" {
  registry_name = "${var.project_name}-${var.environment}-registry"
  description   = "Schema registry for ShopStream event schemas"

  tags = {
    Name = "${var.project_name}-${var.environment}-registry"
  }
}

resource "aws_glue_schema" "clickstream_event" {
  schema_name       = "ClickstreamEvent"
  registry_arn      = aws_glue_registry.shopstream.arn
  data_format       = "AVRO"
  compatibility     = "BACKWARD"
  schema_definition = file("${path.module}/../src/producer/schemas/clickstream_event.avsc")

  tags = {
    Name = "${var.project_name}-${var.environment}-clickstream-schema"
  }
}

# -----------------------------------------------------
# CLOUDWATCH LOG GROUP (for producer/consumer logs)
# -----------------------------------------------------

resource "aws_cloudwatch_log_group" "msk_app" {
  name              = "/shopstream/${var.environment}/streaming"
  retention_in_days = 7

  tags = {
    Name = "${var.project_name}-${var.environment}-streaming-logs"
  }
}

# -----------------------------------------------------
# IAM POLICY FOR MSK ACCESS (used by Lambda producer)
# -----------------------------------------------------

data "aws_iam_policy_document" "msk_access" {
  statement {
    sid    = "MSKGetBrokers"
    effect = "Allow"
    actions = [
      "kafka:GetBootstrapBrokers",
      "kafka:DescribeCluster",
    ]
    resources = [aws_msk_serverless_cluster.main.arn]
  }

  statement {
    sid    = "MSKConnect"
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
    ]
    resources = [aws_msk_serverless_cluster.main.arn]
  }

  statement {
    sid    = "MSKTopicReadWrite"
    effect = "Allow"
    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData",
      "kafka-cluster:ReadData",
    ]
    resources = [
      "arn:aws:kafka:${var.aws_region}:${local.account_id}:topic/${local.cluster_name}/*"
    ]
  }

  statement {
    sid    = "MSKConsumerGroups"
    effect = "Allow"
    actions = [
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup",
    ]
    resources = [
      "arn:aws:kafka:${var.aws_region}:${local.account_id}:group/${local.cluster_name}/*"
    ]
  }
}

resource "aws_iam_policy" "msk_access" {
  name   = "${var.project_name}-${var.environment}-msk-access"
  policy = data.aws_iam_policy_document.msk_access.json
}

# Lambda execution role for the producer
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "producer_lambda" {
  name               = "${var.project_name}-${var.environment}-producer-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "producer_msk" {
  role       = aws_iam_role.producer_lambda.name
  policy_arn = aws_iam_policy.msk_access.arn
}

resource "aws_iam_role_policy_attachment" "producer_logs" {
  role       = aws_iam_role.producer_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "producer_vpc" {
  role       = aws_iam_role.producer_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Glue Schema Registry access for Avro serialization
data "aws_iam_policy_document" "glue_schema_access" {
  statement {
    sid    = "GlueSchemaRegistry"
    effect = "Allow"
    actions = [
      "glue:GetRegistry",
      "glue:GetSchema",
      "glue:GetSchemaVersion",
      "glue:GetSchemaByDefinition",
      "glue:GetSchemaVersionsDiff",
      "glue:ListSchemaVersions",
      "glue:RegisterSchemaVersion",
      "glue:GetTags",
    ]
    resources = [
      aws_glue_registry.shopstream.arn,
      aws_glue_schema.clickstream_event.arn,
      "${aws_glue_schema.clickstream_event.arn}*",
    ]
  }
}

resource "aws_iam_policy" "glue_schema_access" {
  name   = "${var.project_name}-${var.environment}-glue-schema-access"
  policy = data.aws_iam_policy_document.glue_schema_access.json
}

resource "aws_iam_role_policy_attachment" "producer_glue" {
  role       = aws_iam_role.producer_lambda.name
  policy_arn = aws_iam_policy.glue_schema_access.arn
}

# CloudWatch metrics + S3 DLQ access
data "aws_iam_policy_document" "producer_extras" {
  statement {
    sid    = "CloudWatchMetrics"
    effect = "Allow"
    actions = [
      "cloudwatch:PutMetricData",
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["ShopStream/Kafka"]
    }
  }

  statement {
    sid    = "S3DLQWrite"
    effect = "Allow"
    actions = [
      "s3:PutObject",
    ]
    resources = [
      "${aws_s3_bucket.dlq.arn}/dlq/*",
    ]
  }
}

resource "aws_iam_policy" "producer_extras" {
  name   = "${var.project_name}-${var.environment}-producer-extras"
  policy = data.aws_iam_policy_document.producer_extras.json
}

resource "aws_iam_role_policy_attachment" "producer_extras" {
  role       = aws_iam_role.producer_lambda.name
  policy_arn = aws_iam_policy.producer_extras.arn
}

# S3 bucket for DLQ (dead-letter path)
resource "aws_s3_bucket" "dlq" {
  bucket = "${var.project_name}-raw-${local.account_id}"

  tags = {
    Name = "${var.project_name}-raw-${local.account_id}"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "dlq" {
  bucket = aws_s3_bucket.dlq.id

  rule {
    id     = "expire-dlq"
    status = "Enabled"

    filter {
      prefix = "dlq/"
    }

    expiration {
      days = 30
    }
  }
}

# -----------------------------------------------------
# PRODUCER LAMBDA
# -----------------------------------------------------

data "archive_file" "producer" {
  type        = "zip"
  source_dir  = "${path.module}/../src/producer"
  output_path = "${path.module}/.build/producer.zip"
}

resource "aws_lambda_function" "producer" {
  function_name    = "${var.project_name}-${var.environment}-clickstream-producer"
  role             = aws_iam_role.producer_lambda.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = 60
  memory_size      = 256
  filename         = data.archive_file.producer.output_path
  source_code_hash = data.archive_file.producer.output_base64sha256

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.msk.id]
  }

  environment {
    variables = {
      MSK_CLUSTER_ARN      = aws_msk_serverless_cluster.main.arn
      TOPIC_NAME           = local.topic_name
      AWS_MSK_REGION       = var.aws_region
      SCHEMA_REGISTRY_NAME = aws_glue_registry.shopstream.registry_name
      SCHEMA_NAME          = aws_glue_schema.clickstream_event.schema_name
      DLQ_BUCKET           = aws_s3_bucket.dlq.id
    }
  }

  layers = [aws_lambda_layer_version.kafka_deps.arn]

  tags = {
    Name = "${var.project_name}-${var.environment}-clickstream-producer"
  }
}

# Lambda layer with confluent-kafka + dependencies
resource "aws_lambda_layer_version" "kafka_deps" {
  filename            = "${path.module}/../layers/kafka-deps.zip"
  layer_name          = "${var.project_name}-${var.environment}-kafka-deps"
  compatible_runtimes = ["python3.12"]
  description         = "confluent-kafka and aws-msk-iam-sasl-signer"
}

# -----------------------------------------------------
# MONITORING — SNS + ALARMS + DASHBOARD
# -----------------------------------------------------

# Metric filters — extract produce rate and error count from Lambda logs
resource "aws_cloudwatch_log_group" "producer_lambda" {
  name              = "/aws/lambda/${aws_lambda_function.producer.function_name}"
  retention_in_days = 7

  tags = {
    Name = "${var.project_name}-${var.environment}-producer-lambda-logs"
  }
}

# Metric filters — extract produce rate and error count from Lambda logs
resource "aws_cloudwatch_log_metric_filter" "delivered_count" {
  name           = "${var.project_name}-${var.environment}-delivered-count"
  log_group_name = aws_cloudwatch_log_group.producer_lambda.name
  pattern        = "{ $.delivered > 0 }"

  metric_transformation {
    name      = "EventsDelivered"
    namespace = "ShopStream/Kafka"
    value     = "$.delivered"
  }
}

resource "aws_cloudwatch_log_metric_filter" "error_count" {
  name           = "${var.project_name}-${var.environment}-error-count"
  log_group_name = aws_cloudwatch_log_group.producer_lambda.name
  pattern        = "{ $.errors > 0 }"

  metric_transformation {
    name      = "EventErrors"
    namespace = "ShopStream/Kafka"
    value     = "$.errors"
  }
}

# SNS topic for alerts
resource "aws_sns_topic" "alerts" {
  name = "${var.project_name}-${var.environment}-streaming-alerts"

  tags = {
    Name = "${var.project_name}-${var.environment}-streaming-alerts"
  }
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Alarm: DLQ file written (any error = immediate alert)
resource "aws_cloudwatch_metric_alarm" "dlq_objects" {
  alarm_name          = "${var.project_name}-${var.environment}-dlq-not-empty"
  alarm_description   = "Dead-letter queue received failed events — investigate immediately"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0
  period              = 60
  statistic           = "Sum"
  metric_name         = "NumberOfObjects"
  namespace           = "AWS/S3"
  treat_missing_data  = "notBreaching"

  dimensions = {
    BucketName  = aws_s3_bucket.dlq.id
    StorageType = "AllStorageTypes"
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]

  tags = {
    Name = "${var.project_name}-${var.environment}-dlq-alarm"
  }
}

# Alarm: Lambda errors (function failures, not Kafka delivery errors)
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.project_name}-${var.environment}-producer-errors"
  alarm_description   = "Producer Lambda is throwing exceptions"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0
  period              = 300
  statistic           = "Sum"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.producer.function_name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]

  tags = {
    Name = "${var.project_name}-${var.environment}-producer-errors-alarm"
  }
}

# Alarm: Lambda duration approaching timeout
resource "aws_cloudwatch_metric_alarm" "lambda_duration" {
  alarm_name          = "${var.project_name}-${var.environment}-producer-slow"
  alarm_description   = "Producer Lambda duration > 45s (timeout is 60s) — likely MSK connectivity issues"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  threshold           = 45000
  period              = 300
  statistic           = "Maximum"
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.producer.function_name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]

  tags = {
    Name = "${var.project_name}-${var.environment}-producer-slow-alarm"
  }
}

# CloudWatch Dashboard
resource "aws_cloudwatch_dashboard" "producer" {
  dashboard_name = "${var.project_name}-${var.environment}-producer"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Producer Invocations & Errors"
          region  = var.aws_region
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", aws_lambda_function.producer.function_name, { stat = "Sum", label = "Invocations" }],
            ["AWS/Lambda", "Errors", "FunctionName", aws_lambda_function.producer.function_name, { stat = "Sum", label = "Errors", color = "#d13212" }],
          ]
          period = 60
          view   = "timeSeries"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Producer Duration (ms)"
          region  = var.aws_region
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", aws_lambda_function.producer.function_name, { stat = "Average", label = "Avg" }],
            ["AWS/Lambda", "Duration", "FunctionName", aws_lambda_function.producer.function_name, { stat = "Maximum", label = "p100", color = "#d13212" }],
          ]
          period = 60
          view   = "timeSeries"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "Messages Per Partition"
          region  = var.aws_region
          metrics = [for i in range(12) :
            ["ShopStream/Kafka", "MessagesPerPartition", "Topic", local.topic_name, "Partition", tostring(i), { stat = "Sum", label = "P${i}" }]
          ]
          period = 300
          view   = "timeSeries"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "S3 DLQ Object Count"
          region  = var.aws_region
          metrics = [
            ["AWS/S3", "NumberOfObjects", "BucketName", aws_s3_bucket.dlq.id, "StorageType", "AllStorageTypes", { stat = "Average", label = "DLQ Files" }],
          ]
          period = 86400
          view   = "timeSeries"
        }
      },
    ]
  })
}
