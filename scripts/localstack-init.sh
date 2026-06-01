#!/bin/sh
set -e

ENDPOINT=http://localhost:4566
REGION=us-east-1

echo "Inicializando LocalStack para framecast-worker..."

# Buckets S3
awslocal s3 mb s3://framecast-videos-raw   --region $REGION
awslocal s3 mb s3://framecast-videos-output --region $REGION

# Lifecycle: abortar multipart incompleto após 7 dias (bucket raw)
awslocal s3api put-bucket-lifecycle-configuration \
  --bucket framecast-videos-raw \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "abort-incomplete-multipart",
      "Status": "Enabled",
      "Filter": {"Prefix": ""},
      "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}
    }]
  }'

# DLQ
awslocal sqs create-queue \
  --queue-name framecast-processing-dlq \
  --region $REGION

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url $ENDPOINT/000000000000/framecast-processing-dlq \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)

# Fila principal com redrive para DLQ após 3 tentativas
awslocal sqs create-queue \
  --queue-name framecast-processing \
  --region $REGION \
  --attributes "{
    \"VisibilityTimeout\": \"900\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"
  }"

# Identidade SES verificada para dev
awslocal sesv2 create-email-identity \
  --email-identity noreply@framecast.local \
  --region $REGION 2>/dev/null || true

awslocal sesv2 create-email-identity \
  --email-identity dev@framecast.local \
  --region $REGION 2>/dev/null || true

echo "LocalStack pronto."
