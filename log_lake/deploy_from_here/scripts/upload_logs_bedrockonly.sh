#!/bin/bash
# Simple script to upload log files to S3
read -p "Enter bucket name for Supporting Files: " BUCKET_SUPPORTING_FILES
read -p "Enter bucket for cloudtrail raw: " BUCKET_CLOUDTRAIL_RAW
read -p "Enter bucket for bedrock raw: " BUCKET_BEDROCK_RAW

echo "copy from Supporting Files S3 bucket to raw CloudTrail and Bedrock S3 buckets to trigger raw producer function (1 time per file)..."

echo "Uploading CloudTrail log..."
aws s3 cp s3://"${BUCKET_SUPPORTING_FILES}"/demo_data/bedrock/111111111111_CloudTrail_us-east-1_20250618T1925Z_bedrockcontroltest.json.gz \
    s3://"${BUCKET_CLOUDTRAIL_RAW}"/AWSLogs/111111111111/CloudTrail/us-east-1/2025/06/18/111111111111_CloudTrail_us-east-1_20250618T1925Z_bedrockcontroltest.json.gz

echo "Uploading Bedrock log that matches CloudTrail..."
aws s3 cp s3://"${BUCKET_SUPPORTING_FILES}"/demo_data/bedrock/20250618T191959438Z_14e8eb987f077da5_bedrock_invocation_log.json.gz \
    s3://"${BUCKET_BEDROCK_RAW}"/model_invocation_logging/AWSLogs/111111111111/BedrockModelInvocationLogs/us-east-1/2025/06/18/19/20250618T191959438Z_14e8eb987f077da5_bedrock_invocation_log.json.gz

echo "done."
