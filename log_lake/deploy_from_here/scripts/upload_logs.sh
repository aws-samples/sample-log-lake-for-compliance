#!/bin/bash
# Simple script to upload log files to S3
read -p "Enter bucket name for Supporting Files: " BUCKET_SUPPORTING_FILES
read -p "Enter bucket name for CloudTrail raw: " BUCKET_CLOUDTRAIL_RAW
read -p "Enter bucket name for CloudWatch raw: " BUCKET_CLOUDWATCH_RAW

echo "copy from Supporting Files bucket to raw S3 buckets to trigger raw producer function (1 time per file)..."

echo "Uploading CloudTrail log..."
aws s3 cp s3://"${BUCKET_SUPPORTING_FILES}"/demo_data/ssm/111111111111_CloudTrail_us-east-1_20240912T1554Z_sample.json.gz \
    s3://"${BUCKET_CLOUDTRAIL_RAW}"/AWSLogs/111111111111/CloudTrail/us-east-1/2024/09/12/111111111111_CloudTrail_us-east-1_20240912T1554Z_sample.json.gz

echo "Uploading CloudWatch log that matches CloudTrail..."
aws s3 cp s3://"${BUCKET_SUPPORTING_FILES}"/demo_data/ssm/cloudwatchrenametogz \
    s3://"${BUCKET_CLOUDWATCH_RAW}"/111111111111/aws/other/2024/09/12/15/cloudwatchrenametogz

echo "Uploading CloudWatch log with many message..."
aws s3 cp s3://"${BUCKET_SUPPORTING_FILES}"/demo_data/ssm/cloudwatch_manymessage_sample.json.renametogz \
    s3://"${BUCKET_CLOUDWATCH_RAW}"/111111111111/aws/other/2025/06/04/19/cloudwatch_manymessage_sample.json.renametogz

echo "done."
