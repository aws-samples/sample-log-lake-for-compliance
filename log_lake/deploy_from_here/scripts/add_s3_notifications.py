#!/usr/bin/env python3
"""
Script to set up S3 notifications for multiple buckets to trigger Lambda functions.
This script assumes all necessary permissions are already in place, from the AddAPart stacks (1 for raw, 1 for readready).

Usage:
    python add_s3_notifications.py \
        --readready-lambda-arn "arn:aws:lambda:region:account:function:readready-lambda" \
        --raw-lambda-arn "arn:aws:lambda:region:account:function:raw-lambda" \
        --raw-buckets "bucket1,bucket2" \
        --readready-buckets "bucket3,bucket4"
"""

import argparse
import json
import subprocess
import sys


def run_command(command):
    """Execute a shell command and return the output."""
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        print(f"Error message: {e.stderr}")
        sys.exit(1)


def create_notification_config(lambda_arn):
    """Create S3 notification configuration for a Lambda function.
    See https://docs.aws.amazon.com/cli/latest/reference/s3api/put-bucket-notification-configuration.html
    """
    return {
        "LambdaFunctionConfigurations": [
            {
                "Id": "triggerproducer_createdbyscript",
                "LambdaFunctionArn": lambda_arn,
                "Events": ["s3:ObjectCreated:*"],
            }
        ]
    }


def check_existing_notifications(bucket_name):
    """Check if bucket already has notification configurations."""
    try:
        command = (
            f"aws s3api get-bucket-notification-configuration --bucket {bucket_name}"
        )
        result = run_command(command)
        config = json.loads(result) if result else {}

        # Check if our specific notification already exists
        lambda_configs = config.get("LambdaFunctionConfigurations", [])
        for config in lambda_configs:
            if config.get("Id") == "triggerproducer_createdbyscript":
                return True
        return False
    except:
        # If command fails, assume no notifications exist
        return False


def apply_notification_to_bucket(bucket_name, config):
    """Apply notification configuration to an S3 bucket."""
    # Check if notification already exists
    if check_existing_notifications(bucket_name):
        print(
            f"Notification 'triggerproducer_createdbyscript' already exists for bucket: {bucket_name}"
        )
        return

    # Write config to a temporary file
    config_file = f"/tmp/{bucket_name}_notification_config.json"
    with open(config_file, "w") as f:
        json.dump(config, f)

    # Apply the configuration
    command = f"aws s3api put-bucket-notification-configuration --bucket {bucket_name} --notification-configuration file://{config_file}"
    result = run_command(command)
    print(
        f"Applied notification configuration in {config_file} to bucket: {bucket_name}"
    )
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Set up s3 event notifications to map bucket to producer lambda function"
    )
    parser.add_argument(
        "--readready-lambda-arn",
        required=True,
        help="ARN of producer lambda created by readready stack",
    )
    parser.add_argument(
        "--raw-lambda-arn",
        required=True,
        help="ARN of producer lambda created by raw stack",
    )
    parser.add_argument(
        "--raw-buckets",
        required=True,
        help="comma-separated list of bucket names that should trigger producer lambda for raw",
    )
    parser.add_argument(
        "--readready-buckets",
        required=True,
        help="comma-separated list of bucket names that should trigger producer lambda for readready",
    )

    args = parser.parse_args()

    # Parse bucket lists
    raw_buckets = [bucket.strip() for bucket in args.raw_buckets.split(",")]
    print(f"raw buckets list is: {raw_buckets}")
    readready_buckets = [bucket.strip() for bucket in args.readready_buckets.split(",")]
    print(f"readready buckets list is: {readready_buckets}")

    # Create notification configurations
    raw_config = create_notification_config(args.raw_lambda_arn)
    readready_config = create_notification_config(args.readready_lambda_arn)

    # Apply configurations to buckets
    print("Setting up notifications for raw buckets...")
    for bucket in raw_buckets:
        apply_notification_to_bucket(bucket, raw_config)

    print("Setting up notifications for readready buckets...")
    for bucket in readready_buckets:
        apply_notification_to_bucket(bucket, readready_config)

    print("All S3 notification configurations have been applied successfully!")


if __name__ == "__main__":
    main()
