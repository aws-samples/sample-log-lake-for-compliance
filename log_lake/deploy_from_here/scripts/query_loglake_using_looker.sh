#!/bin/bash

read -p "Enter your custom label that you entered when you created the Looker Lambda: " LOOKER_LAMBDA_LABEL
read -p "Enter your AWS Account Number: " AWS_ACCOUNT_NUMBER

FUNCTION_NAME="as-$LOOKER_LAMBDA_LABEL-looker-lambda"
LOG_GROUP="/aws/lambda/$FUNCTION_NAME"
LOG_GROUP_ARN="arn:aws:logs:us-east-1:$AWS_ACCOUNT_NUMBER:log-group:/aws/lambda/as-loglakeblog-looker-lambda:*"
RESPONSE_FILE="response_$(date +%Y%m%d_%H%M%S).json"

(sleep 120; kill $$) &

aws lambda invoke \
--function-name "$FUNCTION_NAME" \
--payload '{"prompt_from_user":"can you query my bedrock logs table and tell me what you find?"}' \
--cli-binary-format raw-in-base64-out \
"$RESPONSE_FILE"

aws logs tail "$LOG_GROUP" --follow --since 5s