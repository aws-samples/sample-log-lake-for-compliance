# How to Demo Log Lake Looker Lambda

This guide demonstrates how to use the Looker Lambda function, which allows you to query your Log Lake data using natural language. Looker uses Amazon Bedrock to convert conversational queries into SQL and execute them against your Athena tables.

## Prerequisites

Before starting, ensure you have:
- Completed the Bedrock stack deployment (see `how_to_deploy_bedrockonly.md`)
- Sample Bedrock logs loaded and processed (see `how_to_demo_bedrockonly.md`)
- The Glue database name (e.g., `loglakeblog`)
- The KMS key ARN used by Log Lake

## 1. Deploy the Looker Lambda Stack

Deploy the CloudFormation stack that creates the Looker Lambda function and its dependencies.

### Using AWS Console

1. Navigate to CloudFormation in the AWS Console
2. Click "Create stack" → "With new resources"
3. Upload the template file:
   ```
   for_blog/log_lake/deploy_from_here/stacks/stack_for_loglake_looker.yml
   ```
4. Provide the required parameters:
   - **myLabel**: Your resource naming prefix (e.g., `loglakeblog`)
   - **myBucketNameForSupportingFiles**: Bucket containing Lambda code
   - **myBucketNameForQueryResults**: Athena query results bucket
   - **myEncryptKeyArnForLogLake**: KMS key ARN from storage stack output
   - **myEncryptKeyForQueueIdOnly**: KMS key ID from storage stack output
   - **myBucketNameForRawBedrock**: Raw Bedrock bucket name
   - **myBucketNameForReadReadyBedrock**: Read-ready Bedrock bucket name

5. Review and create the stack


## 2. Update KMS Key Policy

Add the Looker Lambda's IAM role to your existing Log Lake KMS key policy to allow encryption/decryption operations.

### Steps

1. Open the KMS console and locate your Log Lake KMS key
2. Click "Edit" on the key policy
3. Find the statement that lists principals with `aws:PrincipalArn` condition
4. Add the Looker Lambda role ARN to the list:

```json
{
  "Sid": "PermissionsForLogLakePipeline",
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::YOUR_ACCOUNT_ID:root"
  },
  "Action": [
    "kms:Decrypt",
    "kms:Encrypt",
    "kms:GenerateDataKey*",
    "kms:DescribeKey"
  ],
  "Resource": "*",
  "Condition": {
    "StringEquals": {
      "aws:PrincipalArn": [
        "arn:aws:iam::123456789012:role/as-loglakeblog-addapart-readready-producer-rol",
        "arn:aws:iam::123456789012:role/as-loglakeblog-addapart-readready-consumer-rol",
        "arn:aws:iam::123456789012:role/as-loglakeblog-addapart-consumer-raw-rol",
        "arn:aws:iam::123456789012:role/as-loglakeblog-addapart-producer-raw-rol",
        "arn:aws:iam::123456789012:role/as-loglakeblog-glue-job-readready-rol",
        "arn:aws:iam::123456789012:role/as-loglakeblog-looker-lambda-role"
      ]
    }
  }
}
```

5. Replace `123456789012` with your actual AWS account ID
6. Replace role names with your actual role names (use the naming pattern from your `myLabel` parameter)
7. Save the key policy changes

**Note**: The Looker Lambda role ARN can be found in the CloudFormation stack outputs or in the IAM console.

## 3. Test the Looker Lambda function

Test the function by invoking it with a natural language query about your Bedrock logs.

### Option A: Test via AWS Console

1. Open the Lambda console and navigate to your Looker function (e.g., `as-loglakeblog-looker-lambda`)
2. Go to the "Test" tab
3. Create a new test event with this JSON structure:

```json
{
  "prompt_from_user": "Show me who used tools in Bedrock and what tools they used"
}
```

4. Click "Test" to invoke the function
5. Review the response in the execution results

### Option B: Test via AWS CLI

Run this command (replace `FUNCTION_NAME` with your actual function name):

```bash
aws lambda invoke \
  --function-name as-loglakeblog-looker-lambda \
  --payload '{"prompt_from_user": "Show me who used tools in Bedrock and what tools they used"}' \
  response.json

cat response.json
```

### Sample Prompts to Try

- "Show me who used tools in Bedrock and what tools they used"
- "List all Bedrock model invocations from the last week"
- "What models were invoked and how many tokens were used?"
- "Show me any Bedrock invocations that used the list_files_in_s3 tool"

The function will:
1. Convert your natural language query into SQL
2. Execute the query against your Log Lake tables in Athena
3. Return the results in a conversational format

## Notes

Allowing a model to generate SQL statements based on user input has risks, such
as prompt injection, improper content, and proliferation of senstive data.
Sanitizing and validating user inputs and model responses should used as part
of a defense in depth approach (more than one place). Here,
for simplicity we include what we consider to be minimal defensive measures:

1. The application code requires the sql starts with "select" prior to sending
to Athena
2. The IAM role used by the function has “glue:Get*” actions only (no mutation,
such as create, update, delete tables, partitions, or databases)
3. Only used interactively as part of adhoc human-in-the-loop review (not in
bulk or systemic)
4. System prompting outside a user's control

The above measures are application controls, within Looker's code. In addition,
or instead of custom application managed controls, Bedrock Guardrails can be
used without invoking the model.

For more info see:

GENSEC04-BP02 Sanitize and validate user inputs to foundation models
https://docs.aws.amazon.com/wellarchitected/latest/generative-ai-lens/gensec04-bp02.html

Use the [Bedrock] ApplyGuardrail API in your application
https://docs.aws.amazon.com/bedrock/latest/userguide/guardrails-use-independent-api.html