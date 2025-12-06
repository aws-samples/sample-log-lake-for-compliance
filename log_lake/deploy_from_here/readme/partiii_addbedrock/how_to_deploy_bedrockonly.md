# How to deploy

## AWS Bedrock Model Invocation Logs

This guide walks you through adding Amazon Bedrock model invocation logs to your existing log lake deployment.

### 1. Run the CloudFormation template
`stack_for_loglake_bedrock_stack.yaml`

Follow the parameter descriptions for populating the CloudFormation parameters. This stack will create:

* 2 new S3 Buckets for Bedrock raw & readready, with appropriate security configurations (encryption, lifecycle policies, and access controls)
* 1 Glue Job to process raw to readready 
* 2 new Glue Tables: `from_bedrock_raw` & `from_bedrock_readready`


### 2. Update the Glue Job IAM Role Permissions

The Bedrock Glue job will reuse the existing Glue job IAM role from the main log lake deployment. This role needs additional permissions to read from the raw Bedrock S3 bucket and write to the readready Bedrock S3 bucket.

#### 1. Locate the existing Glue job IAM role:
   - In the AWS Console, navigate to IAM → Roles
   - Find the role created by your main log lake stack: `as-loglakeblog-glue-job-readready-rol`
   - This is the same role used by the LogLake CloudTrail and CloudWatch Glue jobs

#### 2. Update the IAM role's inline policies to include the new Bedrock S3 buckets:
   - Find the Bedrock S3 bucket ARNs in your Bedrock CloudFormation stack outputs: `BucketNameForRawBedrock` and `BucketNameForReadReadyBedrock`
   - Add these bucket ARNs to the existing inline policies as shown below

In the `ReadFromBuckets` IAM Policy block, add both Bedrock S3 Buckets to the `Resource` array:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SourceBucketsForRead",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "other existing bucket ARNs...",
                "arn:aws:s3:::as-loglakeblog-store-bedrock-raw",
                "arn:aws:s3:::as-loglakeblog-store-bedrock-raw/*",
                "arn:aws:s3:::as-loglakeblog-store-bedrock-readready",
                "arn:aws:s3:::as-loglakeblog-store-bedrock-readready/*"
            ]            
        }
    ]
}
```

In the `WriteToBucketsForReadReady` IAM Policy block, add only the readready Bedrock bucket to the `Resource` array:

``` json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "TargetBucketsForWrite",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "other existing bucket ARNs...",
                "arn:aws:s3:::as-loglakeblog-store-bedrock-readready",
                "arn:aws:s3:::as-loglakeblog-store-bedrock-readready/*"
            ]            
        }
    ]
}
```

### 3. Update the AddAPart Lambda Role Permissions

The addAPart Lambda functions need permissions to access the new Bedrock buckets to add partition metadata.

#### 1. Update the AddAPart Producer Raw Lambda role

- In the AWS Console, navigate to IAM → Roles
- Find the role `as-loglakeblog-addapart-producer-raw-rol`
- Locate the inline policy that grants S3 read permissions

In the `readFilesFromBucket` IAM Policy block, add only the **raw** Bedrock bucket to the `Resource` array:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetObject",
                "s3:GetObjectTagging",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::as-loglakeblog-store-supporting-files",
                "arn:aws:s3:::as-loglakeblog-store-supporting-files/*",
                "arn:aws:s3:::as-loglakeblog-store-cloudtrail-raw",
                "arn:aws:s3:::as-loglakeblog-store-cloudtrail-raw/*",
                "arn:aws:s3:::as-loglakeblog-store-cloudwatch-raw",
                "arn:aws:s3:::as-loglakeblog-store-cloudwatch-raw/*",
                "arn:aws:s3:::as-loglakeblog-store-bedrock-raw",
                "arn:aws:s3:::as-loglakeblog-store-bedrock-raw/*"
            ],
            "Effect": "Allow"
        }
    ]
}
```

#### 2. Update the AddAPart Producer Readready Lambda role

- In the AWS Console, navigate to IAM → Roles
- Find the role `as-loglakeblog-addapart-producer-readready-rol`
- Locate the inline policy that grants S3 read permissions

In the `readFilesFromBucket` IAM Policy block, add only the **readready** Bedrock bucket to the `Resource` array:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetObject",
                "s3:GetObjectTagging",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::as-loglakeblog-store-supporting-files",
                "arn:aws:s3:::as-loglakeblog-store-supporting-files/*",
                "arn:aws:s3:::as-loglakeblog-store-cloudtrail-readready",
                "arn:aws:s3:::as-loglakeblog-store-cloudtrail-readready/*",
                "arn:aws:s3:::as-loglakeblog-store-cloudwatch-readready",
                "arn:aws:s3:::as-loglakeblog-store-cloudwatch-readready/*",
                "arn:aws:s3:::as-loglakeblog-store-bedrock-readready",
                "arn:aws:s3:::as-loglakeblog-store-bedrock-readready/*"
            ],
            "Effect": "Allow"
        }
    ]
}
```

### 4. Configure S3 Event Notifications

The Bedrock buckets need to trigger the addAPart Lambda functions when new objects are created.

#### Option A: Use the provided script (recommended)

1. Add Lambda invoke permissions for S3 using the AWS CLI commands below:

``` sh
# For the raw bucket AddAPart Producer Lambda
aws lambda add-permission \
  --function-name as-loglakeblog-addapart-producer-raw \
  --statement-id bedrock-s3-trigger \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::as-loglakeblog-store-bedrock-raw \
  --source-account 123456789012

# For the readready bucket AddAPart Producer Lambda
aws lambda add-permission \
  --function-name as-loglakeblog-addapart-producer-readready \
  --statement-id bedrock-s3-trigger \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::as-loglakeblog-store-bedrock-readready \
  --source-account 123456789012
```
⚠️ Important: Replace `123456789012` with your actual AWS account number, and `loglakeblog` with the custom label you defined during stack creation.

Note: We add permissions separately (rather than in the CloudFormation stack like the snippet below) to avoid creating a circular dependency on the Lambda ARN and to demonstrate alternative permission management approaches.

``` yaml
S3BedrockRawLambdaPermission:
  Type: AWS::Lambda::Permission
  Properties:
    FunctionName: !Ref arnForAddAPartProducerRaw
    Action: lambda:InvokeFunction
    Principal: s3.amazonaws.com
    SourceAccount: !Ref 'AWS::AccountId'
    SourceArn: !GetAtt StoreRawBedrockBucket.Arn
```

2. Run the `add_s3_notifications.py` script to automatically configure the S3 event notifications.

Script: `./log_lake/deploy_from_here/scripts/add_s3_notifications.py`

Example usage:

``` sh
$ python add_s3_notifications.py \
--raw-lambda-arn arn:aws:lambda:us-east-1:123456789012:function:as-loglakeblog-addapart-producer-raw \
--readready-lambda-arn arn:aws:lambda:us-east-1:123456789012:function:as-loglakeblog-addapart-producer-readready \
--raw-buckets as-loglakeblog-store-bedrock-raw \
--readready-buckets as-loglakeblog-store-bedrock-readready
```

#### Option B: Manual configuration via AWS Console

1. Navigate to S3 → Select the bucket → Properties → Event notifications
2. Create a notification for "All object create events"
3. Set the destination to the appropriate Lambda function
4. Add the Lambda invoke permissions using the AWS CLI commands shown above

### 5. Validate Deployment

Now go to `<repo>/log_lake/deploy_from_here/readme/how_to_demo_bedrockonly.md` which walks you through:
- Uploading sample CloudTrail and Bedrock model invocation logs to your S3 buckets
- Running the Glue job to process raw Bedrock logs into readready format
- Executing a sample Athena query (`who_used_a_tool_in_bedrock.sql`) to analyze tool usage in Bedrock

This demo validates your entire deployment is working end-to-end.