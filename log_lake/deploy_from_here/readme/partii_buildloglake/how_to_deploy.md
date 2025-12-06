# How to deploy

## I. Before you begin

* Active AWS account 
* Local copy of repo files 
* S3 bucket (and optionally, KMS key for this bucket) for supporting files such as stacks, Lambda zips, Glue scripts 

## II. Steps

### 0. Review and customize configuration files (if needed)

Inspect the `conf.py` files used by the AddAPart Lambda functions and the code used by both Glue jobs. 

If you use custom settings or your CloudWatch and CloudTrail configurations differ from the default behavior assumed in this blog, you may need to update:
- The `conf.py` settings for the Lambda functions
- The SQL queries for the Glue jobs

**If you make changes**, regenerate the necessary zip files by running:

`/log_lake/deploy_from_here/scripts/create_files_for_functions_flat.sh`

### 1. Upload supporting files to S3

Upload all files under `/log_lake/deploy_from_here` to your S3 bucket, preserving the same directory structure. These files include CloudFormation stacks and resources referenced by the stacks (Glue scripts, Lambda deployment packages).

### 2. Deploy the CloudFormation parent stack

Deploy the parent stack using the S3 URL where you uploaded the files.

Example URL:
`https://somebucket-for-supporting-files.s3.us-east-1.amazonaws.com/stacks/stack_for_loglake_parent_stack.yml`

Note: One of the parameters you will define is a unique label name that will be included in your AWS resource names. This guide uses the label `loglakeblog` as an example.

**Expected deployment time:** 5-10 minutes

## III. Post-deployment manual configuration

### 1. Update KMS key policies

#### a. Update the Supporting Files S3 Bucket KMS Key Policy

Add the following policy block to the KMS key associated with your supporting files S3 bucket. This grants the 5 IAM roles (2 for AddAPart raw, 2 for AddAPart readready, 1 for Glue jobs) permission to read the archived files.

``` json
{
    "Sid": "PermissionsForLogLakePipeline",
    "Effect": "Allow",
    "Principal": {
        "AWS": "*"
    },
    "Action": "kms:Decrypt",
    "Resource": "*",
    "Condition": {
        "StringEquals": {
            "aws:PrincipalArn": [
                "arn:aws:iam::123456789012:role/as-<YOUR-LABEL>-addapart-readready-producer-rol",
                "arn:aws:iam::123456789012:role/as-<YOUR-LABEL>-addapart-readready-consumer-rol",
                "arn:aws:iam::123456789012:role/as-<YOUR-LABEL>-addapart-consumer-raw-rol",
                "arn:aws:iam::123456789012:role/as-<YOUR-LABEL>-addapart-producer-raw-rol",
                "arn:aws:iam::123456789012:role/as-<YOUR-LABEL>-glue-job-readready-rol"
            ]
        }
    }
}
```

⚠️ Important: Replace `123456789012` with your actual AWS account number, and `<YOUR-LABEL>` with the custom label you defined during stack creation, in all five role ARNs.

#### b. Update the Log Lake KMS Key Policy

Add the policy block below to your Log Lake KMS key. This key encrypts your raw & readready S3 files and SQS queue.

To find this key:

1. Go to CloudFormation > Stacks
2. Select the StorageOnly stack (created in step1_storageonly)
3. Click the Resources tab
4. Find and open the EncryptKeyForLogLake resource

Policy to add:

``` json
{
    "Sid": "PermissionsForLogLakePipeline",
    "Effect": "Allow",
    "Principal": {
        "AWS": "*"
    },
    "Action": [
        "kms:GenerateDataKey*",
        "kms:DescribeKey",
        "kms:Decrypt"
    ],
    "Resource": "*",
    "Condition": {
        "StringEquals": {
            "aws:PrincipalArn": [
                "arn:aws:iam::123456789012:role/as-<YOUR-LABEL>-addapart-readready-producer-rol",
                "arn:aws:iam::123456789012:role/as-<YOUR-LABEL>-addapart-readready-consumer-rol",
                "arn:aws:iam::123456789012:role/as-<YOUR-LABEL>-addapart-consumer-raw-rol",
                "arn:aws:iam::123456789012:role/as-<YOUR-LABEL>-addapart-producer-raw-rol",
                "arn:aws:iam::123456789012:role/as-<YOUR-LABEL>-glue-job-readready-rol"
            ]
        }
    }
}
```

⚠️ Important: Replace `123456789012` with your actual AWS account number, and `<YOUR-LABEL>` with the custom label you defined during stack creation, in all five role ARNs.

### 2. Create S3 Event Notifications for Raw and ReadReady S3 Buckets

Before files can be associated with tables in the Glue Data Catalog, you must configure the AddAPart Lambda functions as targets for S3 event notifications. This is a manual step that serves as a gate before "lots of little lambdas" (LoLLs) start running on potentially significant untested volume.

Required configurations (4 total):

1. CloudTrail raw bucket → trigger raw producer function on object create events
2. CloudTrail readready bucket → trigger readready producer function on object create events
3. CloudWatch raw bucket → trigger raw producer function on object create events
4. CloudWatch readready bucket → trigger readready producer function on object create events

First time setup: We recommend configuring the S3 event notifications manually through the AWS Console to ensure you review all settings and avoid adding triggers for files or buckets that are not ready yet. However, we have also provided an automation script below that accomplishes this using the AWS CLI.

Automation script (for repeat deployments):

`/log_lake/deploy_from_here/scripts/add_s3_notifications.py`

Example usage:

``` sh
$ python add_s3_notifications.py \
--raw-lambda-arn arn:aws:lambda:us-east-1:123456789012:function:as-<YOUR-LABEL>-addapart-producer-raw \
--readready-lambda-arn arn:aws:lambda:us-east-1:123456789012:function:as-<YOUR-LABEL>-addapart-producer-readready \
--raw-buckets as-<YOUR-LABEL>-store-cloudtrail-raw,as-<YOUR-LABEL>-store-cloudwatch-raw \
--readready-buckets as-<YOUR-LABEL>-store-cloudtrail-readready,as-<YOUR-LABEL>-store-cloudwatch-readready
```

⚠️ Important: Replace `123456789012` with your actual AWS account number, and `<YOUR-LABEL>` with the custom label you defined during stack creation.

For help configuring event notifications that trigger your lambdas, see:
https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-event-notifications.html

### 3. Validate Deployment

Now go to `<repo>/log_lake/deploy_from_here/readme/how_to_demo.md` which walks you through:
- Uploading sample log files (CloudTrail and CloudWatch data)
- Validating that data flows through the pipeline (raw → readready tables)
- Running a sample query that correlates CloudTrail and CloudWatch logs to answer "who used Session Manager and what did they do?"

This demo validates your entire deployment is working end-to-end.