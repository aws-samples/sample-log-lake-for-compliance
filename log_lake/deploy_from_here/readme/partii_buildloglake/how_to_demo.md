# How to Demo the Log Lake Solution

This guide walks you through validating your Log Lake deployment by uploading sample data and querying it with AWS Athena. By the end, you'll be able to answer questions like "Who used Session Manager, and what did they do?"

## Prerequisites

1. Before starting this demo, you must complete all deployment steps in:
`./log_lake/deploy_from_here/readme/deployment_steps.md`

2. Enable the Glue trigger that schedules both Glue jobs:

* For initial validation, set the trigger to run frequently (e.g., every 2 minutes)
* ⚠️ Remember to disable this after testing to avoid unwanted costs

3. Record your Glue database name to use in queries below. (This guide uses `loglakeblog` as an example)

4. Ensure clean environment:

* No existing partitions in tables (metadata)
* No existing files in S3 buckets (data)
* See Cleanup Scripts section for automation

## Choose Your Data Source

You have two options for demo data:

### Option A: Use Provided Sample Files (Recommended for First Time)

This is the fastest and simplest approach. Upload the pre-configured sample files from the `/log_lake/deploy_from_here/demo_data/ssm` folder.

#### Files to upload:

* **CloudTrail**: `111111111111_CloudTrail_us-east-1_20240912T1554Z_sample.json.gz`
* **CloudWatch (single message)**: `cloudwatchrenametogz`(this file matches with the CloudTrail sample above)
* **CloudWatch (multiple messages)**: `cloudwatch_manymessage_sample.json.r enametogz`
(tests parsing multiple messages from one JSON file)

#### Quick upload via automation script

Use the provided `upload_logs.sh` script to automatically upload all sample files:

1. Navigate to the scripts directory:
```sh
cd log_lake/deploy_from_here/scripts
```

2. Make the script executable (if needed):
```sh
chmod +x upload_logs.sh
```

3. Run the script:
```sh
./upload_logs.sh
```

4. When prompted, enter your bucket names:
   - Supporting Files bucket name (where demo data is stored)
   - CloudTrail raw bucket name (destination for CloudTrail logs)
   - CloudWatch raw bucket name (destination for CloudWatch logs)

The script will copy the sample log files from your Supporting Files bucket to the appropriate raw buckets, triggering the Lambda functions that add partitions to your Glue tables.

**What the script does:**
- Copies CloudTrail sample log to the correct S3 path structure
- Copies CloudWatch single-message sample (matches the CloudTrail session)
- Copies CloudWatch multi-message sample (tests parsing multiple events from one file)

### Option B: Generate Your Own Data

If you want to use real data from your environment:

1. [Launch an EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html)
2. [Complete Session Manager prerequisites](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-prerequisites.html)
3. [Configure Session Manager logging to S3](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-logging-s3.html)
4. [Connect to your instance via Session Manager](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/connect-with-systems-manager-session-manager.html)
5. Retrieve the resulting CloudTrail (StartSession API) and CloudWatch (sessiondata) files from the appropriate S3 buckets these were saved to (this depends on when you tested, your organization's approach for CloudTrail, your permissions, etc, to find these files)

Alternatively, set up S3 replication rules to populate the raw buckets with existing log data (files must be in their original, unprocessed, compressed format).

## Demo Steps

### Step 1: Upload Demo Data

Upload at least one CloudTrail file and one CloudWatch file to their respective raw S3 buckets. You will have accomplished this as part of the "Choose Your Data Source" step above.

Each upload triggers the `addapart` raw producer Lambda function.

### Step 2: Verify Raw Data in Athena

Query the raw tables to confirm your files were ingested. Example usage below (remember to replace `loglakeblog` with the name of the Glue Database you defined during stack creation)

CloudTrail
``` sql
SELECT * FROM loglakeblog.from_cloudtrail_raw
```

CloudWatch
``` sql
SELECT * FROM loglakeblog.from_cloudwatch_raw 
WHERE "$path" LIKE '%.gz'
```

Note: CloudWatch files without `.gz` extensions won't be correctly processed by the SerDe. The Glue Job filters for `%.gz` files to handle this.

### Step 3: Confirm Glue Jobs Ran Successfully

Check the AWS Glue console to ensure both Glue Jobs completed successfully after data appeared in the raw tables.

### Step 4: Verify ReadReady Data

Query the read-ready tables to confirm data was transformed correctly.

``` sql
SELECT * FROM loglakeblog.from_cloudtrail_readready
SELECT * FROM loglakeblog.from_cloudwatch_readready
```

**Expected results:**

CloudWatch should have 3 rows:

* 2 rows from the multi-message test file
* 1 row from the single-message file (matching CloudTrail)

### Step 5: Run the Combined Query

Execute the final query that joins CloudTrail and CloudWatch data to answer: **"Who used Session Manager, and what did they do?"**

Run this SQL file in Athena:
`/log_lake/deploy_from_here/scripts/who_did_what_in_session_manager.sql`

Expected result: 1 row with eventtime `2024-09-12 15:54:21.000`

## Deployment Validation Checklist

* ✅ Files exist in both raw S3 buckets
* ✅ Data appears in both raw tables `from_cloudwatch_raw` and `from_cloudtrail_raw`
* ✅ Files exist in both readready S3 buckets
* ✅ Data appears in both readready tables `from_cloudtrail_readready` and `from_cloudwatch_readready`
* ✅ Repeatability test: You can delete all data and metadata, then repeat the steps to successfully reload data (should complete in minutes)

## Reference Information

### Sample Log File Details

The provided sample files represent real AWS Systems Manager (SSM) session data:

**CloudTrail File Structure:**

S3 Key Pattern:
`AWSLogs/111111111111/CloudTrail/us-east-1/2024/09/12/111111111111_CloudTrail_us-east-1_20240912T1554Z_sample.json.gz`
Format: JSON gzipped (single line)
Note: Hour is derived from filename (e.g., 1554Z = hour 15)

**CloudWatch File Structure:**

S3 Key Pattern:
`111111111111/aws/other/2023/09/21/12/cloudwatch-logs-otherlogs-3-2023-09-21-12-01-04-831851e4-fb40-437b-8651-b693c9313b13`
Format: Gzipped, but no .gz extension (Amazon Data Firehose behavior)
Note: Log Lake's addAPart function adds the .gz extension for Athena/Glue compatibility

**Key Differences to Note:**

* Account ID represents where files originate, not necessarily where the bucket is located
* Partitioning schemes differ (CloudTrail includes region, CloudWatch includes hour)
* CloudWatch files lack .gz suffix in original format

## Cleanup Scripts

### Managing Partitions (Metadata)

``` sql
show partitions loglakeblog.from_cloudtrail_raw 
show partitions loglakeblog.from_cloudwatch_raw
show partitions loglakeblog.from_cloudtrail_readready
show partitions loglakeblog.from_cloudwatch_readready

--drop all partitions when testing raw 
alter table loglakeblog.from_cloudtrail_raw
drop if exists partition (logyear=2024)

alter table loglakeblog.from_cloudwatch_raw
drop if exists partition (logyear=2024)

--demo how readready has different time partitioning
alter table loglakeblog.from_cloudtrail_readready
drop partition (logcalendarday='20240912')

--demo how readready has different time partitioning
alter table loglakeblog.from_cloudwatch_readready
drop partition (logcalendarday='20240912')
```

### Deleting S3 Files (Data)

Use the provided automation script `.deploy_from_here/scripts/delete_s3_versions_simple.sh`

When prompted, enter your bucket names (comma-separated).


## Sample Log Files

The below sample CloudTrail and CloudWatch log files are generated from a real AWS
System Manager (SSM) session with an EC2 instance. 

### CloudTrail

For CloudTrail, this means you have logs with this S3 key naming (bucket name omitted): `AWSLogs/111111111111/CloudTrail/us-east-1/2024/09/12/111111111111_CloudTrail_us-east-1_20240912T1554Z_sample.json.gz`

Note that hour is not present in the folder path, but can be derived from the file name (in example above, loghour is 15).

**AWS CloudTrail File**

Sample S3 Path:
`s3://name-of-s3-bucket/AWSLogs/111111111111/CloudTrail/us-east-1/2024/09/15/111111111111_CloudTrail_us-east-1_20240912T1600Z_6Cs5Cnb3bXTQrGmh.json.gz`

For readability, we have expanded the contents multiline below. The actual file’s JSON contents must be on a single line for deployment, and the file format should be native CloudTrail format, JSON gzipped. 

``` json
{
    "Records": [
        {
            "eventVersion": "1.10",
            "userIdentity": {
                "type": "AssumedRole",
                "principalId": "SOMEPRINCIPALID:theuser-thefederation",
                "arn": "arn:aws:sts::111111111111:assumed-role/Admin/theuser-thefederation",
                "accountId": "111111111111",
                "accessKeyId": "somekey",
                "sessionContext": {
                    "sessionIssuer": {
                        "type": "Role",
                        "principalId": "SOMEPRINCIPALID",
                        "arn": "arn:aws:iam::111111111111:role/Admin",
                        "accountId": "111111111111",
                        "userName": "Admin"
                    },
                    "attributes": {
                        "creationDate": "2024-09-12T15:37:17Z",
                        "mfaAuthenticated": "false"
                    }
                }
            },
            "eventTime": "2024-09-12T15:54:21Z",
            "eventSource": "ssm.amazonaws.com",
            "eventName": "StartSession",
            "awsRegion": "us-east-1",
            "sourceIPAddress": "1.1.1.1",
            "userAgent": "",
            "requestParameters": {
                "target": "i-089ed2b5b1eaeebc2",
                "reason": "blog"
            },
            "responseElements": {
                "sessionId": "theuser-thefederation-0b7c1cc185ccf51a9",
                "tokenValue": "Value hidden due to security reasons.",
                "streamUrl": ""
            },
            "requestID": "f3e7c46a-5924-44be-87d4-dc8b00c86656",
            "eventID": "c5830d5e-b5da-4128-925b-0d8fef4a815d",
            "readOnly": false,
            "eventType": "AwsApiCall",
            "managementEvent": true,
            "recipientAccountId": "111111111111",
            "eventCategory": "Management",
            "tlsDetails": {
                "tlsVersion": "TLSv1.2",
                "cipherSuite": "ECDHE-RSA-AES128-GCM-SHA256",
                "clientProvidedHostHeader": "ssm.us-east-1.amazonaws.com"
            },
            "sessionCredentialFromConsole": "true"
        }
        
    ]
}
```

### CloudWatch

For CloudWatch, this means you have logs from Amazon Data Firehose that are gzipped, but do not have the gz extension with this S3 key (bucket name omitted): `111111111111/aws/other/2023/09/21/12/cloudwatch-logs-otherlogs-3-2023-09-21-12-01-04-831851e4-fb40-437b-8651-b693c9313b13`

**AWS CloudWatch File**

`s3://name-of-s3-bucket/2024/09/12/15/PUT-S3-loglake-1-2024-09-12-15-55-20-a89d3ee8-87b9-4696-9e1e-cbbda9ccd9d3 `
(format is gzip)

Note: when Data Firehose delivers compressed files to S3, it typically does not add a .gz extension to the filenames, even if the files are gzip-compressed. 

``` json
{ "messageType": "DATA_MESSAGE", "owner": "111111111111", "logGroup": "loggroup_for_sessionlogs", "logStream": "theuser-thefederation-0b7c1cc185ccf51a9", "subscriptionFilters": [ "mysessions" ], "logEvents": [ { "id": "38494576724164991552386496314680297076562947493154521088", "timestamp": 1726156519949, "message": "{\"eventVersion\":\"1.0\",\"eventTime\":\"2024-09-12T15:55:19Z\",\"awsRegion\":\"us-east-1\",\"target\":{\"id\":\"i-089ed2b5b1eaeebc2\"},\"userIdentity\":{\"arn\":\"arn:aws:sts::111111111111:assumed-role/Admin/theuser-thefederation\"},\"runAsUser\":\"ssm-user\",\"sessionId\":\"theuser-thefederation-0b7c1cc185ccf51a9\",\"sessionData\":[\"sh-5.2$ echo \\\"as colin and sean make a session at 8:55am sep 12 2024\\\"\",\"as colin and sean make a session at 8:55am sep 12 2024\"]}" } ] }
```

In the files above, notice these things:

* Account is where files come from, not the account where the bucket is
* Partitioning is different for these different sources (eg CloudTrail has region, CloudWatch has hour)
* There is no `.gz` suffix on CloudWatch original files (it is gzipped, but no `.gz` suffix is added by Data Firehose. Log Lake AddAPart Lamdbdas add the `.gz` so that Athena & Glue can read these files)

## Troubleshooting Tips

* If queries return no results, verify Glue jobs completed successfully
* Ensure file uploads triggered the Lambda function (check CloudWatch Logs)
* Confirm partitions exist using SHOW PARTITIONS commands
* Verify files have .gz extensions in processed buckets
* Check that your Glue database name matches what you're using in queries

## Need Help?

* Review deployment steps: `./log_lake/deploy_from_here/readme/deployment_steps.md`
* Check automation scripts in: `./log_lake/deploy_from_here/scripts/`
* Ensure file permissions are correct: `chmod +x <script_name>`