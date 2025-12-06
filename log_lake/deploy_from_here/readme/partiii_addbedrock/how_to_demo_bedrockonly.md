# Demo: Bedrock Model Invocation Logs

This demo validates that your Bedrock log lake deployment is working correctly by:
1. Uploading sample logs to S3
2. Processing them with Glue
3. Querying them with Athena

## Step 1: Upload Sample Logs to S3

We'll upload sample CloudTrail and Bedrock model invocation logs to test the pipeline.

Fastest and simplest approach - Upload pre-configured sample files from:
`/log_lake/deploy_from_here/demo_data/bedrock`

#### Files to upload:

* **CloudTrail**: 
`111111111111_CloudTrail_us-east-1_20250618T1925Z_bedrockcontroltest.json`
* **Bedrock Invocation Log**: 
`20250618T191959438Z_14e8eb987f077da5_bedrock_invocation_log.json`

#### Quick upload via automation script

Use the provided `upload_logs_bedrockonly.sh` script to automatically upload all sample files:

1. Navigate to the scripts directory:
```sh
cd log_lake/deploy_from_here/scripts
```

2. Make the script executable (if needed):
```sh
chmod +x upload_logs_bedrockonly.sh
```

3. Run the script:
```sh
./upload_logs_bedrockonly.sh
```

4. When prompted, enter your bucket names:
   - Supporting Files bucket name (where demo data is stored)
   - CloudTrail raw bucket name (destination for CloudTrail logs)
   - Bedrock raw bucket name (destination for Bedrock model invocation logs)

The script will copy the sample log files from your Supporting Files bucket to the appropriate raw buckets, triggering the Lambda functions that add partitions to your Glue tables.

**What the script does:**
- Copies CloudTrail sample log with Bedrock InvokeModel API call
- Copies Bedrock model invocation log showing tool usage (matches the CloudTrail event)


## Step 2: Run the Glue Job

Run the Glue job to process raw logs into ORC format:

1. Open Glue Console and click 'Run Job' on the `as-loglakeblog-process-raw-to-readready-bedrock` Glue Job.
2. Check job status shows "Succeeded" in the Glue Console.

The job parses raw JSON logs, extracts fields (timestamp, model ID, tool usage, tokens), and writes ORC files to the `from_bedrock_readready` table. Typically completes in 1-2 minutes.

## Step 3: Query with Athena

Query the processed logs to see tool usage:

1. Open Athena Console and select your database
2. Run the query from `./log_lake/deploy_from_here/scripts/who_used_a_tool_in_bedrock.sql`

This joins CloudTrail and Bedrock logs to show which IAM identities used tools in Bedrock, including the tool inputs and token counts.
 
## Appendix: Note on sample files

See files in `./log_lake/deploy_from_here/demo_data/bedrock`

These files were created using the sample function here:

`./log_lake/src/invoke_model_with_tool_use_for_logs`

**Sample files:**

The CloudTrail format has already been discussed in the demo steps. For Bedrock model invocation logs specifically, we use this example here:

LogLakeProofOfConcept/for_blog/log_lake/deploy_from_here/demo_data/bedrock/*bedrock_invocation_log.json.gz

``` json
{"schemaType":"ModelInvocationLog","schemaVersion":"1.0","timestamp":"2025-06-18T19:18:59Z","accountId":"111111111111","identity":{"arn":"arn:aws:sts::111111111111:assumed-role/createBedrockModelInvocationLogsWTool-role-j39nuuy3/createBedrockModelInvocationLogsWTool"},"region":"us-east-1","requestId":"f1b169e0-f596-4749-b13d-05c4b2569f1f","operation":"InvokeModel","modelId":"arn:aws:bedrock:us-east-1:111111111111:inference-profile/us.anthropic.claude-3-7-sonnet-20250219-v1:0","input":{"inputContentType":"application/json","inputBodyJson":{"anthropic_version":"bedrock-2023-05-31","max_tokens":1000,"messages":[{"role":"user","content":[{"type":"text","text":"list the files in an s3 bucket named as-colin-do-a-demo. Use the tool named list_files_in_s3"}]}],"tools":[{"name":"list_files_in_s3","description":"List files in an S3 bucket","input_schema":{"type":"object","properties":{"bucket_name":{"type":"string","description":"The name of the S3 bucket to list files from"}},"required":["bucket_name"]}}]},"inputTokenCount":434},"output":{"outputContentType":"application/json","outputBodyJson":{"id":"msg_bdrk_01JTFEVkdNKgCYAeuiJdFW5p","type":"message","role":"assistant","model":"claude-3-7-sonnet-20250219","content":[{"type":"text","text":"I'll help you list the files in the S3 bucket named \"as-colin-do-a-demo\" using the list_files_in_s3 tool."},{"type":"tool_use","id":"toolu_bdrk_01Q6PZ7sTqHKiEi87pmAV3Eb","name":"list_files_in_s3","input":{"bucket_name":"as-colin-do-a-demo"}}],"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":434,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":107}},"outputTokenCount":107},"inferenceRegion":"us-west-2"}
```

This file is created by setting up Bedrock model invocation logs as described here:
https://docs.aws.amazon.com/bedrock/latest/userguide/model-invocation-logging.html
