# Log Lake: Centralized AWS Log Analytics

A serverless data lake solution for procuring, processing, and providing AWS logs (CloudTrail, CloudWatch, and Bedrock model invocation logs) using S3, AWS Glue, Lambda, and Amazon Athena.

## What It Does

This solution creates a scalable log analytics pipeline that:
- Ingests raw AWS logs from multiple sources into S3
- Automatically partitions data using event-driven Lambda functions
- Transforms raw logs into optimized ORC format using AWS Glue
- Enables SQL queries across log sources using Amazon Athena

**Example use case:** Correlate CloudTrail API calls with CloudWatch session data to answer "Who used Session Manager and what did they do?"

## Architecture

- **Storage**: S3 buckets for raw and processed (readready) logs, encrypted with KMS
- **Metadata**: AWS Glue Data Catalog tables with automatic partition management
- **Processing**: AWS Glue jobs transform raw JSON to ORC format
- **Orchestration**: Lambda functions triggered by S3 events add partitions dynamically
- **Query**: Amazon Athena for SQL-based log analysis

## What's Included

### Part II: Core Log Lake (CloudTrail + CloudWatch)
- CloudTrail API call logs
- CloudWatch Logs (e.g., Session Manager session data)
- Automated raw-to-readready ETL pipeline
- Sample queries for security and operational analysis

### Part III: Bedrock Model Invocation Logs
- Amazon Bedrock model invocation logs
- Tool usage tracking and token consumption analysis
- Integration with CloudTrail for identity correlation

## Quick Start

### Prerequisites
- Active AWS account
- S3 bucket for supporting files (CloudFormation templates, Lambda packages, Glue scripts)
- Optional: KMS key for bucket encryption

### Deployment

1. **Deploy Core Log Lake**  
   Follow the guide: [`for_blog/log_lake/deploy_from_here/readme/partii_buildloglake/how_to_deploy.md`](for_blog/log_lake/deploy_from_here/readme/partii_buildloglake/how_to_deploy.md)
   
   - Upload supporting files to S3
   - Deploy CloudFormation parent stack
   - Configure KMS key policies
   - Set up S3 event notifications

2. **Validate Deployment**  
   Follow the demo: [`for_blog/log_lake/deploy_from_here/readme/partii_buildloglake/how_to_demo.md`](for_blog/log_lake/deploy_from_here/readme/partii_buildloglake/how_to_demo.md)
   
   - Upload sample CloudTrail and CloudWatch logs
   - Verify data flows through raw → readready tables
   - Run sample query: "Who used Session Manager?"

3. **Add Bedrock Logs (Optional)**  
   Follow the guide: [`for_blog/log_lake/deploy_from_here/readme/partiii_addbedrock/how_to_deploy_bedrockonly.md`](for_blog/log_lake/deploy_from_here/readme/partiii_addbedrock/how_to_deploy_bedrockonly.md)
   
   - Deploy Bedrock CloudFormation stack
   - Update IAM role permissions
   - Configure S3 event notifications for Bedrock buckets

4. **Demo Bedrock Integration**  
   Follow the demo: [`for_blog/log_lake/deploy_from_here/readme/partiii_addbedrock/how_to_demo_bedrockonly.md`](for_blog/log_lake/deploy_from_here/readme/partiii_addbedrock/how_to_demo_bedrockonly.md)

## Documentation

- **Deployment Guides**: [`for_blog/log_lake/deploy_from_here/readme/`](for_blog/log_lake/deploy_from_here/readme/)
- **Automation Scripts**: [`for_blog/log_lake/deploy_from_here/scripts/`](for_blog/log_lake/deploy_from_here/scripts/)
- **Sample Data**: [`for_blog/log_lake/deploy_from_here/demo_data/`](for_blog/log_lake/deploy_from_here/demo_data/)
- **Partition Indexes**: [`for_blog/log_lake/deploy_from_here/readme/partii_buildloglake/how_to_add_partition_indexes_in_glue.md`](for_blog/log_lake/deploy_from_here/readme/partii_buildloglake/how_to_add_partition_indexes_in_glue.md)

## Key Features

- **Event-driven partitioning**: Lambda functions automatically add Glue partitions when new logs arrive
- **Optimized storage**: Raw JSON logs transformed to ORC format for faster queries and lower costs
- **Multi-source correlation**: Join CloudTrail, CloudWatch, and Bedrock logs in a single query
- **Repeatable deployment**: CloudFormation templates and automation scripts for consistent setup
- **Security-first**: KMS encryption, IAM least privilege, and bucket policies

## Cost Optimization

- Glue jobs run on-demand (not scheduled by default)
- ORC format reduces storage and query costs
- Partition pruning minimizes data scanned by Athena
- Lifecycle policies can archive older logs to Glacier

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
