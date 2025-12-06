"""
Configurations below provide an additional port-of-entry control that enables
inclusion/exclusion independently of upstream configuration, for resiliency,
cost, and performance.

Design themes and intents:
*Encapsulate all (or almost all) physical implementations to the s3 key prefix.
In other words, Lambda code can be reused for different sources and buckets and
only the conf.py needs to change.

*resiliency: regex patterns that can cover multiple sources or varying
s3 key prefixes. Because of asynchronous event processing using queues, high
volume workloads can be dropped in s3 and workloads spread over time. No dead
letter queues needed for storing failures, because the raw tables are used as
the authoritative data store, not sqs. Cloudwatch alerts and alarms can be set up for
reacting to failures, and mitigations are simple: mechanisms to query target table's
metadata for missing expected partitions and adding what is expected, repeating
file movement. This ensures actual partitions are what's inspected, not just
messages (sqs can only consider content of 1 message, not partitions that were
actually added to a table across all historical messages).

*cost/performance: exclude unwanted individual accounts, such as a testing account that
generates most volume. Also can control scaling, through controls and features
of lambda and sqs, which has impact on cost & performance.

*customization. You can reuse this code for any source, as long as data files
come in to s3, and should be registered in Hive tables in the Glue catalog. We
recommend one function per bucket, but if you had 10 small data feeds going to
the same bucket, you could in theory specify all 10 mappings here, and reuse
the same function & config for 10 sources flowing to 1 bucket.
"""

partition_placer_mapping = {
    "from_cloudtrail_raw": {
        "path_patterns": {
            "startswith_or_searchstring": "CloudTrail",
            "ext_to_add": "",
        },
        "partition_placer_profile": {
            "samplekeytomatch": "AWSLogs/111111111111/CloudTrail/us-east-1/2024/09/12/111111111111_CloudTrail_us-east-1_20240912T1554Z_sample.json.gz",
            "location_under_bucket": r"(^AWSLogs/[0-9]{12}/CloudTrail/[a-z0-9-]{1,50}/[0-9]{4}/[0-9]{2}/[0-9]{2}/)(.*)",
            "pattern_for_parts": r"^AWSLogs/([^/]+)/CloudTrail/([^/]+)/([0-9^/]{4})/([0-9^/]{2})/([0-9^/]{2})",
            "part_names": [
                "logaccountid",
                "logregion",
                "logyear",
                "logmonth",
                "logday",
            ],
            "storage_descriptor_template": {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    "Parameters": {},
                },
            },
        },
    },
    "from_cloudwatch_raw": {
        "path_patterns": {
            "startswith_or_searchstring": "aws/other/",
            "ext_to_add": ".gz",
        },
        "partition_placer_profile": {
            "samplekeytomatch": "111111111111/aws/other/2023/09/21/12/cloudwatch-logs-otherlogs-3-2023-09-21-12-01-04-831851e4-fb40-437b-8651-b693c9313b13",
            "location_under_bucket": r"(^[0-9]{12}/aws/other/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/)(.*)",
            "pattern_for_parts": r"^([^/]+)/aws/other/([0-9^/]{4})/([0-9^/]{2})/([0-9^/]{2})/([0-9^/]{2})",
            "part_names": [
                "logaccountid",
                "logyear",
                "logmonth",
                "logday",
                "loghour",
            ],
            "storage_descriptor_template": {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.RegexSerDe",
                    "Parameters": {"input.regex": "(.*)"},
                },
            },
        },
    },
    "from_bedrock_raw": {
        "path_patterns": {
            "startswith_or_searchstring": "model_invocation_logging/AWSLogs/",
            "ext_to_add": "",
        },
        "partition_placer_profile": {
            "samplekeytomatch": "model_invocation_logging/AWSLogs/111111111111/BedrockModelInvocationLogs/us-east-1/2025/06/16/21/20250616T214839289Z_d9d645f13e82f32c.json.gz",
            "location_under_bucket": r"(^[^/]*/AWSLogs/[0-9]{12}/BedrockModelInvocationLogs/[^/]*/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/)(.*)",
            "pattern_for_parts": r"^[^/]*/AWSLogs/[0-9]{12}/BedrockModelInvocationLogs/([a-z0-9-]*)/([0-9^/]{4})/([0-9^/]{2})/([0-9^/]{2})/([0-9^/]{2})",
            "part_names": [
                "logregion",
                "logyear",
                "logmonth",
                "logday",
                "loghour",
            ],
            "storage_descriptor_template": {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    "Parameters": {},
                },
            },
        },
    },
}

# lever of control that allows filtering on account. Most used when data files
# are consolidated to one bucket from many origins, such as cloudtrail or
# cloudwatch logs from thousands of accounts flowing to 1 bucket.
allowed_accounts_only = False  # if True only process files with accounts in allowed_accounts list. if False no filter on accounts
allowed_accounts = [
    "111111111111",
]
