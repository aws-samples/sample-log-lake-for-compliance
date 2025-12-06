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
    "from_cloudtrail_readready": {
        "path_patterns": {
            "startswith_or_searchstring": "from_cloudtrail_readready/orc/",
            "ext_to_add": "",
        },
        "partition_placer_profile": {
            "samplekeytomatch": "from_cloudtrail_readready/orc/logcalendarday=20240912/loghour=15/part-00000-802c785c-9de1-4a8b-8535-f2d4a019551d.c000.zlib.orc",
            "location_under_bucket": r"(^from_cloudtrail_readready/orc/logcalendarday=[0-9]{8}/loghour=[0-9]{2}/)(.*)",
            "pattern_for_parts": r"^from_cloudtrail_readready/orc/logcalendarday=([0-9^/]{8})/loghour=([0-9^/]{2})/(.*)",
            "part_names": ["logcalendarday", "loghour"],
            "storage_descriptor_template": {
                "InputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                    "Parameters": {
                        "classification": "orc",
                        "notes": "demo",
                        "orc.compress": "ZLIB",
                        "partition_filtering.enabled": "true",
                    },
                },
            },
        },
    },
    "from_cloudwatch_readready": {
        "path_patterns": {
            "startswith_or_searchstring": "from_cloudwatch_readready/orc/",
            "ext_to_add": "",
        },
        "partition_placer_profile": {
            "samplekeytomatch": "from_cloudwatch_readready/orc/logcalendarday=20240912/loghour=15/part-00000-802c785c-9de1-4a8b-8535-f2d4a019551d.c000.zlib.orc",
            "location_under_bucket": r"(^from_cloudwatch_readready/orc/logcalendarday=[0-9]{8}/loghour=[0-9]{2}/)(.*)",
            "pattern_for_parts": r"^from_cloudwatch_readready/orc/logcalendarday=([0-9^/]{8})/loghour=([0-9^/]{2})/(.*)",
            "part_names": ["logcalendarday", "loghour"],
            "storage_descriptor_template": {
                "InputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                    "Parameters": {
                        "classification": "orc",
                        "notes": "demo",
                        "orc.compress": "ZLIB",
                        "partition_filtering.enabled": "true",
                    },
                },
            },
        },
    },
    "from_bedrock_readready": {
        "path_patterns": {
            "startswith_or_searchstring": "from_bedrock_readready/orc/",
            "ext_to_add": "",
        },
        "partition_placer_profile": {
            "samplekeytomatch": "from_bedrock_readready/orc/logcalendarday=20250617/loghour=15/part-00000-802c785c-9de1-4a8b-8535-f2d4a019551d.c000.zlib.orc",
            "location_under_bucket": r"(^from_bedrock_readready/orc/logcalendarday=[0-9]{8}/loghour=[0-9]{2}/)(.*)",
            "pattern_for_parts": r"^from_bedrock_readready/orc/logcalendarday=([0-9^/]{8})/loghour=([0-9^/]{2})/(.*)",
            "part_names": ["logcalendarday", "loghour"],
            "storage_descriptor_template": {
                "InputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                    "Parameters": {
                        "classification": "orc",
                        "notes": "demo",
                        "orc.compress": "ZLIB",
                        "partition_filtering.enabled": "true",
                    },
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
