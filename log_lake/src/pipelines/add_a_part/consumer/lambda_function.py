"""
AddAPart is a mechanism to add partitions to hive tables with
high performance, low cost, and customization. Source blog:
https://aws.amazon.com/blogs/big-data/create-a-customizable-cross-company-log-lake-for-compliance-part-i-business-background/
"""

import boto3
from botocore.exceptions import ClientError
import copy
import conf
import re
from typing import Optional, Union, List, Tuple, Dict
import logging
import os

glue_client = boto3.client("glue")
DATABASE_IN_GLUE = os.environ.get("DATABASE_IN_GLUE", "database_in_glue")
my_logging_signature = "addAPartConsumer"

PARTITION_PLACER_MAPPING = conf.partition_placer_mapping

"""
Pattern cache for compiled regex patterns, to insert new 
compiled patterns for reuse, or benefit from previously cached 
compiled patterns across invocations.
"""
PATTERN_CACHE = {}


def set_logging(passed_logger_name) -> logging.Logger:
    """
    Specify logging details, such as level
    """

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)s %(funcName)s %(levelname)-8s %(message)s",
        handlers=[stream_handler],
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

    # retrieve logger instance
    logger = logging.getLogger(
        passed_logger_name
    )  # root is the default logger name if not specified otherwise
    return logger


logger = set_logging(my_logging_signature)


def extract_parts_from_key(part_placer_profile, key) -> Tuple[dict, str]:
    """
    Parse the actual key using predefined, onboard config (zero external
    dependencies outside the function).

    """
    samplekey = part_placer_profile["samplekeytomatch"]
    pattern_for_parts_str = part_placer_profile["pattern_for_parts"]
    part_names = part_placer_profile["part_names"]
    location_under_bucket_str = part_placer_profile["location_under_bucket"]

    if location_under_bucket_str not in PATTERN_CACHE:
        PATTERN_CACHE[location_under_bucket_str] = re.compile(location_under_bucket_str)
    location_under_bucket = PATTERN_CACHE[location_under_bucket_str]

    m_location = location_under_bucket.match(key)
    if m_location:
        location_under_bucket_actual = m_location.group(1)
    else:
        message = f"location extracted from key {key} using pattern {location_under_bucket_str} yielded no result"
        raise ValueError(message)

    if pattern_for_parts_str not in PATTERN_CACHE:
        PATTERN_CACHE[pattern_for_parts_str] = re.compile(pattern_for_parts_str)
    pattern_for_parts = PATTERN_CACHE[pattern_for_parts_str]
    # check static sample key used for integrity check
    m_integritycheck = pattern_for_parts.match(samplekey)
    if m_integritycheck:
        # check if actual key matches expected pattern's capture groups
        m = pattern_for_parts.match(key)
        if m:
            parts = {}
            for i, part_name in enumerate(part_names, 1):  # regex groups start at 1
                groups_captured = len(m.groups())
                parts[part_name] = m.group(i) if i <= groups_captured else None

            return parts, location_under_bucket_actual

        message = f"Error: actual key does not match provided pattern, fix key or pattern (pattern: {pattern}, key: {key})"
        raise ValueError(message)
    message = f"Error: sample key integrity check does not match provided pattern, fix key or pattern (pattern: {pattern}, key: {samplekey})"
    raise ValueError(message)


def talk_to_glue(database_in_glue, table_name, part_list, storage_descriptor):
    """
    This is interface for adding partitions that do not exist.
    All talking between Lambda and Glue is encapsulated here.
    """
    try:
        glue_client.create_partition(
            DatabaseName=database_in_glue,
            TableName=table_name,
            PartitionInput={
                "Values": list(part_list.values()),
                "StorageDescriptor": storage_descriptor,
            },
        )
        logging.info(
            f"Partition values {part_list} with location {storage_descriptor['Location']} added to {database_in_glue}.{table_name}"
        )
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "AlreadyExistsException":
            logger.debug("Partition already exists.")
        else:
            raise (e)
    except Exception as e:
        message = f"Error: {e}"
        raise ValueError(message)


def tokenize_record_from_event(passed_record):
    """
    Take message from SQS and split it into parts. These parts will be later parsed according to the partition_placer_profile in conf.py
    Sample message from producer:
    message i will send is from_cloudwatch_raw,as-loglakedeux-store-cloudwatch-raw,111111111111/aws/other/2025/06/04/19/cloudwatch_manymessage_sample.json.renametogz.gz...
    """
    body = passed_record["body"]
    body_parts = body.split(",")
    failed_assertions = []

    try:
        assert len(body_parts) == 3
        tablename = body_parts[0]
        bucketname = body_parts[1]
        keyinbucket = body_parts[2]
    except AssertionError as e:
        message = f"unexpected tokens in message, expected format is tablename,bucketname,keyinbucket: {e}"
        failed_assertions.append(message)
    if failed_assertions:
        failed_assertions_str = "\n".join(failed_assertions)
        raise AssertionError(failed_assertions_str)
    return tablename, bucketname, keyinbucket


def build_storage_descriptor(
    template_from_profile, passed_bucket, passed_location_under_bucket
):
    # prepare StorageDescriptor for PartitionInput
    my_storage_descriptor = copy.deepcopy(template_from_profile)
    if len(passed_location_under_bucket) > 1:
        my_location = "s3://" + passed_bucket + "/" + passed_location_under_bucket
    else:
        my_location = "s3://" + passed_bucket
    my_storage_descriptor["Location"] = my_location
    return my_storage_descriptor


def add_a_part(passed_record):
    """
    Derive the tokens from the record, then use these tokens to
    add a partition to the existing table.

    """

    tablename, bucketname, keyinbucket = tokenize_record_from_event(passed_record)

    my_part_placer_profile = PARTITION_PLACER_MAPPING[tablename][
        "partition_placer_profile"
    ]
    logger.debug(f"partition_placer_profile is {my_part_placer_profile}")

    parts, location_under_bucket_actual = extract_parts_from_key(
        my_part_placer_profile, keyinbucket
    )
    logger.debug(f"parts are {parts}")
    logger.debug(f"location_under_bucket_actual is {location_under_bucket_actual}")

    my_storage_descriptor = build_storage_descriptor(
        my_part_placer_profile["storage_descriptor_template"],
        bucketname,
        location_under_bucket_actual,
    )
    logger.debug(f"storage_descriptor is {my_storage_descriptor}")

    if DATABASE_IN_GLUE:
        talk_to_glue(
            DATABASE_IN_GLUE,
            tablename,
            parts,
            my_storage_descriptor,
        )
    else:
        message = "database in glue missing, please check and retry"
        raise ValueError(message)


def lambda_handler(event, context):
    """
    Top level function that calls other functions from AWS Lambda service.
    Env vars and output back to caller handled here.
    All core logic should happen outside this function!
    """

    try:
        logger.info("initated...")
        # trigger is queue message event, NOT s3!
        if event["Records"] is not None:
            for record in event["Records"]:
                add_a_part(record)
    except KeyError as e:
        logger.info("key not found")
        raise e
    except Exception as e:
        raise e


"""
if __name__ == "__main__":
    pass
"""
