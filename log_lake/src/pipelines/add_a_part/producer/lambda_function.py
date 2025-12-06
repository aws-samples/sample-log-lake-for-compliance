"""
AddAPart is a mechanism to add partitions to hive tables with
high performance, low cost, and customization. Source blog:
https://aws.amazon.com/blogs/big-data/create-a-customizable-cross-company-log-lake-for-compliance-part-i-business-background/
"""

import os
import logging
import boto3
import urllib.parse
import conf
import zlib
from typing import Optional, Union, List, Tuple, Dict


session = boto3.Session()
sqs_client = session.client("sqs")
qurl_as_str = os.environ["thequeueurl"]
s3_client = boto3.client("s3")
my_logging_signature = "addAPartProducer"

PARTITION_PLACER_MAPPING = conf.partition_placer_mapping


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


def send_message(themessage, theid, the_dup_id) -> str:
    """
    Send message to the sqs queue, which will trigger the consumer lambda to do work.
    """

    responses = sqs_client.send_message(
        QueueUrl=qurl_as_str,
        MessageBody=themessage,
        MessageGroupId=theid,
        MessageDeduplicationId=the_dup_id,
    )
    return responses


def should_i_send(key: str) -> bool:
    """
    apply any additional screening based on incoming key and conf settings.
    """

    # poisonpilled by default
    send = False
    if conf.allowed_accounts_only:
        for account in conf.allowed_accounts:
            # is the account string contained anywhere in the s3 key
            if account in key:
                logging.debug(f"allowed account {account} in key {key}, i will send...")
                send = True
    else:
        # no account filtering, send all
        logging.debug(f"no account filtering, i will send...")
        send = True
    assert send is True or send is False
    return send


def get_checksum_for_dedup(text: str) -> str:
    """
    Uses CRC32 hash for faster checksum calculation than sha256.
    This is for tracking if file name changes, not for encryption/security.
    """
    # Convert text to bytes using utf-8 encoding
    text_bytes = text.encode("utf8")

    # Calculate CRC32 checksum
    crc_object = zlib.crc32(text_bytes)

    # Convert to hexadecimal string to shorten key used in boto3 send_message call
    hex_string = format(crc_object, "08x")
    assert len(hex_string) == 8
    return hex_string


def make_my_message(tablename: str, bucket: str, key: str) -> Tuple[str, str, str]:
    """
    Below is the message contract that consumer directly couples against. if
    you change this contract, it must be updated in consumer lambda too.

    """

    # Generate unique hash for bucketlocation that can be used as dedup id, direct location can not be used due to 128 char length limit
    mymessageid = get_checksum_for_dedup(key)
    mymessagegroupid = mymessageid  # required
    mymessagededupid = (
        mymessageid  # needed to dedup messages for files under same partition.
    )

    mymessagestring = f"{tablename},{bucket},{key}"
    assert len(mymessagestring.split(",")) == 3
    return mymessagestring, mymessagegroupid, mymessagededupid


def get_bucket_and_key_from_event(event) -> Tuple[str, str]:
    """
    Parse the event object to get the bucket and key.
    """
    if "Records" in event:
        k = urllib.parse.unquote_plus(
            event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
        )
        b = event["Records"][0]["s3"]["bucket"]["name"]
        return b, k
    raise ValueError("event does not contain Records")


def add_missing_ext(passed_bucket, passed_key, missing_ext) -> str:
    """
    Used for situations where incoming file name is not desired as-is and must
    be mutated in some way, such as files from data firehose that are gzipped
    but missing gz extension, which must have .gz extension to be correctly
    read by aws services we use (but name/metadata only, no content/data change).
    This is a mitigation for the best preferred approach: assign a useful file
    name as upsteam as possible (in the case of data firehose, we would prefer
    to add a post processer lambda to add .gz "prior to the put" that would
    trigger addAPart.
    """
    _, ext = os.path.splitext(passed_key)
    if ext != missing_ext:
        copy_source = {"Bucket": passed_bucket, "Key": passed_key}
        key_with_ext = f"{passed_key}{missing_ext}"
        copy_response = s3_client.copy_object(
            Bucket=passed_bucket,
            CopySource=copy_source,
            Key=key_with_ext,  # most important line of function: thefile becomes thefile.gz
        )
        logger.debug(
            f"{copy_source} copied to {key_with_ext} and response was {copy_response}"
        )  # for troubleshooting initial setup only
        # if you uncomment line below, job can throw error about file not found
        # when addapart changes files used by job. we recommend you clean up in separate
        # mechanism if keeping orig copy is undesired, to avoid duplicate file cost.
        # delete_response = s3_client.delete_object(Bucket=passed_bucket, Key=passed_key)  # destroy original
        return key_with_ext
    return passed_key


def get_table_from_key(
    partition_placer_mapping, k
) -> Tuple[Optional[str], Optional[dict]]:
    """
    Determines the appropriate mapping configuration for a given S3 Path
    """
    for tablename, path_patterns in partition_placer_mapping.items():
        path_patterns = path_patterns["path_patterns"]
        s = path_patterns["startswith_or_searchstring"]
        # first try the prefix
        if k.startswith(s):
            return tablename, path_patterns
        # if no hit from start, look across entire key's string
        if s in k:
            return tablename, path_patterns
    return None, None


def lambda_handler(event, context):
    """
    This should be the handler only. Do not put main program logic here.
    """
    try:
        logging.info("initiated...")

        b, k = get_bucket_and_key_from_event(event)
        logging.debug(f"Bucket: {b}, key: {k}")

        tablename, path_patterns = get_table_from_key(PARTITION_PLACER_MAPPING, k)
        logging.debug(f"tablename: {tablename}, path_patterns: {path_patterns}")

        send_indicator = should_i_send(k)

        if send_indicator:
            if path_patterns["ext_to_add"]:
                logging.debug(f'expected file extension: {path_patterns["ext_to_add"]}')
                # we overwrite orig key with key+ext if actual key's ext not match
                # expected ext
                k = add_missing_ext(b, k, path_patterns["ext_to_add"])
                logging.debug(f"key with ext: {k}")

            ret_message, ret_messagegroupid, ret_messagededupid = make_my_message(
                tablename, b, k
            )
            logging.debug(f"message i will send is {ret_message}...")
            send_message(
                themessage=ret_message,
                theid=ret_messagegroupid,
                the_dup_id=ret_messagededupid,
            )
            logger.info(f"message sent to queue...")
            logger.info(f"done.")
        else:
            logger.info(
                f"Given key {k} not eligible for message (if message is desired, you must change key name in s3 or change partition_placer_mapping)"
            )
    except Exception as e:
        raise (e)
