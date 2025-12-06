"""
All functionality specific to the sql engine that interacts
with the metastore should be managed here.
We use Athena as the sql engine to interact with the metastore in
the Glue catalog.

Validation of inputs or outputs from user or assistant (model)
should not be contained here.
"""

import os
import logging
import boto3
import time
from typing import Dict, Any, Tuple, Optional, List
from botocore.exceptions import ClientError
import os


# athena settings
SLEEP_SECONDS_FOR_QUERY = 2
ATHENA_CLIENT = boto3.client("athena")
ATHENA_OUTPUT_BUCKET = os.environ.get("ATHENA_OUTPUT_BUCKET")
ATHENA_OUTPUT_LOCATION = f"s3://{ATHENA_OUTPUT_BUCKET}/"
MAXROWS_PER_REQ = 20  # affects context window
QUERY_STATUS_TIMEOUT = 120
MAXROWS = 5


def check_query_status(logger: logging.Logger, passed_query_execution_id: str):
    start_time = time.time()
    timeout_seconds = QUERY_STATUS_TIMEOUT

    while time.time() - start_time < timeout_seconds:
        time.sleep(SLEEP_SECONDS_FOR_QUERY)
        logger.info(f"sleep done, will get query execution status next...")
        query_status = ATHENA_CLIENT.get_query_execution(
            QueryExecutionId=passed_query_execution_id
        )
        query_execution_status = query_status["QueryExecution"]["Status"]["State"]

        if query_execution_status == "FAILED" or query_execution_status == "CANCELLED":
            raise Exception(f"{query_execution_status=}")
        elif query_execution_status == "SUCCEEDED":
            logger.info(f"{query_execution_status=}")
            return
        else:
            logger.info(f"{query_execution_status=}")
    raise Exception(f"query timed out after {timeout_seconds}")


def run_athena_query(logger: logging.Logger, query: str, db_for_run_query: str) -> str:
    """
    Run a query in Athena and return the query execution ID.

    If SSE_KMS encryption option is used, then the api call looks like this:
    start_query_response = athenaClient.start_query_execution(
        QueryString=passed_statement,
        QueryExecutionContext={"Database": database_in_athena},
        ResultConfiguration={
            "OutputLocation": "s3://" + storage_for_queries + "/",
            "EncryptionConfiguration": {
                "EncryptionOption": encryptopt,
                "KmsKey": key_for_storage_for_queries,
            },
        },
    )
    """

    try:
        response = ATHENA_CLIENT.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": db_for_run_query},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        )

        query_execution_id = response["QueryExecutionId"]
        logger.info(f"Started Athena query with execution ID: {query_execution_id}")

        return query_execution_id

    except ClientError as e:
        raise e


def get_query_results(logger: logging.Logger, passed_query_execution_id: str) -> List:
    """returns array of dicts, 1 dict for each page of results from api"""
    rows_from_queries = []
    max_total_rows = MAXROWS
    next_token = "?"
    response = None
    params = {
        "QueryExecutionId": passed_query_execution_id,
        "MaxResults": MAXROWS_PER_REQ,
    }
    max_requests = 1
    if MAXROWS_PER_REQ < max_total_rows:
        max_requests = max_total_rows / MAXROWS_PER_REQ
    logger.info(
        f"max total rows is {max_total_rows}, which means {max_requests} requests of size {MAXROWS_PER_REQ}"
    )
    request_count = 0
    while next_token and request_count < max_requests:
        request_count += 1
        if next_token == "?":
            next_token = None
        if next_token:
            logger.info(f"nexttoken found: {next_token}")
            params["NextToken"] = next_token
        response = ATHENA_CLIENT.get_query_results(**params)
        try:
            rows_from_query = response["ResultSet"]["Rows"]
            logger.info(
                f"query results for query {passed_query_execution_id} has {len(rows_from_query)} rows..."
            )
        except KeyError as e:
            message = f"rows not found: {e}"
            logger.info(message)
            raise ValueError from e
        rows_from_queries.extend(rows_from_query)
        next_token = response.get("NextToken")
        logger.info(f"nexttoken is {next_token}")
        if next_token:
            logger.info(f"nexttoken {next_token}, continuing...")
        else:
            # short circuit by artificially setting token to nothing, breaking loop
            next_token = None
    return rows_from_queries
