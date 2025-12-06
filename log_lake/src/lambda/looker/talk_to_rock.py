import boto3
from botocore.exceptions import ClientError, ValidationError
from botocore.config import Config
from typing import Dict, Any, Tuple, Optional, List
import logging
import prompt_preparation_module as p
import json
import os

# bedrock settings
MAX_RETRIES = 2
BOTO_DEFAULT_TIMEOUT_IN_SEC = 60
BEDROCK_CONFIG = Config(
    region_name="us-east-1",
    connect_timeout=BOTO_DEFAULT_TIMEOUT_IN_SEC * 5,
    read_timeout=BOTO_DEFAULT_TIMEOUT_IN_SEC * 5,
    retries={"max_attempts": MAX_RETRIES, "mode": "standard"},
)
BEDROCK_RUNTIME_CLIENT = boto3.client(
    service_name="bedrock-runtime", config=BEDROCK_CONFIG
)
GUARDRAIL_FOR_SENSITIVE_ID = os.environ.get("GUARDRAIL_ARN")
GUARDRAIL_FOR_SENSITIVE_VER = "1"

# model settings
VERSION = "bedrock-2023-05-31"
MY_MODEL_ID = "global.anthropic.claude-sonnet-4-5-20250929-v1:0"
MAX_TOKENS_FOR_CLAUDE = (
    8000  # higher consumes more context due to conversational accumulation
)
TEMP_FOR_CLAUDE = 0


def prepare_request(
    logger: logging.Logger,
    passed_tools: list,
    db_for_run_query: str,
    tool_for_run_query: str,
) -> dict:
    """
    Initialize a request body with standard settings. We insert messages
    outside this function, because messages are different each conversational
    turn.
    """
    request_body_template = {
        "anthropic_version": VERSION,
        "max_tokens": MAX_TOKENS_FOR_CLAUDE,
        "temperature": TEMP_FOR_CLAUDE,
        "messages": [],
        "tools": passed_tools,
        "system": p.system_prompt_template.format(
            database_for_run_query=db_for_run_query,
            tool_name_for_run_query=tool_for_run_query,
        ),
    }
    return request_body_template


def apply_rails_to_response(logger: logging.Logger, response_content: list) -> list:
    """Check and filter model response content for sensitive data"""
    try:

        filtered_content = []
        for item in response_content:
            if item.get("type") == "text" and item.get("text"):
                text_from_response = item["text"]
                content_for_text = [{"text": {"text": text_from_response}}]
                if isinstance(text_from_response, str):
                    response = BEDROCK_RUNTIME_CLIENT.apply_guardrail(
                        guardrailIdentifier=GUARDRAIL_FOR_SENSITIVE_ID,
                        guardrailVersion=GUARDRAIL_FOR_SENSITIVE_VER,
                        source="OUTPUT",
                        content=content_for_text,  # list of dict
                    )
                    if response["action"] == "GUARDRAIL_INTERVENED":
                        item["text"] = (
                            "Response filtered to protect sensitive information."
                        )
                else:
                    logger.info("text from response not a string")

            filtered_content.append(item)
        return filtered_content
    except ValidationError as err:
        raise ValidationError from err
    except Exception as err:
        raise err


def get_response(logger: logging.Logger, request_body_as_json: str) -> dict:
    """
    Using prepared request body as json formatted string, call boto3 invoke_model.
    response syntax:
    {
    'body': StreamingBody(),
    'contentType': 'string',
    'performanceConfigLatency': 'standard'|'optimized'
    }
    """
    try:
        response = BEDROCK_RUNTIME_CLIENT.invoke_model(
            modelId=MY_MODEL_ID, body=request_body_as_json
        )
        return response
    except ClientError as err:
        error_code = err.response["Error"]["Code"]
        error_message = err.response["Error"]["Message"]
        error_comment = f"Unexpected ClientError: {err=} {type(err)=} {error_code=} {error_message=}"
        if error_code == "ValidationException":
            raise ValueError(error_comment) from err
        raise err
    except Exception as err:
        raise err


def get_response_w_read(
    logger: logging.Logger, request_body_as_json: str
) -> Tuple[bytes, dict]:
    """
    Calls invoke_model via the get_response function, but adds additional
    logic.
    This includes, but is not limited to:
    *reading response as bytes object & json for later use by guardrails
    *applying guardrails to response
    *additional error handling prior
    """
    response_body_as_d = {}
    try:
        response = get_response(logger, request_body_as_json)
        # read immediately to enable guardrail checks before returning
        # make StreamingBody() object a bytes object
        response_body: bytes = response["body"].read()
        if not response_body:
            message = "empty body in response!"
            raise RuntimeError(message)
        # deserialize json obj to py dict
        response_body_as_d = json.loads(response_body)

        # apply guardrails. these must exist prior to run!
        if response_body_as_d.get("content"):
            logger.info(f"validating response...")
            response_body_as_d["content"] = apply_rails_to_response(
                logger, response_body_as_d["content"]
            )
            logger.info(f"response validation successful...")
        return response_body, response_body_as_d
    except (KeyError, json.JSONDecodeError) as e:
        message = f"error parsing response due to key or decode error: {e}"
        logger.info(message)
        raise e
    except Exception as e:
        raise e
