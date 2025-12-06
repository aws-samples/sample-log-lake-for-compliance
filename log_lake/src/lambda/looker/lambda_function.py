"""
This function implements Log Lake Looker, an agent that runs tools in a loop.
Looker can be used by an auditor or security investigator to query Log
Lake's tables using conversational text as input, as part of human-in-the-loop
 review to assess appropriate use of tools.

Looker can be expanded to use more tools or more tables (such as adding
from_cloudtrail_readready & from_cloudwatch_readready to system prompt).
See Log Lake blog post for more details.
"""

from typing import Dict, Any, Tuple, Optional, List
import logging
import json
import os
from botocore.exceptions import ClientError
import re
import prompt_preparation_module as p
import query_manager_module as qry
import talk_to_rock as llm
import boto3
import os

# prompt settings
KEY_FROM_EVENT = "prompt_from_user"


# tool settings (could be agnostic of model or database)
TOOL_FOR_RUN_QUERY = "run_query_in_athena"
DATABASE_FOR_RUN_QUERY = os.environ.get("GLUE_DATABASE_NAME", "loglakeblog")


def create_tools() -> list:
    """
    Define tools available to LLM, so it can choose. Can be expanded later for more agent autonomy.
    """
    tools = [
        {
            "name": TOOL_FOR_RUN_QUERY,
            "description": "Execute prepared sql statement in Athena",
            "input_schema": {
                "type": "object",
                "properties": {
                    "sqlstatement": {
                        "type": "string",
                        "description": "The sql statement to run, in valid syntax for AWS Athena.",
                    }
                },
                "required": ["sqlstatement"],
            },
        },
    ]
    return tools


def is_sqlstatement_readonly(query: str) -> bool:
    """
    Check to make sure statement is a read-only query that does not start with
    update, create, drop, delete, alter.
    """
    # normalize the string for whitespace and case
    query_lower = query.lower().strip()

    # Must start with SELECT
    if query_lower.startswith("select"):
        return True
    return False


def get_query_from_response(
    logger: logging.Logger, tool_input: dict, text_content: str
) -> str:
    """Extract the query from tool input if possible, or from text content
    returned from previous assistant response, to get a clean query ready to
    run.
    """
    the_query = None
    if tool_input.get("sqlstatement"):
        the_query = tool_input.get("sqlstatement")
        logger.info(f"query extracted from tool input...")
    else:
        logger.info(
            "sql statement not found from tool input, so will try parsing text content w/regex..."
        )
        mypattern = r"<query>\s*(.*?)\s*</query>"  # we look for tags from system prompt
        m = re.search(mypattern, text_content, re.DOTALL | re.IGNORECASE)
        if m:
            the_query = m.group(1).strip()

    if the_query:
        assert is_sqlstatement_readonly(the_query)
        logger.info("sql statement validated...")
        return the_query
    else:
        raise ValueError(
            "no query found in either tool input or text content, check both and retry..."
        )


def log_step(logger: logging.Logger, step: int, comment: str) -> None:
    """Log a standardized step message as JSON"""
    myjson = {"step": step, "comment": comment}
    message = json.dumps(myjson)
    logger.info(message)


class StepLogger:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.step = 0

    def log_w_autostep(self, comment: str):
        self.step += 1
        log_step(self.logger, self.step, comment)


def inspect_messages(logger: logging.Logger, messages: list) -> dict:
    """
    For troubleshooting and logging, return a simple summary of conversational
    messages.
    """
    inspection = {
        "messages": len(messages),
        "messages_useronly": 0,
        "messages_assistantonly": 0,
        "tool_results": 0,
        "assistant_details": [],
    }

    for i, message in enumerate(messages):
        role = message.get("role", "?")

        if role == "user":
            inspection["messages_useronly"] += 1
            # Check if this user message contains tool results
            if "content" in message:
                for content_item in message["content"]:
                    if content_item.get("type") == "tool_result":
                        inspection["tool_results"] += 1
        elif role == "assistant":
            inspection["messages_assistantonly"] += 1

            # Analyze assistant message content
            assistant_detail = {
                "message_index": i,
                "has_text": False,
                "has_tool_use": False,
                "tool_names": [],
            }

            if "content" in message:
                for content_item in message["content"]:
                    if content_item.get("type") == "text":
                        assistant_detail["has_text"] = True
                    elif content_item.get("type") == "tool_use":
                        assistant_detail["has_tool_use"] = True
                        tool_name = content_item.get("name")
                        if tool_name:
                            assistant_detail["tool_names"].append(tool_name)

            inspection["assistant_details"].append(assistant_detail)

    logger.info(f"Message inspection: {inspection}")
    return inspection


def loop_logic_for_response(
    logger: logging.Logger,
    turn_count: int,
    response_body_as_d: dict,
    request_body_as_d: dict,
    final_response: dict,
):
    """
    This is the logic that runs on every model response, which makes Looker
    agentic (because it runs in a loop, and the llm can choose the next action
    with autonomy).
    """
    my_steplogger = StepLogger(logger)
    my_steplogger.log_w_autostep("in loop for processing response from model")
    if not (
        response_body_as_d.get("content") and len(response_body_as_d["content"]) > 0
    ):
        logger.info(f"no content items in response, ending conversation")
        response_body_as_d = {}
        return turn_count, response_body_as_d, request_body_as_d, final_response

    my_steplogger.log_w_autostep("look for tool use in any content items")
    tool_use_content = None
    text_content = ""
    for content_item in response_body_as_d["content"]:
        if content_item.get("type") == "tool_use":
            tool_use_content = content_item
        elif content_item.get("type") == "text":
            text_content += content_item.get("text", "")

    my_steplogger.log_w_autostep("was tool requested by model?")
    if not tool_use_content:
        logger.info(f"no tool use in turn {turn_count}, ending conversation loop")
        response_body_as_d = {}
        return turn_count, response_body_as_d, request_body_as_d, final_response

    my_steplogger.log_w_autostep("tool use found...")
    tool_name = tool_use_content.get("name")
    tool_input = tool_use_content.get("input", {})
    tool_id = tool_use_content.get("id")

    logger.info(f"Tool requested: {tool_name}")
    logger.info(f"Tool input: {tool_input}")
    logger.info(f"tool use id {tool_id}")

    result_from_tool = []
    # logic for each tool
    if tool_name == TOOL_FOR_RUN_QUERY:
        my_steplogger.log_w_autostep("executing tool to run query in athena...")
        query_ready_to_run = get_query_from_response(logger, tool_input, text_content)
        logger.info(f"query: {query_ready_to_run}")
        query_execution_id = qry.run_athena_query(
            logger, query_ready_to_run, DATABASE_FOR_RUN_QUERY
        )
        logger.info(f"query started as {query_execution_id}...")
        qry.check_query_status(logger, query_execution_id)
        result_from_tool = qry.get_query_results(logger, query_execution_id)
        logger.info(f"query returned {len(result_from_tool)} results...")
    else:
        logger.info(f"tool requested but not found: {tool_name}...")

    my_steplogger.log_w_autostep("checking for tool result")
    if result_from_tool:
        tool_result_as_json = json.dumps(result_from_tool)
        try:
            json.loads(tool_result_as_json)
        except json.JSONDecodeError as e:
            logger.info(f"error decoding json from tool result")
            raise e
        message = p.make_message_w_tool_result(
            tool_result_as_json,
            tool_id,
        )
        request_body_as_d["messages"], turn_count = p.add_message(
            logger, request_body_as_d["messages"], message, turn_count
        )
        inspect_messages(logger, request_body_as_d["messages"])

        request_body_as_json = json.dumps(request_body_as_d)

        my_steplogger.log_w_autostep("sending request w/tool result back to model...")
        response_body_as_d = {}  # default
        response_body, response_body_as_d = llm.get_response_w_read(
            logger, request_body_as_json
        )

        try:
            # ensure response has body as redundant defense
            if not response_body:
                return turn_count, response_body_as_d, request_body_as_d, final_response
            response_using_toolresult_as_d = json.loads(response_body)
        except (KeyError, json.JSONDecodeError) as e:
            logger.info(f"key or decode error when parsing response: {e}")
            return turn_count, response_body_as_d, request_body_as_d, final_response

        my_steplogger.log_w_autostep("was there a response to a tool result?")
        if response_using_toolresult_as_d.get("content"):
            assistant_message = p.make_message_for_adding_assistant_turn(
                response_using_toolresult_as_d["content"]
            )
            request_body_as_d["messages"], turn_count = p.add_message(
                logger, request_body_as_d["messages"], assistant_message, turn_count
            )
            inspect_messages(logger, request_body_as_d["messages"])
            final_response = extract_text_as_final_response(logger, response_body_as_d)
            my_steplogger.log_w_autostep(
                f"final_response for this step only: {final_response}"
            )

            my_steplogger.log_w_autostep(
                "remove tool_result content to manage context size"
            )
            request_body_as_d["messages"] = p.create_compacted_messages(
                logger, request_body_as_d["messages"]
            )
            response_body_as_d = response_using_toolresult_as_d
            inspect_messages(logger, request_body_as_d["messages"])

            return turn_count, response_body_as_d, request_body_as_d, final_response


def set_logging(passed_logger_name) -> logging.Logger:
    """
    Specify logging details, such as level
    """

    stream_handler = logging.StreamHandler()
    # stream_handler.setLevel(logging.INFO)

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


def extract_text_as_final_response(
    logger: logging.Logger, response_body_as_d: dict
) -> str:
    """
    From a response body as dict, extract the text if an item has key "content"
    """
    final_response = ""
    for content_item in response_body_as_d["content"]:
        if content_item.get("type") == "text":
            final_response = content_item.get("text")
    return final_response


def lambda_handler(event, context):
    """
    Main Lambda handler function that implements a conversational loop with llm
    (Claude).
    1. parse user input as json event as user message
    2. invoke model and get assistant response from llm
    3. enter loop to process response from assistant. This is where Looker has
    autonomy: running tools in a loop and choosing the next action without
    human input.
    """
    logger = set_logging("lambda_function")
    logger.info("Lambda function started")
    logger.info(f"Event received: {json.dumps(event, indent=2)}")

    # extract prompt from event
    first_prompt_from_user = event.get(KEY_FROM_EVENT)
    if not first_prompt_from_user:
        message = "missing prompt from user, aborting..."
        raise ValueError(message)

    try:
        tools = create_tools()
        logger.info(f"Tool(s) created: {tools}")
        turn_count = 0
        loop_count = 0
        loop_limit = 5

        # initialize payload, then add message
        request_body_as_d = llm.prepare_request(
            logger=logger,
            passed_tools=tools,
            db_for_run_query=DATABASE_FOR_RUN_QUERY,
            tool_for_run_query=TOOL_FOR_RUN_QUERY,
        )

        # 0 first user message as first conversational turn
        message_for_first_turn = p.make_message_for_first_turn(first_prompt_from_user)
        request_body_as_d["messages"], turn_count = p.add_message(
            logger, request_body_as_d["messages"], message_for_first_turn, turn_count
        )
        inspect_messages(logger, request_body_as_d["messages"])

        # 1 prepare request as json
        request_body_as_json = json.dumps(request_body_as_d)

        response_body, response_body_as_d = llm.get_response_w_read(
            logger, request_body_as_json
        )
        # response body from first turn
        assistant_message = p.make_message_for_adding_assistant_turn(
            response_body_as_d["content"]
        )
        request_body_as_d["messages"], turn_count = p.add_message(
            logger, request_body_as_d["messages"], assistant_message, turn_count
        )
        inspect_messages(logger, request_body_as_d["messages"])
        final_response = extract_text_as_final_response(logger, response_body_as_d)

        # the assistant response parsing loop
        while loop_count < loop_limit and response_body_as_d:
            message = f"in loop {loop_count}, turn {turn_count}..."
            turn_count, response_body_as_d, request_body_as_d, final_response = (
                loop_logic_for_response(
                    logger,
                    turn_count,
                    response_body_as_d,
                    request_body_as_d,
                    final_response,
                )
            )
            loop_count += 1
            continue
        message = f"Lambda executed successfully with {turn_count} conversation turns & {loop_count} assistant response loops"
        logger.info(message)
        logger.info(f"final_response after all loops: {final_response}")
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": message,
                    "response": final_response,
                }
            ),
        }
    except AssertionError as e:
        message = f"assertion error: {e}"
        raise AssertionError from e
    except ClientError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "AWS Client Error", "message": str(e)}),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Error", "message": str(e)}),
        }
