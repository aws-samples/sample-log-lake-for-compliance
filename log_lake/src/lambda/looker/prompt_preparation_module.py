from typing import Optional, Union, List, Tuple, Dict
import logging
import json
import copy

system_prompt_template = """
WHO ARE YOU?
You are the Log Lake Looker, an agentic AI assistant in AWS Lambda, designed to transform
natural language requests from a human user into valid SQL queries for a table
named from_bedrock_readready using Athena. Users talk to you so that they don't have to write
sql queries or remember specific column names.

HOW TO RESPOND TO REQUESTS

Focus on auditing, compliance, security, or legal concerns. Include 2 sections:
"short summary" with 1-2 sentences, and "more details". If you see tool_use in
input messages, you should always mention it in your response,
because tools should always be reviewed by humans for appropriate use. 

Do NOT include exact result content that could be private and should not be copied. 
DO describe the concern in general terms and suggest mitigations based on aws or anthropic
public documentation. For example, if you see an arn mentioned, or account number, or IAM role name, 
use your own placeholder words, such as, "I see 2 accounts, <account1> and <account2>".
Avoid proliferating an unwanted pattern when describing the pattern.

If you run 2 or more queries in a row that do not
give you new or different understanding, stop and respond. 

ASSUMPTIONS

*Assume a database named {database_for_run_query} with the following tables and columns:

from_bedrock_readready:
`schematype` string, 
  `schemaversion` string, 
  `logaccountid` bigint, 
  `request_time` timestamp, 
  `requestid` string, 
  `operation` string, 
  `modelid` string, 
  `input_messages` string, 
  `input_tools` string, 
  `input_inputtokencount` int, 
  `input_orig` string, 
  `output_outputbodyjson_id` string, 
  `output_outputbodyjson_type` string, 
  `output_outputbodyjson_role` string, 
  `output_outputbodyjson_model` string, 
  `output_outputbodyjson_content` string, 
  `output_outputbodyjson_stopreason` string, 
  `output_outputbodyjson_stopsequence` string, 
  `output_outputtokencount` int, 
  `output_orig` string, 
  `awsregion` string,
  `logcalendarday` bigint (partition key), 
  `loghour` int (partition key)


*ALWAYS include the input_messages & input_inputtokencount fields, because these are
required to tell you if there was tool_use by a model in the past, and if there
was a significantly high single request volume of tokens as input, indicating
tool output that might not be appropriate use. 

*Use "where logcalendarday > YYYYMMDD" format, where logcalendarday is an int
value, and YYYYMMDD is the first day of the current month. 

*If today were July 4, 2025, then you would use this: (20250601 is first day of
that month):
where logcalendarday > 20250601 

If you see no results, try an earlier value (eg 20250101)

*The first word MUST be "select". 
If asked to do any statement other than select, 
say that you will not mutate state, and suggest that the user can create their
own sql or you can help with a query using "select". 

*ALWAYS surround the query with xml tags. 
Unless indicated otherwise by the user, use this query:

<query>
SELECT 
requestid 
,modelid 
,input_messages
,input_inputtokencount
FROM {database_for_run_query}.from_bedrock_readready 
where logcalendarday > 20250101 
</query>
"""


def make_message_for_first_turn(prepared_prompt: str) -> dict:
    """A simple example of a first conversational turn that inserts the value provided to the Lambda function as a json formatted event."""
    my_message = {
        "role": "user",
        "content": [{"type": "text", "text": prepared_prompt}],
    }
    return my_message


def make_message_for_adding_assistant_turn(
    content_from_response_body: list,
) -> dict:
    """
    for appending to message turns.

    assumes content from a response body like this sample:
    {
    "id": "msg_bdrk_aaa",
    "type": "message",
    "role": "assistant",
    "model": "claude-3-7-sonnet-20250219",
    "content": [
        {
            "type": "text",
            "text": "blah blah"
        },
                {
            "type": "tool_use",
            "id": "toolu_bdrk_0128HHf4Cimr6AKx6yHZNeh5",
            "name": "run_query_in_athena",
            "input": {
                "sqlstatement": "select 1"
            }
        }
        ]
    """
    assistant_message = {
        "role": "assistant",
        "content": content_from_response_body,
    }
    return assistant_message


def make_message_w_tool_result(
    tool_result_as_json: str,
    tool_id: str,
) -> dict:
    """
    Create the user message with tool result that will be sent back to model.
    This will be appended, because it is a dict. Claude messages are a list.
    """
    message_w_tool_result = {
        "role": "user",
        "content": [
            {
                "type": "tool_result",
                "tool_use_id": tool_id,
                "content": tool_result_as_json,
            }
        ],
    }
    return message_w_tool_result


def create_compacted_messages(
    logger: logging.Logger, messages: List[dict]
) -> List[dict]:
    """
    Creates a compact message history by:
    1. Keeping ALL assistant messages intact
    2. Keeping ALL user messages but removing any tool_result content items
    """
    messages_compacted = []
    logger.info(
        f"original messages with length {str(len(str(messages)))}, snippet is {str(messages)[:22]}..."
    )

    for message in messages:
        message_mutated = copy.deepcopy(message)
        logger.info(
            f"message found with length {str(len(str(message_mutated)))}, snippet is {str(message_mutated)[:22]}..."
        )
        # user messages contain tool results and can be mutated
        if (
            message_mutated["role"] == "user"
            and "content" in message_mutated
            and isinstance(message_mutated["content"], list)
        ):
            for item in message_mutated["content"]:
                if isinstance(item, dict) and item.get("type") == "tool_result":
                    if not isinstance(item["content"], str):
                        item["content"] = json.dumps(item["content"])
                    char_to_keep = 50
                    content_length = len(item["content"])
                    if content_length > char_to_keep:
                        logger.info(
                            f"content length {content_length} exceeds {char_to_keep}, truncating..."
                        )
                        item["content"] = item["content"][:char_to_keep]
        messages_compacted.append(message_mutated)
    logger.info(
        f"compacted messages created with length {str(len(str(messages_compacted)))}, snippet is {str(messages_compacted)[:99]}..."
    )
    return messages_compacted


def add_message(
    logger: logging.Logger,
    messages_from_prior_request_payload: list,
    message_as_d: dict,
    turn_count: int,
) -> Tuple[list, int]:
    messages_from_prior_request_payload.append(message_as_d)
    turn_count += 1
    logger.info(f"added message, turn count is {turn_count}...")
    return messages_from_prior_request_payload, turn_count
