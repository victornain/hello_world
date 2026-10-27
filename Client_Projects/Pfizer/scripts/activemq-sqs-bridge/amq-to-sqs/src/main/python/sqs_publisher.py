from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


def build_client(region: str, endpoint_url: str | None):
    options = {"region_name": region, "config": Config(connect_timeout=10, read_timeout=60)}
    if endpoint_url:
        options["endpoint_url"] = endpoint_url
    return boto3.client("sqs", **options)


def publish(sqs, options: argparse.Namespace, request: dict) -> dict:
    body = request["body"]
    if not body:
        return {"ok": False, "error_code": "EmptyMessageBody"}
    if len(body.encode("utf-8")) > options.max_body_bytes:
        return {"ok": False, "error_code": "MessageTooLarge"}

    attributes = {}
    for name, value in request.get("message_attributes", {}).items():
        attribute = {"DataType": value["DataType"]}
        if "StringValue" in value:
            attribute["StringValue"] = value["StringValue"]
        elif "BinaryValueBase64" in value:
            attribute["BinaryValue"] = base64.b64decode(value["BinaryValueBase64"])
        attributes[name] = attribute

    arguments = {
        "QueueUrl": options.queue_url,
        "MessageBody": body,
        "MessageAttributes": attributes,
    }
    if options.queue_url.endswith(".fifo"):
        key = f"{options.site}|{request['jms_message_id']}".encode("utf-8")
        arguments["MessageGroupId"] = options.site
        arguments["MessageDeduplicationId"] = hashlib.sha256(key).hexdigest()

    try:
        response = sqs.send_message(**arguments)
        return {"ok": True, "message_id": response["MessageId"]}
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "SQSClientError")
        return {"ok": False, "error_code": code}
    except BotoCoreError as error:
        return {"ok": False, "error_code": type(error).__name__}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", required=True)
    parser.add_argument("--queue-url", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--max-body-bytes", type=int, required=True)
    parser.add_argument("--endpoint-url")
    options = parser.parse_args()
    sqs = build_client(options.region, options.endpoint_url)

    for line in sys.stdin:
        try:
            request = json.loads(line)
            if request.get("shutdown"):
                break
            response = publish(sqs, options, request)
        except Exception as error:
            response = {"ok": False, "error_code": type(error).__name__}
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()