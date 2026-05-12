"""
Lambda 1: SMA (SIP Media Application) Handler
================================================
Triggered by Amazon Chime SDK Voice Connector on every call lifecycle event.

Responsibilities:
  - Answer the inbound call
  - Start Kinesis Data Stream media streaming so audio flows to KDS
  - Store call session metadata in DynamoDB for the Audio Bridge to use
  - Handle audio playback requests from the Audio Bridge
  - Tear down session on call end

Environment variables:
  SESSIONS_TABLE         – DynamoDB table name for active call sessions
  KINESIS_STREAM_ARN     – ARN of the Kinesis Data Stream receiving raw audio
  CHIME_SIP_MEDIA_APP_ID – Chime SIP Media Application ID
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo = boto3.client("dynamodb")

SESSIONS_TABLE     = os.environ["SESSIONS_TABLE"]
KINESIS_STREAM_ARN = os.environ["KINESIS_STREAM_ARN"]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def handler(event, context):
    logger.info("SMA event: %s", json.dumps(event))

    invocation_type = event.get("InvocationEventType")
    call_details    = event.get("CallDetails", {})
    transaction_id  = call_details.get("TransactionId")
    participants    = call_details.get("Participants", [{}])
    call_id         = participants[0].get("CallId") if participants else None

    dispatch = {
        "NEW_INBOUND_CALL":       handle_new_call,
        "ACTION_SUCCESSFUL":      handle_action_successful,
        "ACTION_FAILED":          handle_action_failed,
        "CALL_UPDATE_REQUESTED":  handle_call_update_requested,
        "HANGUP":                 handle_hangup,
    }

    handler_fn = dispatch.get(invocation_type)
    if handler_fn:
        return handler_fn(event, transaction_id, call_id, call_details)

    logger.warning("Unhandled event type: %s", invocation_type)
    return {"SchemaVersion": "1.0", "Actions": []}


# ---------------------------------------------------------------------------
# NEW_INBOUND_CALL – answer and start media streaming
# ---------------------------------------------------------------------------

def handle_new_call(event, transaction_id, call_id, call_details):
    caller_number = call_details.get("Participants", [{}])[0].get("From", "unknown")

    # Persist session so Audio Bridge Lambda can look up call context
    dynamo.put_item(
        TableName=SESSIONS_TABLE,
        Item={
            "transactionId": {"S": transaction_id},
            "callId":        {"S": call_id},
            "status":        {"S": "ACTIVE"},
            "startedAt":     {"S": datetime.now(timezone.utc).isoformat()},
            "callerNumber":  {"S": caller_number},
        },
    )
    logger.info("Session created for transactionId=%s callId=%s", transaction_id, call_id)

    return {
        "SchemaVersion": "1.0",
        "Actions": [
            # 1. Answer the call
            {
                "Type": "Answer",
                "Parameters": {}
            },
            # 2. Brief greeting while the streaming pipeline spins up
            {
                "Type": "Speak",
                "Parameters": {
                    "CallId":       call_id,
                    "Text":         "Please hold while we connect you to our assistant.",
                    "Engine":       "neural",
                    "LanguageCode": "en-US",
                    "VoiceId":      "Joanna",
                },
            },
            # 3. Start bidirectional audio streaming → Kinesis video Stream
            # # if we were to write into kinesis data stream instead of KVS
            # {
            #     "Type": "StartMediaStreamingPipeline",
            #     "Parameters": {
            #         "Streams": [
            #             {
            #                 "Type": "MixedAudio",
            #                 "Destination": {
            #                     "Type":        "KinesisDataStream",
            #                     "ResourceArn": KINESIS_STREAM_ARN,
            #                 },
            #             }
            #         ],
            #         "MediaEncoding":         "pcm",  # 16-bit PCM
            #         "MediaSampleRateHertz":  8000,   # 8 kHz telephony standard
            #     },
            # },
            {
                "Type": "StartMediaStreamingPipeline",
                "Parameters": {
                    "Streams": [
                        {
                            "Type": "MixedAudio",
                            "Destination": {
                                "Type":        "KinesisVideoStream",
                                "ResourceArn": KINESIS_STREAM_ARN,
                            },
                        }
                    ],
                    "MediaEncoding":         "pcm",  # 16-bit PCM
                    "MediaSampleRateHertz":  8000,   # 8 kHz telephony standard
                },
            },
        ],
    }


# ---------------------------------------------------------------------------
# ACTION_SUCCESSFUL
# ---------------------------------------------------------------------------

def handle_action_successful(event, transaction_id, call_id, call_details):
    action_type = event.get("ActionData", {}).get("Type")

    if action_type == "StartMediaStreamingPipeline":
        logger.info("Media streaming pipeline started successfully for callId=%s", call_id)

    # Streaming is live – Audio Bridge Lambda takes over via KDS.
    return {"SchemaVersion": "1.0", "Actions": []}


# ---------------------------------------------------------------------------
# ACTION_FAILED
# ---------------------------------------------------------------------------

def handle_action_failed(event, transaction_id, call_id, call_details):
    logger.error("Action failed: %s", json.dumps(event.get("ActionData", {})))
    return _speak_and_hangup(call_id, "We encountered an error. Goodbye.")


# ---------------------------------------------------------------------------
# CALL_UPDATE_REQUESTED
# Triggered when Audio Bridge calls UpdateSipMediaApplicationCall.
# The Arguments field carries the action instruction from the bridge.
#
# Supported actions:
#   PLAY_S3_AUDIO – play a WAV/PCM file from S3 into the call
#   SPEAK_TEXT    – synthesise speech via Amazon Polly (no S3 needed)
#   HANGUP        – terminate the call gracefully
# ---------------------------------------------------------------------------

def handle_call_update_requested(event, transaction_id, call_id, call_details):
    args   = event.get("ActionData", {}).get("Parameters", {}).get("Arguments", {})
    action = args.get("action")

    logger.info("CALL_UPDATE_REQUESTED action=%s callId=%s", action, call_id)

    if action == "PLAY_S3_AUDIO":
        # s3Uri format: s3://bucket-name/path/to/file.wav
        s3_uri            = args.get("s3Uri", "")
        path_part         = s3_uri.replace("s3://", "")
        bucket, *key_parts = path_part.split("/")
        key               = "/".join(key_parts)

        return {
            "SchemaVersion": "1.0",
            "Actions": [
                {
                    "Type": "PlayAudio",
                    "Parameters": {
                        "CallId":         call_id,
                        "ParticipantTag": "LEG-A",
                        "AudioSource": {
                            "Type":       "S3",
                            "BucketName": bucket,
                            "Key":        key,
                        },
                    },
                }
            ],
        }

    if action == "SPEAK_TEXT":
        return {
            "SchemaVersion": "1.0",
            "Actions": [
                {
                    "Type": "Speak",
                    "Parameters": {
                        "CallId":       call_id,
                        "Text":         args.get("text", ""),
                        "Engine":       "neural",
                        "LanguageCode": "en-US",
                        "VoiceId":      "Joanna",
                    },
                }
            ],
        }

    if action == "HANGUP":
        farewell = args.get("farewellMessage", "Thank you for calling. Goodbye.")
        return _speak_and_hangup(call_id, farewell)

    logger.warning("Unknown CALL_UPDATE_REQUESTED action: %s", action)
    return {"SchemaVersion": "1.0", "Actions": []}


# ---------------------------------------------------------------------------
# HANGUP – caller disconnected; clean up session
# ---------------------------------------------------------------------------

def handle_hangup(event, transaction_id, call_id, call_details):
    _cleanup_session(transaction_id)
    return {"SchemaVersion": "1.0", "Actions": []}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cleanup_session(transaction_id):
    try:
        dynamo.delete_item(
            TableName=SESSIONS_TABLE,
            Key={"transactionId": {"S": transaction_id}},
        )
        logger.info("Session cleaned up: %s", transaction_id)
    except Exception as exc:
        logger.error("Failed to clean up session %s: %s", transaction_id, exc)


def _speak_and_hangup(call_id, message):
    return {
        "SchemaVersion": "1.0",
        "Actions": [
            {
                "Type": "Speak",
                "Parameters": {
                    "CallId":       call_id,
                    "Text":         message,
                    "Engine":       "neural",
                    "LanguageCode": "en-US",
                    "VoiceId":      "Joanna",
                },
            },
            {
                "Type": "Hangup",
                "Parameters": {
                    "SipResponseCode": "0",
                    "CallId":          call_id,
                },
            },
        ],
    }