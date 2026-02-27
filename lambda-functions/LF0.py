import json
from datetime import datetime, timezone
import uuid
import os
import boto3

lex_client = boto3.client('lexv2-runtime', region_name='us-east-1')
BOT_ID = os.environ.get('LEX_BOT_ID')
BOT_ALIAS_ID = os.environ.get('LEX_BOT_ALIAS_ID')
LOCALE_ID = os.environ.get('LEX_LOCALE_ID', 'en_US')


def send_to_lex(message_text, session_id):
    try:
        response = lex_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,
            text=message_text
        )

        print(f"Lex full response: {json.dumps(response, default=str)}")

        messages = response.get('messages', [])
        
        if messages:
            response_texts = [msg.get('content', '') for msg in messages]
            return ' '.join(response_texts)
        else:
            return "Sorry, I didn't understand that. Could you try again?"

    except Exception as e:
        print(f"Error communicating with Lex: {str(e)}")
        raise e


def lambda_handler(event, context):
    try:
        if 'body' not in event:
            return error_response(400, "Missing request body")

        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']

        if 'messages' not in body or not body['messages']:
            return error_response(400, "Missing or empty messages array")

        last_message = body['messages'][-1] 
        
        if 'unstructured' in last_message:
            user_text = last_message['unstructured'].get('text', '')
        elif 'text' in last_message:
            user_text = last_message.get('text', '')
        else:
            return error_response(400, "Message has no text content")

        if not user_text or not user_text.strip():
            return error_response(400, "Empty message text")

        print(f"User message: {user_text}")

        session_id = "default-user"

        if 'sessionId' in body:
            session_id = body['sessionId']
        elif event.get('headers', {}).get('x-session-id'):
            session_id = event['headers']['x-session-id']
        elif event.get('requestContext', {}).get('identity', {}).get('sourceIp'):
            source_ip = event['requestContext']['identity']['sourceIp']
            session_id = source_ip.replace('.', '-')

        print(f"Session ID: {session_id}")
        lex_response_text = send_to_lex(user_text, session_id)
        
        print(f"Lex response: {lex_response_text}")
        bot_message = {
            "type": "unstructured",
            "unstructured": {
                "id": str(uuid.uuid4()),
                "text": lex_response_text,
                "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            }
        }

        response_body = {
            "messages": [bot_message]
        }

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,x-session-id",
                "Access-Control-Allow-Methods": "POST, OPTIONS"
            },
            "body": json.dumps(response_body)
        }

    except json.JSONDecodeError:
        return error_response(400, "Invalid JSON in request body")

    except Exception as e:
        print(f"Error: {str(e)}")
        return error_response(500, "Internal server error")


def error_response(status_code, message):
    """Helper to build error responses."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,x-session-id",
            "Access-Control-Allow-Methods": "POST, OPTIONS"
        },
        "body": json.dumps({
            "code": status_code,
            "message": message
        })
    }