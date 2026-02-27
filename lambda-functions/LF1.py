import json
import datetime
import re
import logging
import os
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

sqs_client = boto3.client('sqs')

def close(session_attributes, intent_name, fulfillment_state, message_content):
    return {
        "sessionState": {
            "sessionAttributes": session_attributes if session_attributes else {},
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": intent_name,
                "state": fulfillment_state
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message_content
            }
        ]
    }


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message_content):
    return {
        "sessionState": {
            "sessionAttributes": session_attributes if session_attributes else {},
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": slot_to_elicit
            },
            "intent": {
                "name": intent_name,
                "slots": slots,
                "state": "InProgress"
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message_content
            }
        ]
    }


def delegate(session_attributes, intent_name, slots):
    return {
        "sessionState": {
            "sessionAttributes": session_attributes if session_attributes else {},
            "dialogAction": {
                "type": "Delegate"
            },
            "intent": {
                "name": intent_name,
                "slots": slots
            }
        }
    }


def get_slot_value(slots, slot_name):
    if slots is None:
        return None
    slot = slots.get(slot_name)
    if slot is None:
        return None
    value = slot.get("value")
    if value is None:
        return None
    return value.get("interpretedValue")

VALID_CUISINES = [
    "chinese", "italian", "mexican", "japanese", "indian",
    "turkish", "spanish"
]

VALID_LOCATIONS = [
    "manhattan", "brooklyn"
]


def is_valid_cuisine(cuisine):
    return cuisine is None or cuisine.lower() in VALID_CUISINES


def is_valid_location(location):
    if location is None:
        return True
    return location.lower().strip() in VALID_LOCATIONS


def is_valid_number_of_people(num):
    if num is None:
        return True
    try:
        return 1 <= int(num) <= 20
    except ValueError:
        return False


def is_valid_dining_time(dining_time):
    if dining_time is None:
        return True
    try:
        if ":" in str(dining_time):
            parts = str(dining_time).split(":")
            return 0 <= int(parts[0]) <= 23 and 0 <= int(parts[1]) <= 59
        return True
    except (ValueError, IndexError):
        return False


def is_valid_email(email):
    if email is None:
        return True
    return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email) is not None


def validate_dining_suggestion(slots):
    location = get_slot_value(slots, "Location")
    cuisine = get_slot_value(slots, "Cuisine")
    dining_time = get_slot_value(slots, "DiningTime")
    number_of_people = get_slot_value(slots, "NumberOfPeople")
    email = get_slot_value(slots, "Email")

    if location is not None and not is_valid_location(location):
        return {"isValid": False, "violatedSlot": "Location",
                "message": f"Sorry, I don't have suggestions for {location}. Try another city?"}

    if cuisine is not None and not is_valid_cuisine(cuisine):
        return {"isValid": False, "violatedSlot": "Cuisine",
                "message": f"{cuisine} isn't a supported cuisine. Choose from: {', '.join(VALID_CUISINES)}."}

    if number_of_people is not None and not is_valid_number_of_people(number_of_people):
        return {"isValid": False, "violatedSlot": "NumberOfPeople",
                "message": "Please enter a valid number of people (1-20)."}

    if dining_time is not None and not is_valid_dining_time(dining_time):
        return {"isValid": False, "violatedSlot": "DiningTime",
                "message": "Please provide a valid time (e.g., 7 pm)."}

    if email is not None and not is_valid_email(email):
        return {"isValid": False, "violatedSlot": "Email",
                "message": "Please provide a valid email address."}

    return {"isValid": True}


def push_to_sqs(location, cuisine, dining_time, number_of_people, email):
    queue_url = os.environ.get('SQS_QUEUE_URL')
    if location.lower() == "manhattan":
        location = "new york"
    if not queue_url:
        logger.error("SQS_QUEUE_URL environment variable not set!")
        raise ValueError("SQS_QUEUE_URL not configured")

    message_body = {
        "Location": location,
        "Cuisine": cuisine,
        "DiningTime": dining_time,
        "NumberOfPeople": number_of_people,
        "Email": email,
        "Timestamp": datetime.datetime.now().isoformat()
    }

    logger.info(f"Sending message to SQS: {json.dumps(message_body)}")

    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message_body),
        MessageAttributes={
            "Cuisine": {
                "StringValue": cuisine,
                "DataType": "String"
            },
            "Location": {
                "StringValue": location,
                "DataType": "String"
            },
            "Email": {
                "StringValue": email,
                "DataType": "String"
            }
        }
    )

    message_id = response.get("MessageId")
    logger.info(f"SQS message sent successfully. MessageId: {message_id}")
    return message_id


def handle_greeting_intent(event):
    session_attributes = event.get("sessionState", {}).get("sessionAttributes", {})
    intent_name = event["sessionState"]["intent"]["name"]
    return close(session_attributes, intent_name, "Fulfilled",
                 "Hi there! How can I help you? You can ask me for dining suggestions.")


def handle_thank_you_intent(event):
    session_attributes = event.get("sessionState", {}).get("sessionAttributes", {})
    intent_name = event["sessionState"]["intent"]["name"]
    return close(session_attributes, intent_name, "Fulfilled",
                 "You're welcome! Have a great day and enjoy your meal!")


def handle_dining_suggestions_intent(event):
    session_attributes = event.get("sessionState", {}).get("sessionAttributes", {})
    intent_name = event["sessionState"]["intent"]["name"]
    slots = event["sessionState"]["intent"].get("slots", {})
    invocation_source = event.get("invocationSource", "")

    logger.debug(f"Invocation source: {invocation_source}")
    logger.debug(f"Slots: {json.dumps(slots, default=str)}")

    if invocation_source == "DialogCodeHook":
        validation_result = validate_dining_suggestion(slots)

        if not validation_result["isValid"]:
            violated_slot = validation_result["violatedSlot"]
            if slots.get(violated_slot):
                slots[violated_slot] = None

            return elicit_slot(
                session_attributes,
                intent_name,
                slots,
                violated_slot,
                validation_result["message"]
            )

        return delegate(session_attributes, intent_name, slots)

    elif invocation_source == "FulfillmentCodeHook":
        # Extract all slots
        location = get_slot_value(slots, "Location")
        cuisine = get_slot_value(slots, "Cuisine")
        dining_time = get_slot_value(slots, "DiningTime")
        number_of_people = get_slot_value(slots, "NumberOfPeople")
        email = get_slot_value(slots, "Email")

        logger.info(
            f"Fulfillment — Location: {location}, Cuisine: {cuisine}, "
            f"Time: {dining_time}, People: {number_of_people}, Email: {email}"
        )

        try:
            message_id = push_to_sqs(
                location=location,
                cuisine=cuisine,
                dining_time=dining_time,
                number_of_people=number_of_people,
                email=email
            )
            logger.info(f"Request queued with MessageId: {message_id}")

            confirmation_message = (
                f"You're all set! I've received your request for "
                f"{cuisine.capitalize()} restaurant suggestions in "
                f"{location} for {number_of_people} people at {dining_time}. "
                f"I will notify you via email at {email} once I have "
                f"the list of suggestions. Expect it shortly!"
            )

            return close(
                session_attributes,
                intent_name,
                "Fulfilled",
                confirmation_message
            )

        except Exception as e:
            logger.error(f"Failed to push to SQS: {str(e)}")

            return close(
                session_attributes,
                intent_name,
                "Failed",
                "I'm sorry, something went wrong while processing your request. "
                "Please try again later."
            )

    return delegate(session_attributes, intent_name, slots)

def lambda_handler(event, context):
    logger.debug(f"Event: {json.dumps(event, default=str)}")

    intent_name = event["sessionState"]["intent"]["name"]

    if intent_name == "GreetingIntent":
        return handle_greeting_intent(event)
    elif intent_name == "ThankYouIntent":
        return handle_thank_you_intent(event)
    elif intent_name == "DiningSuggestionsIntent":
        return handle_dining_suggestions_intent(event)
    else:
        return close({}, intent_name, "Failed",
                     "Sorry, I didn't understand that. Can you try again?")