import os
import sys
from collections import defaultdict
from common.protocol import protocol
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'birthday_dictionary_queue')
GATEWAY_REQUEST_QUEUE = os.environ.get('GATEWAY_REQUEST_QUEUE', 'birthday_dictionary_client_request_queue')
GATEWAY_CLIENT_DATA_QUEUE = os.environ.get('GATEWAY_CLIENT_DATA_QUEUE', 'gateway_client_data_queue')
QUERY4_ANSWER_QUEUE = os.environ.get('QUERY4_ANSWER_QUEUE', 'query4_answer_queue')

def listen_for_top_users():
    messages = []
    user_ids = set()
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, RECEIVER_QUEUE)
    print(f"[birthday_dictionary] Listening for top users on queue: {RECEIVER_QUEUE}")

    def on_message_callback(message: bytes):
        msg_decoded = protocol.decode_result_message(message)
        messages.append(msg_decoded)
        for user in msg_decoded.get('top_users', []):
            user_ids.add(user['user_id'])

        if msg_decoded.get('type') == 'end':
            print("[birthday_dictionary] Received end message, stopping top user collection.")
            queue.stop_consuming()

    try:
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[birthday_dictionary] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[birthday_dictionary] Message error in middleware.", file=sys.stderr)
    finally:
        queue.close()
    return messages, user_ids

def request_client_data():
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_REQUEST_QUEUE)
    request = {'request': 'all_clients'}
    message = protocol.encode_client_request_message(request)
    queue.send(message)
    print(f"[birthday_dictionary] Requested all client data from gateway.")
    queue.close()

def listen_for_client_data(user_ids):

    client_birthdays = {}
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_CLIENT_DATA_QUEUE)
    print(f"[birthday_dictionary] Listening for client data on queue: {GATEWAY_CLIENT_DATA_QUEUE}")

    def on_message_callback(message: bytes):
        client_data = protocol.decode_client_data_message(message)
        client_id = client_data.get('client_id')
        birthday = client_data.get('birthday')
        if client_id in user_ids:
            client_birthdays[client_id] = birthday
        if client_data.get('type') == 'end':
            print("[birthday_dictionary] Received end message, stopping client data collection.")
            queue.stop_consuming()

    try:
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[birthday_dictionary] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[birthday_dictionary] Message error in middleware.", file=sys.stderr)
    finally:
        queue.close()
    return client_birthdays

def append_birthdays_to_messages(messages, client_birthdays):
    enriched_messages = []
    for msg in messages:
        enriched_top_users = []
        for user in msg.get('top_users', []):
            user_id = user['user_id']
            user_with_birthday = dict(user)
            user_with_birthday['birthday'] = client_birthdays.get(user_id)
            enriched_top_users.append(user_with_birthday)
        enriched_msg = dict(msg)
        enriched_msg['top_users'] = enriched_top_users
        enriched_messages.append(enriched_msg)
    return enriched_messages

def send_enriched_messages_to_gateway(enriched_messages):
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, QUERY4_ANSWER_QUEUE)
    for msg in enriched_messages:
        message = protocol.encode_result_message(msg)
        queue.send(message)
        print(f"[birthday_dictionary] Sent enriched message to gateway: {msg}")
    queue.close()

def main():
    messages, user_ids = listen_for_top_users()
    if not user_ids:
        print("[birthday_dictionary] No user IDs received, exiting.")
        return
    request_client_data()
    client_birthdays = listen_for_client_data(user_ids)
    enriched_messages = append_birthdays_to_messages(messages, client_birthdays)
    send_enriched_messages_to_gateway(enriched_messages)
    print("[birthday_dictionary] Worker completed successfully.")

if __name__ == "__main__":
    main()