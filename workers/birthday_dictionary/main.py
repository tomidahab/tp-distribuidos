import os
import sys
from collections import defaultdict
import time
from common.protocol import parse_message, build_message
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'birthday_dictionary_queue')
GATEWAY_REQUEST_QUEUE = os.environ.get('GATEWAY_REQUEST_QUEUE', 'birthday_dictionary_client_request_queue')
GATEWAY_CLIENT_DATA_QUEUE = os.environ.get('GATEWAY_CLIENT_DATA_QUEUE', 'gateway_client_data_queue')
QUERY4_ANSWER_QUEUE = os.environ.get('QUERY4_ANSWER_QUEUE', 'query4_answer_queue')
CATEGORIZER_Q4_WORKERS = int(os.environ.get('AMOUNT_OF_WORKERS', 3))

def listen_for_top_users():
    messages = []
    user_ids = set()
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, RECEIVER_QUEUE)
    print(f"[birthday_dictionary] Listening for top users on queue: {RECEIVER_QUEUE}")

    end_messages_received = 0

    def normalize_user_id(user_id):
        # Remove trailing .0 if present and always use string
        if isinstance(user_id, float):
            user_id = int(user_id)
        user_id = str(user_id)
        if user_id.endswith('.0'):
            user_id = user_id[:-2]
        return user_id

    def on_message_callback(message: bytes):
        nonlocal end_messages_received
        parsed_message = parse_message(message)
        top_users = []
        # print(f"[birthday_dictionary] Received message with {len(parsed_message['rows'])} rows, is_last={parsed_message['is_last']}")
        for row in parsed_message['rows']:
            parts = row.split(',')
            if len(parts) == 3:
                store_id, user_id, count = parts
                user_id = normalize_user_id(user_id)
                top_users.append({'store_id': store_id, 'user_id': user_id, 'count': int(count)})
                user_ids.add(user_id)
        messages.append({'top_users': top_users, 'is_last': parsed_message['is_last']})
        # print(f"[birthday_dictionary] message processed, total messages: {len(messages)}, unique user_ids: {len(user_ids)}")
        if parsed_message['is_last']:
            end_messages_received += 1
            print(f"[birthday_dictionary] Received end message {end_messages_received}/{CATEGORIZER_Q4_WORKERS} from categorizer_q4 workers.")
            if end_messages_received >= CATEGORIZER_Q4_WORKERS:
                print("[birthday_dictionary] All end messages received, stopping top user collection.")
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
    # Use build_message to request client data (empty rows, type 5 for client request, is_last=1)
    message, _ = build_message(0, 5, 1, [])
    queue.send(message)
    print(f"[birthday_dictionary] Requested all client data from gateway.")
    queue.close()

def listen_for_client_data(user_ids):
    client_birthdays = {}
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_CLIENT_DATA_QUEUE)
    print(f"[birthday_dictionary] Listening for client data on queue: {GATEWAY_CLIENT_DATA_QUEUE}")

    def normalize_user_id(user_id):
        if isinstance(user_id, float):
            user_id = int(user_id)
        user_id = str(user_id)
        if user_id.endswith('.0'):
            user_id = user_id[:-2]
        return user_id

    def on_message_callback(message: bytes):
        parsed_message = parse_message(message)
        for row in parsed_message['rows']:
            parts = row.split(',')
            client_id, birthday = parts
            client_id = normalize_user_id(client_id)
            if client_id in user_ids:
                client_birthdays[client_id] = birthday
        if parsed_message['is_last']:
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
        # Prepare rows as CSV lines for each top user
        rows = []
        for user in msg.get('top_users', []):
            # Format: store_id,user_id,count,birthday
            row = f"{user['store_id']},{user['user_id']},{user['count']},{user.get('birthday', '')}"
            rows.append(row)
        # Use build_message to encode the result
        message, _ = build_message(0, 4, int(msg.get('is_last', 0)), rows)
        queue.send(message)
        print(f"[birthday_dictionary] Sent enriched message to gateway: {rows}")
    queue.close()

def main():
    time.sleep(30)
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