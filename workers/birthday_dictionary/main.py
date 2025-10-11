import os
import signal
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

receiver_queue = None
gateway_request_queue = None
gateway_client_data_queue = None
query4_answer_queue = None

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(receiver_queue)
    _close_queue(gateway_request_queue)
    _close_queue(gateway_client_data_queue)
    _close_queue(query4_answer_queue)

def listen_for_top_users():
    messages = []
    user_ids = set()
    receiver_queue = MessageMiddlewareQueue(RABBITMQ_HOST, RECEIVER_QUEUE)
    print(f"[birthday_dictionary] Listening for top users on queue: {RECEIVER_QUEUE}")

    # Track END messages per client: {client_id: count}
    client_end_messages = defaultdict(int)
    completed_clients = set()

    def on_message_callback(message: bytes):
        nonlocal completed_clients
        parsed_message = parse_message(message)
        client_id = parsed_message['client_id']
        
        # Skip if client already completed
        if client_id in completed_clients:
            return
            
        top_users = []
        print(f"[birthday_dictionary] Received message for client {client_id} with {len(parsed_message['rows'])} rows, is_last={parsed_message['is_last']}")
        for row in parsed_message['rows']:
            parts = row.split(',')
            if len(parts) == 3:
                store_id, user_id, count = parts
                top_users.append({'store_id': store_id, 'user_id': user_id, 'count': int(count)})
                user_ids.add(user_id)
        messages.append({'client_id': client_id, 'top_users': top_users, 'is_last': parsed_message['is_last']})
        print(f"[birthday_dictionary] Message for client {client_id} processed, total messages: {len(messages)}, unique user_ids: {len(user_ids)}")
        
        if parsed_message['is_last']:
            client_end_messages[client_id] += 1
            print(f"[birthday_dictionary] Received end message {client_end_messages[client_id]}/{CATEGORIZER_Q4_WORKERS} for client {client_id}")
            if client_end_messages[client_id] >= CATEGORIZER_Q4_WORKERS:
                print(f"[birthday_dictionary] Client {client_id} completed, will send data request for this client")
                completed_clients.add(client_id)
                
                # Send client-specific data request
                request_message, _ = build_message(client_id, 0, 1, [])  # Request data for this client
                request_queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_REQUEST_QUEUE)
                request_queue.send(request_message)
                request_queue.close()
                
                # Check if all clients completed
                if not any(count < CATEGORIZER_Q4_WORKERS for count in client_end_messages.values() if count > 0):
                    print("[birthday_dictionary] All clients completed, stopping")
                    receiver_queue.stop_consuming()

    try:
        receiver_queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[birthday_dictionary] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[birthday_dictionary] Message error in middleware.", file=sys.stderr)
    finally:
        receiver_queue.close()
    return messages, user_ids

def request_client_data():
    global gateway_request_queue
    gateway_request_queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_REQUEST_QUEUE)
    # Use build_message to request client data (empty rows, type 5 for client request, is_last=1)
    message, _ = build_message(0, 5, 1, [])
    gateway_request_queue.send(message)
    print(f"[birthday_dictionary] Requested all client data from gateway.")
    gateway_request_queue.close()

def listen_for_client_data(user_ids):
    client_birthdays = {}
    global gateway_client_data_queue
    gateway_client_data_queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_CLIENT_DATA_QUEUE)
    print(f"[birthday_dictionary] Listening for client data on queue: {GATEWAY_CLIENT_DATA_QUEUE}")

    def on_message_callback(message: bytes):
        parsed_message = parse_message(message)
        for row in parsed_message['rows']:
            parts = row.split(',')
            if len(parts) == 2:
                client_id, birthday = parts
                if client_id in user_ids:
                    client_birthdays[client_id] = birthday
        if parsed_message['is_last']:
            print("[birthday_dictionary] Received end message, stopping client data collection.")
            gateway_client_data_queue.stop_consuming()

    try:
        gateway_client_data_queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[birthday_dictionary] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[birthday_dictionary] Message error in middleware.", file=sys.stderr)
    finally:
        gateway_client_data_queue.close()
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
    global query4_answer_queue
    query4_answer_queue = MessageMiddlewareQueue(RABBITMQ_HOST, QUERY4_ANSWER_QUEUE)
    for msg in enriched_messages:
        # Prepare rows as CSV lines for each top user
        rows = []
        for user in msg.get('top_users', []):
            # Format: store_id,user_id,count,birthday
            row = f"{user['store_id']},{user['user_id']},{user['count']},{user.get('birthday', '')}"
            rows.append(row)
        # Use build_message to encode the result
        message, _ = build_message(0, 4, int(msg.get('is_last', 0)), rows)
        query4_answer_queue.send(message)
        print(f"[birthday_dictionary] Sent enriched message to gateway: {rows}")
    query4_answer_queue.close()

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
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