import os
import signal
import sys
from collections import defaultdict
import time
import threading
from common.protocol import parse_message, build_message
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareExchange
from queue import Queue # Thread safe queue

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'birthday_dictionary_queue')
GATEWAY_REQUEST_QUEUE = os.environ.get('GATEWAY_REQUEST_QUEUE', 'birthday_dictionary_client_request_queue')
GATEWAY_CLIENT_DATA_QUEUE = os.environ.get('GATEWAY_CLIENT_DATA_QUEUE', 'gateway_client_data_queue')
QUERY4_ANSWER_QUEUE = os.environ.get('QUERY4_ANSWER_QUEUE', 'query4_answer_queue')
CATEGORIZER_Q4_WORKERS = int(os.environ.get('CATEGORIZER_Q4_WORKERS', 3))
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', 0))
AMOUNT_OF_WORKERS = int(os.environ.get('AMOUNT_OF_WORKERS', 1))
RECEIVER_EXCHANGE = os.environ.get('RECEIVER_EXCHANGE', 'birthday_dictionary_exchange')

receiver_queue = None
gateway_request_queue = None
gateway_client_data_queue = None
query4_answer_queue = None

clients_lock = threading.RLock()
clients = {}
death_clients = []

class BirthClientHandler(threading.Thread):
    def __init__(self, client_id, user_ids, message):
        super().__init__(daemon=True)
        self.client_id = client_id
        self.data_queue = Queue()
        self.user_ids = user_ids
        self.message = message

    def run(self):
        request_client_data_for_client(self.client_id)

        client_birthdays = {}

        is_last = False
        while not is_last:
            parsed_message = self.data_queue.get()
            for row in parsed_message['rows']:
                parts = row.split(',')
                if len(parts) == 2:
                    client_id, birthday = parts
                    if client_id in self.user_ids:
                        client_birthdays[client_id] = birthday
            is_last = parsed_message['is_last']
        
        enriched_message = append_birthdays_to_message(self.message, client_birthdays)
        send_enriched_message_to_gateway(enriched_message)

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(receiver_queue)
    _close_queue(gateway_request_queue)
    _close_queue(gateway_client_data_queue)
    _close_queue(query4_answer_queue)

def listen_for_top_users():
    receiver_queue = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=RECEIVER_EXCHANGE,
        exchange_type='topic',
        queue_name=f"birthday_dictionary_worker_{WORKER_INDEX}_queue",
        routing_keys=[f"client.{WORKER_INDEX}"]
    )
    print(f"[birthday_dictionary] Listening for top users on queue: {RECEIVER_QUEUE}")

    messages_by_client = defaultdict(list)
    user_ids_by_client = defaultdict(set)
    message_count_by_client = defaultdict(int)
    threads_started = set()

    def on_message_callback(message: bytes):

        # Before proceed with the message, clean up death clients 
        with clients_lock:
            for death_client in death_clients:
                clients[death_client].join()
                del clients[death_client]
                threads_started.remove(death_client)
            death_clients.clear()

        # Process the message
        parsed_message = parse_message(message)
        client_id = parsed_message['client_id']
        top_users = []
        for row in parsed_message['rows']:
            parts = row.split(',')
            if len(parts) == 3:
                store_id, user_id, count = parts
                top_users.append({'store_id': store_id, 'user_id': user_id, 'count': int(count)})
                user_ids_by_client[client_id].add(user_id)
        # Instead of appending a new dict for each message, extend the list of top users
        if client_id not in messages_by_client:
            messages_by_client[client_id] = {'client_id': client_id, 'top_users': []}
        messages_by_client[client_id]['top_users'].extend(top_users)
        message_count_by_client[client_id] += 1
        print(f"[birthday_dictionary] Received message {message_count_by_client[client_id]}/{CATEGORIZER_Q4_WORKERS} for client {client_id}")

        # Only proceed when all expected messages for this client have arrived
        if message_count_by_client[client_id] == CATEGORIZER_Q4_WORKERS and client_id not in threads_started:
            print(f"[birthday_dictionary] Top users: {messages_by_client[client_id]}")
            threads_started.add(client_id)
            with clients_lock:
                clients[client_id] = BirthClientHandler(client_id, user_ids_by_client[client_id], messages_by_client[client_id])
                clients[client_id].start()

    try:
        receiver_queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[birthday_dictionary] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[birthday_dictionary] Message error in middleware.", file=sys.stderr)
    finally:
        receiver_queue.close()


def append_birthdays_to_message(message, client_birthdays):
    enriched_top_users = []
    for user in message.get('top_users', []):
        user_id = user['user_id']
        user_with_birthday = dict(user)
        user_with_birthday['birthday'] = client_birthdays.get(user_id)
        enriched_top_users.append(user_with_birthday)
    enriched_message = dict(message)
    enriched_message['top_users'] = enriched_top_users
    return enriched_message

def send_enriched_message_to_gateway(enriched_message):
    query4_answer_queue = MessageMiddlewareQueue(RABBITMQ_HOST, QUERY4_ANSWER_QUEUE)
    rows = []
    for user in enriched_message.get('top_users', []):
        row = f"{user['store_id']},{user['user_id']},{user['count']},{user.get('birthday', '')}"
        rows.append(row)
    client_id = enriched_message.get('client_id', 0)
    message, _ = build_message(client_id, 4, 1, rows)
    query4_answer_queue.send(message)
    print(f"[birthday_dictionary] Sent enriched message to gateway for client_id {client_id}: {rows}")
    query4_answer_queue.close()

def request_client_data_for_client(client_id):
    """
    Sends a request for client data for a specific client_id.
    The client_id is included in the message so the gateway can route and respond accordingly.
    """
    queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_REQUEST_QUEUE)
    # Use type 5 for client request, is_last=1, and include client_id in the message
    # The rows can be empty or contain the client_id if your protocol expects it
    message, _ = build_message(client_id, 5, 1, [client_id])
    print(f"[birthday_dictionary] Sending client data request for client_id: {client_id}")
    queue.send(message)
    queue.close()

def listen_for_users_data():
    global gateway_client_data_queue

    queue_name = f"{GATEWAY_CLIENT_DATA_QUEUE}_WORKER_{WORKER_INDEX}" # Queue per worker

    gateway_client_data_queue = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name="birth_queue_users_exchange",
        exchange_type='topic',
        queue_name=queue_name,
        routing_keys=[f"birth_dict.{WORKER_INDEX}"]
    )
    print(f"[birthday_dictionary] Listening for client data on queue: {queue_name} with routing key: birth_dict.{WORKER_INDEX}")

    def on_message_callback(message: bytes):
        parsed_message = parse_message(message)

        with clients_lock:
            # print(f"DATA RECEIVED: {parsed_message['client_id']} while clients: {list(clients.keys())}", flush=True)
            
            if(parsed_message['client_id'] not in clients):
                print(f"{parsed_message['client_id']} not in clients: {list(clients.keys())}", flush=True)
                

            clients[parsed_message['client_id']].data_queue.put(parsed_message)
            if parsed_message['is_last']:
                death_clients.append(parsed_message['client_id']) # register for cleanning

    try:
        gateway_client_data_queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[birthday_dictionary] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[birthday_dictionary] Message error in middleware.", file=sys.stderr)
    finally:
        gateway_client_data_queue.close()

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    time.sleep(30)

    users_data_listener_t = threading.Thread(
        target=listen_for_users_data,
        args=(),
        daemon=True
    )
    users_data_listener_t.start()

    listen_for_top_users()
    print("[birthday_dictionary] Worker completed successfully.")

if __name__ == "__main__":
    main()