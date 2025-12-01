import json
import os
import signal
import sys
from collections import defaultdict
import time
import threading
from common.health_check_receiver import HealthCheckReceiver
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

# File paths for backup
PERSISTENCE_DIR = "/app/persistence"
BACKUP_FILE_TOP_USERS = f"{PERSISTENCE_DIR}/birthday_dictionary_worker_{WORKER_INDEX}_backup.txt"
AUXILIARY_FILE_TOP_USERS = f"{PERSISTENCE_DIR}/birthday_dictionary_worker_{WORKER_INDEX}_backup.tmp"

# BACKUP_FILE_USERS_DATA = f"{PERSISTENCE_DIR}/birthday_dictionary_worker_{WORKER_INDEX}_backup.txt"
# AUXILIARY_FILE_USERS_DATA = f"{PERSISTENCE_DIR}/birthday_dictionary_worker_{WORKER_INDEX}_backup.tmp"
BACKUP_FILE_USERS_DATA = f"{PERSISTENCE_DIR}/birthday_dictionary_worker_{WORKER_INDEX}_backup_i.txt"
AUXILIARY_FILE_USERS_DATA = f"{PERSISTENCE_DIR}/birthday_dictionary_worker_{WORKER_INDEX}_backup_i.tmp"

def get_backup_file_users_data(client_id):
    return BACKUP_FILE_TOP_USERS.replace("i", str(client_id))
def get_aux_file_users_data(client_id):
    return AUXILIARY_FILE_USERS_DATA.replace("i", str(client_id))

receiver_queue = None
gateway_request_queue = None
gateway_client_data_queue = None
query4_answer_queue = None

clients_lock = threading.RLock()
clients = {}
death_clients = []

client_in_process_cond = threading.Condition()
client_in_process = False

g_last_message_id = "" # NOTE: Might not be necessary if we assume gateway never dies
birthdays_per_client = {}
passed_top_users = {}

class BirthClientHandler(threading.Thread):
    def __init__(self, client_id, message):
        super().__init__(daemon=True)
        self.client_id = client_id
        self.data_queue = Queue()
        self.user_ids = set()
        for user in message.get('top_users', []):
            self.user_ids.add(user['user_id'])
        self.message = message

        # NOTE: self.last_message_id works since its saved per client (which means more unnecessary savings of this), 
        # could be simplified making sequential processing in 1 file the last_message (sender is always one (gateway))
        # if there is a way to send acks from client threads, this will be ok since it will be necessary to do it per client (sending acks in desorder)
        self.last_message_id = None
        self.client_birthdays = {}

    # NOTE: Functions used before start thre thread
    def send_data_request(self):
        request_client_data_for_client(self.client_id)    
    def recover(self, client_birthdays, last_message_id):
        self.client_birthdays = client_birthdays
        self.last_message_id = last_message_id

    def run(self):
        global client_in_process
        global g_last_message_id
        
        is_last = False
        while not is_last:
            parsed_message = self.data_queue.get()
            mesage_id = parsed_message.get('message_id')
            if self.last_message_id == mesage_id:
                print(f"[birthday_dictionary] A DUPLICATED MESSAGE WAS RECEIVED IN BITHDAY CLIENT HANDLER, IGNORING IT", flush=True)
                with client_in_process_cond:
                    client_in_process = False
                    client_in_process_cond.notify_all()
                continue
            for row in parsed_message['rows']:
                parts = row.split(',')
                if len(parts) == 2:
                    user_id, birthday = parts
                    if user_id in self.user_ids:
                        self.client_birthdays[user_id] = birthday
            is_last = parsed_message['is_last']

            self.last_message_id = mesage_id

            if not is_last:
                # save_user_data_state_to_disk(self.client_id, self.client_birthdays, self.last_message_id)
                with client_in_process_cond:
                    birthdays_per_client[self.client_id] = self.client_birthdays
                    g_last_message_id = self.last_message_id
                    save_users_data_state_to_disk(passed_top_users, birthdays_per_client, g_last_message_id)
                    
                    client_in_process = False
                    client_in_process_cond.notify_all()
        
        enriched_message = append_birthdays_to_message(self.message, self.client_birthdays)
        send_enriched_message_to_gateway(enriched_message)
    
        # Instead of saving, delete
        # os.remove(get_backup_file_users_data(self.client_id))
        
        with client_in_process_cond:
            del passed_top_users[self.client_id]
            del birthdays_per_client[self.client_id]
            g_last_message_id = self.last_message_id
            save_users_data_state_to_disk(passed_top_users, birthdays_per_client, g_last_message_id)
            
            client_in_process = False
            client_in_process_cond.notify_all()


def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(receiver_queue)
    _close_queue(gateway_request_queue)
    _close_queue(gateway_client_data_queue)
    _close_queue(query4_answer_queue)

def listen_for_top_users(messages_by_client, message_count_by_client, last_message_per_sender):

    receiver_queue = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=RECEIVER_EXCHANGE,
        exchange_type='topic',
        queue_name=f"birthday_dictionary_worker_{WORKER_INDEX}_queue",
        routing_keys=[f"client.{WORKER_INDEX}"]
    )
    print(f"[birthday_dictionary] Listening for top users on queue: {RECEIVER_QUEUE}")

    # TODO: Start ready clients threads from recovered state (might be necessary to do out of this scope)

    def on_message_callback(message: bytes, delivery_tag=None, channel=None):

        # Before proceed with the message, clean up death clients 
        with clients_lock:
            for death_client in death_clients:
                clients[death_client].join()
                del clients[death_client]
            death_clients.clear()

        # Process the message
        parsed_message = parse_message(message)
        sender_id = parsed_message['sender']
        message_id = parsed_message.get('message_id')  # Assume each message has a unique 'message_id'
        
        # Skip message if duplicated
        if sender_id in last_message_per_sender and last_message_per_sender[sender_id] == message_id:
            print(f"[birthday_dictionary] A DUPLICATED MESSAGE WAS RECEIVED, IGNORING IT", flush=True)
            # Send ack for duplicated message
            if channel and delivery_tag:
                channel.basic_ack(delivery_tag=delivery_tag)
                print(f"[birthday_dictionary] Acknowledged message with delivery tag {delivery_tag}", flush=True)
            return

        last_message_per_sender[sender_id] = message_id

        client_id = parsed_message['client_id']
        top_users = []
        for row in parsed_message['rows']:
            parts = row.split(',')
            if len(parts) == 3:
                store_id, user_id, count = parts
                top_users.append({'store_id': store_id, 'user_id': user_id, 'count': int(count)})
        # Instead of appending a new dict for each message, extend the list of top users
        if client_id not in messages_by_client:
            messages_by_client[client_id] = {'client_id': client_id, 'top_users': []}
        messages_by_client[client_id]['top_users'].extend(top_users)
        message_count_by_client[client_id] += 1
        print(f"[birthday_dictionary] Received message {message_count_by_client[client_id]}/{CATEGORIZER_Q4_WORKERS} for client {client_id}")

        with clients_lock:
            # Only proceed when all expected messages for this client have arrived
            if message_count_by_client[client_id] == CATEGORIZER_Q4_WORKERS and client_id not in clients:
                print(f"[birthday_dictionary] Top users: {messages_by_client[client_id]}")
                clients[client_id] = BirthClientHandler(client_id, messages_by_client[client_id])
                clients[client_id].send_data_request()

                with client_in_process_cond:
                    passed_top_users[client_id] = messages_by_client[client_id]
                    save_users_data_state_to_disk(passed_top_users, birthdays_per_client, g_last_message_id)

                del messages_by_client[client_id]
                del message_count_by_client[client_id]
                save_top_users_state_to_disk(messages_by_client, message_count_by_client, last_message_per_sender)

                clients[client_id].start()
        # Send acknowledgment after processing the message
        if channel and delivery_tag:
            channel.basic_ack(delivery_tag=delivery_tag)
            print(f"[categorizer_q4] Acknowledged message with delivery tag {delivery_tag}", flush=True)
        else:
            print(f"[categorizer_q4] ACK not manual (top_users) ", flush=True)
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

    def on_message_callback(message: bytes, delivery_tag=None, channel=None):
        parsed_message = parse_message(message)

        with clients_lock:
            # print(f"DATA RECEIVED: {parsed_message['client_id']} while clients: {list(clients.keys())}", flush=True)
            
            # NOTE: And this is new, this filter might be not necessary if sequential processing per sender_id is implemented
            if(parsed_message['client_id'] not in clients):
                print(f"THIS SHOULD NOT HAPPEN: {parsed_message['client_id']} not in clients: {list(clients.keys())}", flush=True)
                return
            
            global client_in_process
            with client_in_process_cond:
                client_in_process = True

            clients[parsed_message['client_id']].data_queue.put(parsed_message)

            if parsed_message['is_last']:
                death_clients.append(parsed_message['client_id']) # register for cleanning

            with client_in_process_cond:
                while client_in_process:
                    client_in_process_cond.wait()

        # NOTE: This is practically sequential, consider avoid client threads OR find the way to send acks from them

        # Send acknowledgment after processing the message
        if channel and delivery_tag:
            channel.basic_ack(delivery_tag=delivery_tag)
            print(f"[categorizer_q4] Acknowledged message with delivery tag {delivery_tag}", flush=True)
        else:
            print(f"[categorizer_q4] ACK not manual (users_data) ", flush=True)

    try:
        gateway_client_data_queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[birthday_dictionary] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[birthday_dictionary] Message error in middleware.", file=sys.stderr)
    finally:
        gateway_client_data_queue.close()


def save_users_data_state_to_disk(passed_top_users, birthdays_per_client, g_last_message_id):
    try:
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)

        serializable_data = {
            # "client_store_user_counter": {
            #     client_id: {
            #         store_id: dict(user_counter)
            #         for store_id, user_counter in store_users_data.items()
            #     }
            #     for client_id, store_users_data in client_store_user_counter.items()
            # },
            # "end_messages": dict(client_end_messages),
            "passed_top_users": dict(passed_top_users),
            "birthdays_per_client": dict(birthdays_per_client),
            "g_last_message_id": g_last_message_id
        }
        
        with open(AUXILIARY_FILE_USERS_DATA, "w") as aux_file:
            json.dump(serializable_data, aux_file)  # Write JSON data
            # aux_file.write("\n")  # Add a newline to separate JSON from messages
        os.rename(AUXILIARY_FILE_USERS_DATA, BACKUP_FILE_USERS_DATA) # Atomic
        print(f"[birthday_dictionary] Worker {WORKER_INDEX} saved state(user_data) to {BACKUP_FILE_USERS_DATA} atomically")
    except Exception as e:
        print(f"[birthday_dictionary] ERROR saving state(user_data) to disk: {e}", file=sys.stderr)

# def save_user_data_state_to_disk(client_id, client_birthdays, last_message_id):
#     try:
#         os.makedirs(PERSISTENCE_DIR, exist_ok=True)

#         serializable_data = {
#             # "client_store_user_counter": {
#             #     client_id: {
#             #         store_id: dict(user_counter)
#             #         for store_id, user_counter in store_users_data.items()
#             #     }
#             #     for client_id, store_users_data in client_store_user_counter.items()
#             # },
#             # "end_messages": dict(client_end_messages),
#             "client_birthdays": dict(client_birthdays),
#             "last_message_id": last_message_id
#         }
        
#         with open(get_aux_file_users_data(client_id), "w") as aux_file:
#             json.dump(serializable_data, aux_file)  # Write JSON data
#             # aux_file.write("\n")  # Add a newline to separate JSON from messages
#         os.rename(get_aux_file_users_data(client_id), get_backup_file_users_data(client_id)) # Atomic
#         print(f"[birthday_dictionary] Worker {WORKER_INDEX} saved state(user_data) to {get_backup_file_users_data(client_id)} atomically")
#     except Exception as e:
#         print(f"[birthday_dictionary] ERROR saving state(user_data) to disk: {e}", file=sys.stderr)

def save_top_users_state_to_disk(messages_by_client, message_count_by_client, last_message_per_sender):
    try:
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)

        serializable_data = {
            # "client_store_user_counter": {
            #     client_id: {
            #         store_id: dict(user_counter)
            #         for store_id, user_counter in store_users_data.items()
            #     }
            #     for client_id, store_users_data in client_store_user_counter.items()
            # },
            # "end_messages": dict(client_end_messages),
            "messages_by_client": dict(messages_by_client),
            "message_count_by_client": dict(message_count_by_client),
            "last_message_per_sender": dict(last_message_per_sender)
        }
        
        with open(AUXILIARY_FILE_TOP_USERS, "w") as aux_file:
            json.dump(serializable_data, aux_file)  # Write JSON data
            # aux_file.write("\n")  # Add a newline to separate JSON from messages
        os.rename(AUXILIARY_FILE_TOP_USERS, BACKUP_FILE_TOP_USERS) # Atomic
        print(f"[birthday_dictionary] Worker {WORKER_INDEX} saved state(top_users) to {BACKUP_FILE_TOP_USERS} atomically")
    except Exception as e:
        print(f"[birthday_dictionary] ERROR saving state(top_users) to disk: {e}", file=sys.stderr)

def recover_top_users_state():
    messages_by_client = defaultdict(list) # better name: top_users_by_client?
    message_count_by_client = defaultdict(int)
    last_message_per_sender = defaultdict(str)

    # Check if the auxiliary file exists
    if os.path.exists(AUXILIARY_FILE_TOP_USERS):
        print(f"[birthday_dictionary] Worker {WORKER_INDEX} detected auxiliary file {AUXILIARY_FILE_TOP_USERS}, discarding it")
        os.remove(AUXILIARY_FILE_TOP_USERS)  # Discard the auxiliary file

    if not os.path.exists(BACKUP_FILE_TOP_USERS):
        print(f"[birthday_dictionary] Worker {WORKER_INDEX} no backup file found, starting fresh")
        return messages_by_client, message_count_by_client, last_message_per_sender

    try:
        with open(BACKUP_FILE_TOP_USERS, "r") as backup_file:
            lines = backup_file.readlines()

            # Parse the first line as JSON
            try:
                # Separate the JSON dictionary (first part) and appended rows (remaining lines)
                # Load the JSON dictionary from the first part of the file
                json_data = lines[0]
                data = json.loads(json_data)

                # Restore last message IDs
                messages_by_client = defaultdict(list, data["messages_by_client"])
                # Restore messages count per client
                message_count_by_client = defaultdict(int, data["message_count_by_client"])
                # Restore last message per sender
                last_message_per_sender = defaultdict(str, data["last_message_per_sender"])

                print(f"[birthday_dictionary] Successfully recovered state from JSON in {BACKUP_FILE_TOP_USERS}")
            except json.JSONDecodeError as e:
                print(f"[birthday_dictionary] ERROR parsing JSON from backup file: {e}", file=sys.stderr)
                return messages_by_client, message_count_by_client, last_message_per_sender

    except Exception as e:
        print(f"[birthday_dictionary] ERROR recovering state from {BACKUP_FILE_TOP_USERS}: {e}", file=sys.stderr)

    return messages_by_client, message_count_by_client, last_message_per_sender


def recover_users_data_state():
    passed_top_users = {}
    birthdays_per_client = {}
    g_last_message_id = ""

    # Check if the auxiliary file exists
    if os.path.exists(AUXILIARY_FILE_USERS_DATA):
        print(f"[birthday_dictionary] Worker {WORKER_INDEX} detected auxiliary file {AUXILIARY_FILE_USERS_DATA}, discarding it")
        os.remove(AUXILIARY_FILE_USERS_DATA)  # Discard the auxiliary file

    if not os.path.exists(BACKUP_FILE_USERS_DATA):
        print(f"[birthday_dictionary] Worker {WORKER_INDEX} no backup file found (users_data), starting fresh")
        return passed_top_users, birthdays_per_client, g_last_message_id

    try:
        with open(BACKUP_FILE_USERS_DATA, "r") as backup_file:
            lines = backup_file.readlines()

            # Parse the first line as JSON
            try:
                # Separate the JSON dictionary (first part) and appended rows (remaining lines)
                # Load the JSON dictionary from the first part of the file
                json_data = lines[0]
                data = json.loads(json_data)

                passed_top_users = dict(data["passed_top_users"])
                birthdays_per_client = dict(data["birthdays_per_client"])
                g_last_message_id = data["g_last_message_id"]

                print(f"[birthday_dictionary] Successfully recovered state from JSON in {BACKUP_FILE_USERS_DATA}")
            except json.JSONDecodeError as e:
                print(f"[birthday_dictionary] ERROR parsing JSON from backup file: {e}", file=sys.stderr)
                return passed_top_users, birthdays_per_client, g_last_message_id

    except Exception as e:
        print(f"[birthday_dictionary] ERROR recovering state from {BACKUP_FILE_USERS_DATA}: {e}", file=sys.stderr)

    return passed_top_users, birthdays_per_client, g_last_message_id

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)

    health_check_receiver = HealthCheckReceiver()
    health_check_receiver.start()

    # Check if a backup file exists
    if not os.path.exists(BACKUP_FILE_TOP_USERS) and not os.path.exists(BACKUP_FILE_USERS_DATA):
        print("[birthday_dictionary] No backup file found. Waiting for RabbitMQ to be ready...", flush=True)
        time.sleep(30)  # Wait for RabbitMQ to be ready
    else:
        print("[birthday_dictionary] Backup file found. Skipping RabbitMQ wait.", flush=True)

    global passed_top_users, birthdays_per_client, g_last_message_id
    messages_by_client, message_count_by_client, last_message_per_sender = recover_top_users_state()
    passed_top_users, birthdays_per_client, g_last_message_id = recover_users_data_state()

    # Cleaning in case of dying between the saving on disk
    for client_id in passed_top_users:
        if client_id in messages_by_client:
            del messages_by_client[client_id]
            del message_count_by_client[client_id]

    # Start recovered client threads
    for client_id, top_users in passed_top_users.items():
        clients[client_id] = BirthClientHandler(client_id, top_users)
        clients[client_id].recover(birthdays_per_client[client_id] if client_id in birthdays_per_client else {}, g_last_message_id)
        clients[client_id].start()

    users_data_listener_t = threading.Thread(
        target=listen_for_users_data,
        args=(),
        daemon=True
    )
    users_data_listener_t.start()

    listen_for_top_users(messages_by_client, message_count_by_client, last_message_per_sender)
    print("[birthday_dictionary] Worker completed successfully.")

if __name__ == "__main__":
    main()