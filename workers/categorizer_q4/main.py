import base64
import heapq
import os
import signal
import sys
from collections import defaultdict, Counter
import time
import json
from common.health_check_receiver import HealthCheckReceiver
from common.protocol import CSV_TYPES_REVERSE, build_message, parse_message, row_to_dict
from common.middleware import MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'store_user_categorizer_queue')
TOPIC_EXCHANGE = os.environ.get('TOPIC_EXCHANGE', 'categorizer_q4_topic_exchange')
FANOUT_EXCHANGE = os.environ.get('FANOUT_EXCHANGE', 'categorizer_q4_fanout_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', 0))
BIRTHDAY_DICT_QUEUE = os.environ.get('BIRTHDAY_DICT_QUEUE', 'birthday_dictionary_queue')
AMOUNT_OF_WORKERS = int(os.environ.get('AMOUNT_OF_WORKERS', 1))
BIRTHDAY_MAPPERS = int(os.environ.get('BIRTHDAY_MAPPERS', 3))
NUMBER_OF_YEAR_WORKERS = int(os.environ.get('NUMBER_OF_YEAR_WORKERS', '3'))


# File paths for backup
PERSISTENCE_DIR = "/app/persistence"
BACKUP_FILE = f"{PERSISTENCE_DIR}/categorizer_q4_worker_{WORKER_INDEX}_backup.txt"
AUXILIARY_FILE = f"{PERSISTENCE_DIR}/categorizer_q4_worker_{WORKER_INDEX}_backup.tmp"

receiver_queue = None
birthday_dict_queue = None

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(receiver_queue)
    _close_queue(birthday_dict_queue)

def listen_for_transactions():
    client_store_user_counter, client_end_messages, last_message_per_sender, messages = recover_state()
    completed_clients_for_debug = set()
    messages_counter = 0  # Counter to track the number of messages processed
    
    # Process recovered messages
    for message in messages:
        try:
            parsed_message = parse_message(message)
            sender_id = parsed_message['sender']
            message_id = parsed_message.get('message_id')  # Assume each message has a unique 'message_id'
            type_of_message = parsed_message['csv_type']
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            
            # Skip if client already completed
            # NOTE: SHOULD NOT HAPPEN
            if client_id in completed_clients_for_debug:
                print(f"[categorizer_q4][recover][debug] Worker {WORKER_INDEX} ignoring message for already completed client {client_id}")
                return
            
            last_message_per_sender[sender_id] = message_id

            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                store_id = dic_fields_row.get('store_id')
                user_id = dic_fields_row.get('user_id')
                if None not in (store_id, user_id) and store_id != '' and user_id != '':
                    client_store_user_counter[client_id][store_id][int(float(user_id))] += 1
            

            if is_last:
                client_end_messages[client_id] += 1
                print(f"[categorizer_q4][recover] Worker {WORKER_INDEX} received END message {client_end_messages[client_id]}/{NUMBER_OF_YEAR_WORKERS} for client {client_id}")
                if client_end_messages[client_id] >= NUMBER_OF_YEAR_WORKERS:
                    print(f"[categorizer_q4][recover] cleaning completed for {client_id}")
                    # Clear tracking for completed client
                    del client_store_user_counter[client_id]
                    del client_end_messages[client_id]
            
            messages_counter += 1
        except Exception as e:
            print(f"[categorizer_q4] ERROR processing recovered row: {e}", file=sys.stderr)

    topic_routing_key = f"store.{WORKER_INDEX}"
    print(f"[categorizer_q4] Worker index: {WORKER_INDEX}, routing key: {topic_routing_key}")
    # Bind to both topic and fanout exchanges
    global receiver_queue
    receiver_queue = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=TOPIC_EXCHANGE,
        exchange_type='topic',
        queue_name=RECEIVER_QUEUE,
        routing_keys=[topic_routing_key]
    )

    print(f"[categorizer_q4] Listening for transactions on queue: {RECEIVER_QUEUE} (topic key: {topic_routing_key})")

    def on_message_callback(message: bytes, delivery_tag=None, channel=None):
        nonlocal completed_clients_for_debug, messages_counter
        try:
            parsed_message = parse_message(message)
            sender_id = parsed_message['sender']
            message_id = parsed_message.get('message_id')  # Assume each message has a unique 'message_id'
            type_of_message = parsed_message['csv_type']
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            
            # Skip if client already completed
            # NOTE: THIS SHOULD NOT HAPPEN?
            if client_id in completed_clients_for_debug:
                print(f"[categorizer_q4][debug] Worker {WORKER_INDEX} ignoring message for already completed client {client_id}", flush=True)
                return
            
            # Skip message if duplicated
            if sender_id in last_message_per_sender and last_message_per_sender[sender_id] == message_id:
                print(f"[categorizer_q4] A DUPLICATED MESSAGE WAS RECEIVED, IGNORING IT", flush=True)

                # Send ack for duplicated message
                if channel and delivery_tag:
                    channel.basic_ack(delivery_tag=delivery_tag)
                    print(f"[categorizer_q4] Acknowledged message with delivery tag {delivery_tag}", flush=True)
                return
            
            last_message_per_sender[sender_id] = message_id

            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                store_id = dic_fields_row.get('store_id')
                user_id = dic_fields_row.get('user_id')
                if None not in (store_id, user_id) and store_id != '' and user_id != '':
                    client_store_user_counter[client_id][store_id][int(float(user_id))] += 1

            if is_last:
                client_end_messages[client_id] += 1
                print(f"[categorizer_q4] Worker {WORKER_INDEX} received END message {client_end_messages[client_id]}/{NUMBER_OF_YEAR_WORKERS} for client {client_id}", flush=True)
                
                if client_end_messages[client_id] >= NUMBER_OF_YEAR_WORKERS:
                    print(f"[categorizer_q4] Client {client_id} received all END messages, processing results", flush=True)
                    completed_clients_for_debug.add(client_id)
                    
                    # Process and send results for this client
                    send_client_q4_results(client_id, client_store_user_counter[client_id])
                    
                    print(f"[categorizer_q4] cleaning completed for {client_id}")
                    # Once the results are sent, clear tracking for completed client
                    del client_store_user_counter[client_id]
                    del client_end_messages[client_id]

                    print(f"[categorizer_q4] Client {client_id} processing completed", flush=True)

            # Write the message to disk
            append_message_to_disk(message)
            print(f"[categorizer_q4] Message {message_id} stored", flush=True)

            messages_counter += 1
            # Every 100 rows, write the full dictionary to the backup file
            if messages_counter >= 100:
                save_state_to_disk(client_store_user_counter, client_end_messages, last_message_per_sender)
                messages_counter = 0

            # Send acknowledgment after processing the batch
            if channel and delivery_tag:
                channel.basic_ack(delivery_tag=delivery_tag)
                print(f"[categorizer_q4] Acknowledged message with delivery tag {delivery_tag}", flush=True)
        except Exception as e:
            print(f"[categorizer_q4] Error processing message: {e}", file=sys.stderr)

    try:
        receiver_queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q4] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q4] Message error in middleware.", file=sys.stderr)
    finally:
        receiver_queue.close()

def get_top_users(users_counter, n=3):
    items = users_counter.items()
    return heapq.nlargest(
        n,
        items,
        key=lambda x: (x[1], x[0]) # Top by count, then by id
    )

def get_top_users_per_store(store_user_counter, top_n=3):
    # Returns {store_id: [(user_id, purchase_count), ...]}
    top_users = {}
    for store_id, user_counter in store_user_counter.items():
        try:
            top_users[store_id] = get_top_users(user_counter)
        except Exception as e:
            print(f"[categorizer_q4] ERROR TAKE: {e}", file=sys.stderr)
            print(f"[categorizer_q4] ERROR TAKE DETAILS: {store_id}, {user_counter}")
    return top_users

def send_client_q4_results(client_id, store_user_counter):
    """Send Q4 results for specific client to birthday_dictionary as a single message"""
    try:
        # Calculate worker_index for this client
        worker_index = int(client_id.split("_")[1]) % BIRTHDAY_MAPPERS
        # Use topic exchange and routing key client.{worker_index}
        birthday_dict_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name='birthday_dictionary_exchange',
            exchange_type='topic',
            queue_name='',
            routing_keys=None
        )
        print(f"[categorizer_q4] STEP 1 {client_id}")
        # Get top users per store for this client
        top_users = get_top_users_per_store(store_user_counter, top_n=3)

        print(f"[categorizer_q4] STEP 2 {client_id}")
        # Prepare all rows for all stores in one message
        rows = []
        for store_id, users in top_users.items():
            for user_id, count in users:
                rows.append(f"{store_id},{user_id},{count}")
        print(f"[categorizer_q4] STEP 3 {client_id}")
        
        # Send a single message with all top users for all stores
        outgoing_sender = f"categorier_q4_worker_{WORKER_INDEX}"
        message, _ = build_message(client_id, 4, 1, rows, sender=outgoing_sender)
        birthday_dict_exchange.send(message, routing_key=f"client.{worker_index}")
        print(f"[categorizer_q4] Worker {WORKER_INDEX} sent ALL top users for client {client_id} to Birthday_Dictionary with routing key client.{worker_index}: {rows}")
        birthday_dict_exchange.close()
    except Exception as e:
        print(f"[categorizer_q4] ERROR sending results for client {client_id}: {e}", file=sys.stderr)
        # Don't raise to avoid stopping other clients

def append_message_to_disk(message):
    """
    Appends message to the backup file.
    """
    try:
        is_first_write = not os.path.exists(BACKUP_FILE) # On this method the file is created, so let an empty json as first line
        with open(BACKUP_FILE, "a") as backup_file:
            if is_first_write:
                backup_file.write("{}\n")
            encoded_msg = base64.b64encode(message).decode("ascii")
            backup_file.write(encoded_msg + "\n")  # Write each row on a new line
    except Exception as e:
        print(f"[categorizer_q4] ERROR appending message to disk: {e}", file=sys.stderr)

def save_state_to_disk(client_store_user_counter, client_end_messages, last_message_per_sender):
    try:
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)

        serializable_data = {
            "client_store_user_counter": {
                client_id: {
                    store_id: dict(user_counter)
                    for store_id, user_counter in store_users_data.items()
                }
                for client_id, store_users_data in client_store_user_counter.items()
            },
            "end_messages": dict(client_end_messages),
            "last_message_per_sender": dict(last_message_per_sender),
        }
        
        with open(AUXILIARY_FILE, "w") as aux_file:
            json.dump(serializable_data, aux_file)  # Write JSON data
            aux_file.write("\n")  # Add a newline to separate JSON from messages
        os.rename(AUXILIARY_FILE, BACKUP_FILE) # Atomic
        print(f"[categorizer_q4] Worker {WORKER_INDEX} saved state to {BACKUP_FILE} atomically")
    except Exception as e:
        print(f"[categorizer_q4] ERROR saving state to disk: {e}", file=sys.stderr)

def recover_state():
    """
    Recovers the client_store_user_counter, client_end_messages, and transaction rows from disk.
    If the auxiliary file exists, discard it and use the main backup file.
    Detects and deletes corrupted rows during recovery.
    """
    client_store_user_counter = defaultdict(lambda: defaultdict(Counter))
    client_end_messages = defaultdict(int)
    last_message_per_sender = defaultdict(str)
    messages = []

    # Check if the auxiliary file exists
    if os.path.exists(AUXILIARY_FILE):
        print(f"[categorizer_q4] Worker {WORKER_INDEX} detected auxiliary file {AUXILIARY_FILE}, discarding it")
        os.remove(AUXILIARY_FILE)  # Discard the auxiliary file

    if not os.path.exists(BACKUP_FILE):
        print(f"[categorizer_q4] Worker {WORKER_INDEX} no backup file found, starting fresh")
        return client_store_user_counter, client_end_messages, last_message_per_sender, messages

    try:
        with open(BACKUP_FILE, "r") as backup_file:
            lines = backup_file.readlines()

            # Parse the first line as JSON
            try:
                # Separate the JSON dictionary (first part) and appended rows (remaining lines)
                # Load the JSON dictionary from the first part of the file
                json_data = lines[0]
                data = json.loads(json_data)
                # Restore client_store_user_counter
                client_store_user_counter = defaultdict(
                    lambda: defaultdict(Counter),
                    {
                        client_id: defaultdict(
                            Counter,
                            {
                                store_id: Counter({int(user_id): count for user_id, count in user_counter.items()})
                                for store_id, user_counter in store_data.items()
                            }
                        )
                        for client_id, store_data in data["client_store_user_counter"].items()
                    }
                )
                # Restore end message per client
                client_end_messages = defaultdict(int, data["end_messages"])
                # Restore last message IDs
                last_message_per_sender = defaultdict(str, data["last_message_per_sender"])

                print(f"[categorizer_q4] Successfully recovered state from JSON in {BACKUP_FILE}")
            except json.JSONDecodeError as e:
                print(f"[categorizer_q4] ERROR parsing JSON from backup file: {e}", file=sys.stderr)
                return client_store_user_counter, client_end_messages, last_message_per_sender, messages

            # Process remaining lines as messages
            for line in lines[1:]:
                if not line.endswith('\n'):
                    print(f"[categorizer_q4] Corrupted row detected and skipped: {line}", file=sys.stderr)
                    continue
                messages.append(base64.b64decode(line.strip()))

    except Exception as e:
        print(f"[categorizer_q4] ERROR recovering state from {BACKUP_FILE}: {e}", file=sys.stderr)

    return client_store_user_counter, client_end_messages, last_message_per_sender, messages


def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
      
    health_check_receiver = HealthCheckReceiver()
    health_check_receiver.start()

    # Check if a backup file exists
    if not os.path.exists(BACKUP_FILE):
        print("[categorizer_q4] No backup file found. Waiting for RabbitMQ to be ready...", flush=True)
        time.sleep(30)  # Wait for RabbitMQ to be ready
    else:
        print("[categorizer_q4] Backup file found. Skipping RabbitMQ wait.", flush=True)

    listen_for_transactions()
    print("[categorizer_q4] All clients processed successfully.")

if __name__ == "__main__":
    main()