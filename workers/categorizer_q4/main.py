import os
import signal
import sys
from collections import defaultdict, Counter
import time
import json
from common.health_check_receiver import HealthCheckReceiver
from common.protocol import build_message, parse_message, row_to_dict
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

receiver_queue = None
birthday_dict_queue = None

# Global variable for client_store_user_counter
global_client_store_user_counter = defaultdict(lambda: defaultdict(Counter))

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(receiver_queue)
    _close_queue(birthday_dict_queue)

def init(filename="backup_data.txt"):
    """
    Initializes the global client_store_user_counter dictionary and client_end_messages by:
    1. Loading the dictionary from the backup_data file (if it exists).
    2. Updating the dictionary with rows appended after the backup.
    """
    global global_client_store_user_counter, client_end_messages

    client_end_messages = defaultdict(int)  # Initialize END message counter

    try:
        # Step 1: Load the dictionary from the backup file
        if os.path.exists(filename):
            with open(filename, "r") as backup_file:
                lines = backup_file.readlines()
            
            # Separate the JSON dictionary (first part) and appended rows (remaining lines)
            if lines:
                # Load the JSON dictionary from the first part of the file
                json_data = lines[0]
                restored_data = json.loads(json_data)
                # Restore client_store_user_counter
                global_client_store_user_counter = defaultdict(
                    lambda: defaultdict(Counter),
                    {
                        client_id: defaultdict(Counter, {
                            store_id: Counter(user_counter)
                            for store_id, user_counter in store_data["store_data"].items()
                        })
                        for client_id, store_data in restored_data.items()
                    }
                )
                # Restore client_end_messages
                client_end_messages = defaultdict(
                    int,
                    {client_id: store_data["end_messages"] for client_id, store_data in restored_data.items()}
                )
                print(f"[categorizer_q4] Successfully restored client_store_user_counter and END messages from {filename}")
            
            # Step 2: Update the dictionary with appended rows
            for row in lines[1:]:  # Skip the first line (JSON dictionary)
                row = row.strip()
                if row:
                    # Parse the row and update the dictionary
                    store_id, user_id, count = row.split(",")
                    count = int(count)
                    for client_id in global_client_store_user_counter:
                        global_client_store_user_counter[client_id][store_id][int(user_id)] += count
            print(f"[categorizer_q4] Updated client_store_user_counter with appended rows from {filename}")
        else:
            print(f"[categorizer_q4] No backup file found. Starting with an empty client_store_user_counter.")
    except Exception as e:
        print(f"[categorizer_q4] ERROR initializing from backup: {e}", file=sys.stderr)

def listen_for_transactions():
    global global_client_store_user_counter
    client_end_messages = defaultdict(int)
    completed_clients = set()
    row_counter = 0  # Counter to track the number of rows processed

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
    fanout_queue = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=FANOUT_EXCHANGE,
        exchange_type='fanout',
        queue_name=RECEIVER_QUEUE
    )

    print(f"[categorizer_q4] Listening for transactions on queue: {RECEIVER_QUEUE} (topic key: {topic_routing_key})")

    def on_message_callback(message: bytes, delivery_tag=None, channel=None):
        nonlocal completed_clients, row_counter
        try:
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            
            # Skip if client already completed
            if client_id in completed_clients:
                print(f"[categorizer_q4] Worker {WORKER_INDEX} ignoring message for already completed client {client_id}")
                return
            
            batch_rows = []  # Collect rows for batch writing
            for row in parsed_message['rows']:
                batch_rows.append(row)
                row_counter += 1

                dic_fields_row = row_to_dict(row, type_of_message)
                store_id = dic_fields_row.get('store_id')
                user_id = dic_fields_row.get('user_id')
                if None not in (store_id, user_id) and store_id != '' and user_id != '':
                    global_client_store_user_counter[client_id][store_id][int(float(user_id))] += 1

            # Write the batch of rows to the file all at once
            if batch_rows:
                with open("backup_data.txt", "a") as backup_file:
                    backup_file.write("\n".join(batch_rows) + "\n")
                print(f"[categorizer_q4] Batch of {len(batch_rows)} rows written to backup_data.txt")

            # Every 500 rows, write the full dictionary to the backup file
            if row_counter % 500 == 0:
                write_backup_data(global_client_store_user_counter, client_end_messages)

            if is_last:
                client_end_messages[client_id] += 1
                print(f"[categorizer_q4] Worker {WORKER_INDEX} received END message {client_end_messages[client_id]}/{NUMBER_OF_YEAR_WORKERS} for client {client_id}")
                
                if client_end_messages[client_id] >= NUMBER_OF_YEAR_WORKERS:
                    print(f"[categorizer_q4] Client {client_id} received all END messages, processing results")
                    completed_clients.add(client_id)
                    
                    # Process and send results for this client
                    send_client_q4_results(client_id, global_client_store_user_counter[client_id])
                    
                    print(f"[categorizer_q4] Client {client_id} processing completed")

            # Send acknowledgment after processing the batch
            if channel and delivery_tag:
                channel.basic_ack(delivery_tag=delivery_tag)
                print(f"[categorizer_q4] Acknowledged message with delivery tag {delivery_tag}")
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
        # fanout_queue.close()

def get_top_users_per_store(store_user_counter, top_n=3):
    # Returns {store_id: [(user_id, purchase_count), ...]}
    top_users = {}
    for store_id, user_counter in store_user_counter.items():
        top_users[store_id] = user_counter.most_common(top_n)
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
        # Get top users per store for this client
        top_users = get_top_users_per_store(store_user_counter, top_n=3)
        # Prepare all rows for all stores in one message
        rows = []
        for store_id, users in top_users.items():
            for user_id, count in users:
                rows.append(f"{store_id},{user_id},{count}")
        # Send a single message with all top users for all stores
        message, _ = build_message(client_id, 4, 1, rows)
        birthday_dict_exchange.send(message, routing_key=f"client.{worker_index}")
        print(f"[categorizer_q4] Worker {WORKER_INDEX} sent ALL top users for client {client_id} to Birthday_Dictionary with routing key client.{worker_index}: {rows}")
        birthday_dict_exchange.close()
    except Exception as e:
        print(f"[categorizer_q4] ERROR sending results for client {client_id}: {e}", file=sys.stderr)
        # Don't raise to avoid stopping other clients

def write_backup_data(client_store_user_counter, client_end_messages, filename="backup_data.txt"):
    """
    Writes the client_store_user_counter dictionary and client_end_messages to a file, overwriting any existing data.
    The dictionary is serialized to JSON format for easy readability and restoration.
    """
    try:
        # Convert defaultdict and Counter objects to regular dictionaries for JSON serialization
        serializable_data = {
            client_id: {
                "store_data": {
                    store_id: dict(user_counter)
                    for store_id, user_counter in store_data.items()
                },
                "end_messages": client_end_messages[client_id]
            }
            for client_id, store_data in client_store_user_counter.items()
        }
        # Write the serialized data to the file
        with open(filename, "w") as backup_file:
            json.dump(serializable_data, backup_file, indent=4)
        print(f"[categorizer_q4] Backup data written to {filename}")
    except Exception as e:
        print(f"[categorizer_q4] ERROR writing backup data: {e}", file=sys.stderr)

def append_to_backup_data(row, filename="backup_data.txt"):
    """
    Appends a single row to the backup_data file.
    Each row is written as a new line in the file.
    """
    try:
        with open(filename, "a") as backup_file:
            backup_file.write(row + "\n")
        print(f"[categorizer_q4] Appended row to {filename}: {row}")
    except Exception as e:
        print(f"[categorizer_q4] ERROR appending row to backup data: {e}", file=sys.stderr)

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    
    health_check_receiver = HealthCheckReceiver()
    health_check_receiver.start()

    time.sleep(30)
    init()  
    listen_for_transactions()
    print("[categorizer_q4] All clients processed successfully.")

if __name__ == "__main__":
    main()