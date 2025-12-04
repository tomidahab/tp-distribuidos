import os
import signal
import sys
from collections import defaultdict
import time
from datetime import datetime
from common import config
import json
import base64

from common.health_check_receiver import HealthCheckReceiver
# Debug startup
print("[categorizer_q3] STARTING UP - Basic imports done", flush=True)

# Debug imports
try:
    print("[categorizer_q3] Attempting to import common modules...", flush=True)
    from common.protocol import parse_message, row_to_dict, build_message, CSV_TYPES_REVERSE,create_response_message
    from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
    print("[categorizer_q3] Successfully imported common modules", flush=True)
except ImportError as e:
    print(f"[categorizer_q3] IMPORT ERROR: {e}", flush=True)
    sys.exit(1)
except Exception as e:
    print(f"[categorizer_q3] UNEXPECTED IMPORT ERROR: {e}", flush=True)
    sys.exit(1)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_EXCHANGE = os.environ.get('RECEIVER_EXCHANGE', 'categorizer_q3_exchange')
FANOUT_EXCHANGE = os.environ.get('FANOUT_EXCHANGE', 'categorizer_q3_fanout_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', '0'))
GATEWAY_QUEUE = os.environ.get('GATEWAY_QUEUE', 'query3_result_receiver_queue')
NUMBER_OF_HOUR_WORKERS = int(os.environ.get('NUMBER_OF_HOUR_WORKERS', '4'))

# Parse assigned semesters from environment variable
assigned_semesters_str = os.environ.get('ASSIGNED_SEMESTERS', '')
ASSIGNED_SEMESTERS = [s.strip() for s in assigned_semesters_str.split(',') if s.strip()]

print(f"[categorizer_q3] Worker {WORKER_INDEX} assigned semesters: {ASSIGNED_SEMESTERS}", flush=True)

# Create semester mapping from environment configuration
SEMESTER_MAPPING = {}
if ASSIGNED_SEMESTERS:
    SEMESTER_MAPPING[WORKER_INDEX] = ASSIGNED_SEMESTERS
else:
    # Fallback to hardcoded mapping if environment variable not set
    SEMESTER_MAPPING = {
        0: ['semester.2024-1', 'semester.2024-2'],  # Worker 0: second semesters
        1: ['semester.2025-2', 'semester.2025-1']   # Worker 1: first semesters
    }

topic_middleware = None
gateway_result_queue = None

# File paths for backup
PERSISTENCE_DIR = "/app/persistence"
BACKUP_FILE = f"{PERSISTENCE_DIR}/categorizer_q3_worker_{WORKER_INDEX}_backup.txt"
AUXILIARY_FILE = f"{PERSISTENCE_DIR}/categorizer_q3_worker_{WORKER_INDEX}_backup.tmp"


def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(topic_middleware)
    _close_queue(gateway_result_queue)

def get_semester(month):
    return 1 if 1 <= month <= 6 else 2

def listen_for_transactions():
    """
    Listens for transactions and processes recovered messages from disk.
    """
    # Recover state from disk
    client_semester_store_stats, client_end_messages, last_message_per_sender, recovered_messages = recover_state_from_disk()
    processed_rows = 0
    completed_clients = set()
    processed_message_ids = set()

    # Process recovered messages
    for message in recovered_messages:
        try:
            # Use the same logic as on_message_callback to process recovered messages
            parsed_message = parse_message(message)
            message_id = parsed_message.get('message_id')
            if message_id in processed_message_ids:
                continue  # Skip already processed messages

            # Mark the message as processed
            processed_message_ids.add(message_id)

            # Process the message
            type_of_message = parsed_message['csv_type']
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']

            # Handle END messages
            if is_last:
                client_end_messages[client_id] += 1
                if client_end_messages[client_id] >= NUMBER_OF_HOUR_WORKERS:
                    if client_id not in completed_clients:
                        completed_clients.add(client_id)

            # Skip if client already completed
            if client_id in completed_clients:
                continue

            # Process rows in the message
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                created_at = dic_fields_row['created_at']
                datetime_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                year = str(datetime_obj.year)
                month = datetime_obj.month
                semester = str(get_semester(month))
                store_id = str(dic_fields_row['store_id'])
                payment_value = float(dic_fields_row.get('final_amount', 0.0))

                if payment_value > 0:
                    key = (year, semester, store_id)
                    client_semester_store_stats[client_id][key] += payment_value
                    processed_rows += 1

            last_message_per_sender[parsed_message['sender']] = message_id
        except Exception as e:
            print(f"[categorizer_q3] ERROR processing recovered message: {e}", file=sys.stderr)

    # Get routing keys for this worker
    worker_routing_keys = SEMESTER_MAPPING.get(WORKER_INDEX, [])
    if not worker_routing_keys:
        print(f"[categorizer_q3] No routing keys for worker {WORKER_INDEX}", file=sys.stderr)
        return client_semester_store_stats

    print(f"[categorizer_q3] Worker {WORKER_INDEX} will handle routing keys: {worker_routing_keys}", flush=True)
    print(f"[categorizer_q3] About to create MessageMiddlewareExchange with exchange: {RECEIVER_EXCHANGE}", flush=True)
    
    try:
        print(f"[categorizer_q3] Creating topic MessageMiddlewareExchange for data messages...", flush=True)
        global topic_middleware
        topic_middleware = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=RECEIVER_EXCHANGE,
            exchange_type='topic',
            queue_name=f"categorizer_q3_worker_{WORKER_INDEX}_queue",
            routing_keys=worker_routing_keys
        )
        print(f"[categorizer_q3] Successfully created topic MessageMiddlewareExchange", flush=True)

        print(f"[categorizer_q3] Creating fanout MessageMiddlewareExchange for END messages using SAME queue...", flush=True)
        fanout_middleware = MessageMiddlewareExchange(
            host=RABBITMQ_HOST, 
            exchange_name=FANOUT_EXCHANGE, 
            exchange_type='fanout',
            queue_name=f"categorizer_q3_worker_{WORKER_INDEX}_queue",  # SAME queue as topic exchange
            routing_keys=[]  # Fanout doesn't use routing keys
        )
        print(f"[categorizer_q3] Successfully created fanout MessageMiddlewareExchange", flush=True)

    except Exception as e:
        print(f"[categorizer_q3] Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        return client_semester_store_stats
    
    print(f"[categorizer_q3] Worker {WORKER_INDEX} listening for transactions on exchange: {RECEIVER_EXCHANGE} with routing keys: {worker_routing_keys}")

    def on_message_callback(message: bytes, delivery_tag=None, channel=None):
        nonlocal processed_rows, completed_clients, last_message_per_sender
        try:
            parsed_message = parse_message(message)
            sender_id = parsed_message['sender']
            message_id = parsed_message.get('message_id')  # Assume each message has a unique 'message_id'
            type_of_message = parsed_message['csv_type']
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']

            # Check if the message has already been processed
            if last_message_per_sender[sender_id] == message_id:
                # print(f"[categorizer_q3] Worker {WORKER_INDEX} ignoring repeated message with ID: {message_id}")
                if channel and delivery_tag:
                    channel.basic_ack(delivery_tag=delivery_tag)  # Acknowledge the message
                return

            # Mark the message as processed
            last_message_per_sender[sender_id] = message_id

            # Handle END messages
            if is_last:
                client_end_messages[client_id] += 1
                print(f"[categorizer_q3] Worker {WORKER_INDEX} received END message {client_end_messages[client_id]}/{NUMBER_OF_HOUR_WORKERS} for client {client_id}", flush=True)
                
                # Save state after receiving END message to preserve END message count
                save_state_to_disk(client_semester_store_stats, client_end_messages, last_message_per_sender)
                
                if client_end_messages[client_id] >= NUMBER_OF_HOUR_WORKERS:
                    if client_id not in completed_clients:
                        completed_clients.add(client_id)
                        send_client_results(client_id, client_semester_store_stats[client_id])
                        print(f"[categorizer_q3] Client {client_id} processing completed, sending results")

            # Skip if client already completed
            if client_id in completed_clients:
                return

            # Process rows
            batch_rows = []
            for row in parsed_message['rows']:
                batch_rows.append(row)
                dic_fields_row = row_to_dict(row, type_of_message)
                created_at = dic_fields_row['created_at']
                datetime_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                year = str(datetime_obj.year)
                month = datetime_obj.month
                semester = str(get_semester(month))
                store_id = str(dic_fields_row['store_id'])
                payment_value = float(dic_fields_row.get('final_amount', 0.0))

                if payment_value > 0:
                    key = (year, semester, store_id)
                    client_semester_store_stats[client_id][key] += payment_value
                    processed_rows += 1


            # Append the entire message to disk
            append_message_to_disk(message)

            # Save state every 100 rows
            if processed_rows >= 100:
                save_state_to_disk(client_semester_store_stats, client_end_messages, last_message_per_sender)
                processed_rows = 0

            # Send acknowledgment after successful processing
            if channel and delivery_tag:
                channel.basic_ack(delivery_tag=delivery_tag)
                print(f"[categorizer_q3] Worker {WORKER_INDEX} acknowledged message")

        except Exception as e:
            print(f"[categorizer_q3] ERROR processing message: {e}", file=sys.stderr)
            # Optionally, reject the message if processing fails
            if channel and delivery_tag:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                print(f"[categorizer_q3] Worker {WORKER_INDEX} rejected message")

    try:
        topic_middleware.start_consuming(on_message_callback)
    except Exception as e:
        print(f"[categorizer_q3] ERROR while consuming messages: {e}", file=sys.stderr)
    finally:
        topic_middleware.close()

    return client_semester_store_stats

def send_client_results(client_id, semester_store_stats):
    """Send results for specific client to gateway"""
    try:
        global gateway_result_queue
        if gateway_result_queue is None:
            gateway_result_queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
        
        # Convert to CSV format like Q1
        csv_lines = []
        for (year, semester, store_id), total_payment in semester_store_stats.items():
            csv_line = f"{year},{semester},{store_id},{total_payment}"
            csv_lines.append(csv_line)
            
        # Send as Q3-style message with client_id
        message, _ = build_message(client_id, 3, 1, csv_lines)  # csv_type=3 for Q3, is_last=1 (final message)
        gateway_result_queue.send(message)
        print(f"[categorizer_q3] Worker {WORKER_INDEX} sent {len(csv_lines)} results for client {client_id} to gateway")
            
    except Exception as e:
        print(f"[categorizer_q3] ERROR sending results for client {client_id}: {e}", file=sys.stderr)
        # Don't raise to avoid stopping other clients

def send_results_to_gateway(semester_store_stats):
    try:
        global gateway_result_queue
        gateway_result_queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
        
        # Convert to CSV format like Q1
        csv_lines = []
        for (year, semester, store_id), total_payment in semester_store_stats.items():
            csv_line = f"{year},{semester},{store_id},{total_payment}"
            csv_lines.append(csv_line)
            
        # Send as Q1-style message with rows and is_last=1
        message, _ = build_message(0, 3, 1, csv_lines)  # client_id=0, csv_type=3, is_last=1 (final message)
        gateway_result_queue.send(message)
        print(f"[categorizer_q3] Sent {len(csv_lines)} results to gateway in batch")
            
        gateway_result_queue.close()
    except Exception as e:
        print(f"[categorizer_q3] ERROR in send_results_to_gateway: {e}")
        raise e

def save_state_to_disk(client_semester_store_stats, client_end_messages, last_message_per_sender):
    """
    Saves the client_semester_store_stats, client_end_messages, and last_message_per_sender to disk atomically.
    Writes the JSON data as the first line of the file.
    """
    try:
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)

        # Prepare the JSON data to save
        serializable_data = {
            "semester_store_stats": {
                client_id: {
                    f"{year},{semester},{store_id}": total_payment
                    for (year, semester, store_id), total_payment in semester_store_stats.items()
                }
                for client_id, semester_store_stats in client_semester_store_stats.items()
            },
            "end_messages": dict(client_end_messages),
            "last_message_per_sender": dict(last_message_per_sender),  # Save last processed message IDs
        }

        # Write the JSON data to the auxiliary file
        with open(AUXILIARY_FILE, "w") as aux_file:
            json.dump(serializable_data, aux_file)  # Write JSON data
            aux_file.write("\n")  # Add a newline to separate JSON from messages

        # Atomically rename the auxiliary file to the backup file
        os.rename(AUXILIARY_FILE, BACKUP_FILE)
        print(f"[categorizer_q3] Worker {WORKER_INDEX} saved state to {BACKUP_FILE} atomically")
    except Exception as e:
        print(f"[categorizer_q3] ERROR saving state to disk: {e}", file=sys.stderr)

def append_transaction_rows_to_disk(rows):
    """
    Appends transaction rows to the backup file, one row per line.
    """
    try:
        with open(BACKUP_FILE, "a") as backup_file:
            for row in rows:
                backup_file.write(row + "\n")  # Write each row on a new line
        print(f"[categorizer_q3] Worker {WORKER_INDEX} appended {len(rows)} rows to {BACKUP_FILE}")
    except Exception as e:
        print(f"[categorizer_q3] ERROR appending transaction rows to disk: {e}", file=sys.stderr)

def append_message_to_disk(message):
    """
    Appends a whole message to the backup file, encoded in base64.
    """
    try:
        is_first_write = not os.path.exists(BACKUP_FILE)  # Check if the file is being created for the first time
        with open(BACKUP_FILE, "a") as backup_file:
            if is_first_write:
                backup_file.write("{}\n")  # Write an empty JSON object as the first line
            encoded_message = base64.b64encode(message).decode("ascii")
            backup_file.write(encoded_message + "\n")  # Write the encoded message on a new line
        print(f"[categorizer_q3] Worker {WORKER_INDEX} appended a message to {BACKUP_FILE}")
    except Exception as e:
        print(f"[categorizer_q3] ERROR appending message to disk: {e}", file=sys.stderr)

def recover_state_from_disk():
    """
    Recovers the client_semester_store_stats, client_end_messages, last_message_per_sender, and messages from disk.
    If the auxiliary file exists, discard it and use the main backup file.
    Detects and deletes corrupted messages during recovery.
    """
    client_semester_store_stats = defaultdict(lambda: defaultdict(float))
    client_end_messages = defaultdict(int)
    last_message_per_sender = defaultdict(str)
    recovered_messages = []

    # Check if the auxiliary file exists
    if os.path.exists(AUXILIARY_FILE):
        print(f"[categorizer_q3] Worker {WORKER_INDEX} detected auxiliary file {AUXILIARY_FILE}, discarding it")
        os.remove(AUXILIARY_FILE)  # Discard the auxiliary file

    if not os.path.exists(BACKUP_FILE):
        print(f"[categorizer_q3] Worker {WORKER_INDEX} no backup file found, starting fresh")
        return client_semester_store_stats, client_end_messages, last_message_per_sender, recovered_messages

    try:
        with open(BACKUP_FILE, "r") as backup_file:
            lines = backup_file.readlines()
            if lines:
                # Parse the first line as JSON
                try:
                    json_data = lines[0].strip()
                    data = json.loads(json_data)
                    # Restore semester store stats
                    client_semester_store_stats = defaultdict(
                        lambda: defaultdict(float),
                        {
                            client_id: {
                                tuple(key.split(",")): total_payment
                                for key, total_payment in semester_store_stats.items()
                            }
                            for client_id, semester_store_stats in data["semester_store_stats"].items()
                        }
                    )
                    # Restore end messages
                    client_end_messages = defaultdict(int, data["end_messages"])
                    # Restore last message IDs
                    last_message_per_sender = defaultdict(str, data["last_message_per_sender"])
                    print(f"[categorizer_q3] Successfully recovered state from JSON in {BACKUP_FILE}")
                except json.JSONDecodeError as e:
                    print(f"[categorizer_q3] ERROR parsing JSON from backup file: {e}", file=sys.stderr)
                    return client_semester_store_stats, client_end_messages, last_message_per_sender, recovered_messages

                # Decode and process remaining lines as messages
                for line in lines[1:]:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        decoded_message = base64.b64decode(line)
                        recovered_messages.append(decoded_message)
                    except Exception as e:
                        print(f"[categorizer_q3] Corrupted message detected and skipped: {line}. Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[categorizer_q3] ERROR recovering state from {BACKUP_FILE}: {e}", file=sys.stderr)

    return client_semester_store_stats, client_end_messages, last_message_per_sender, recovered_messages

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    health_check_receiver = HealthCheckReceiver()
    health_check_receiver.start()
    print("[categorizer_q3] MAIN FUNCTION STARTED", flush=True)
    try:
        # Check if a backup file exists
        if not os.path.exists(f"{PERSISTENCE_DIR}/eee.txt"):
            open(f"{PERSISTENCE_DIR}/eee.txt", "w")
            print("[categorizer_q3] No backup file found. Waiting for RabbitMQ to be ready...", flush=True)
            time.sleep(60)  # Wait for RabbitMQ to be ready

        else:
            print("[categorizer_q3] Backup file found. Skipping RabbitMQ wait.", flush=True)

        print("[categorizer_q3] Starting worker...", flush=True)
        print("[categorizer_q3] About to call listen_for_transactions()...", flush=True)
        
        listen_for_transactions()
        print("[categorizer_q3] listen_for_transactions() completed - all clients processed", flush=True)
        print("[categorizer_q3] Worker completed successfully.", flush=True)
        
    except Exception as e:
        print(f"[categorizer_q3] Error in main: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

if __name__ == "__main__":
    print("[categorizer_q3] Script starting - __name__ == '__main__'", flush=True)
    main()