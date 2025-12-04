import hashlib
import os
import signal
import sys
import threading
import json
from time import sleep
from datetime import datetime
import uuid
from common.protocol import parse_message, row_to_dict, build_message
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
import common.config as config
from collections import defaultdict
from common.health_check_receiver import HealthCheckReceiver

# Configurable parameters (could be set via env vars or args)
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', os.environ.get('rabbitmq_server_HOST', 'rabbitmq_server'))
RECEIVER_EXCHANGE_T_ITEMS = os.environ.get('RECEIVER_EXCHANGE_T_ITEMS', 'filter_by_year_transaction_items_exchange')
RECEIVER_EXCHANGE_T = os.environ.get('RECEIVER_EXCHANGE_T', 'filter_by_year_transactions_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', '0'))
HOUR_FILTER_EXCHANGE = os.environ.get('HOUR_FILTER_EXCHANGE', 'filter_by_hour_exchange')
NUMBER_OF_HOUR_WORKERS = int(os.environ.get('NUMBER_OF_HOUR_WORKERS', '3'))
NUMBER_OF_YEAR_WORKERS = int(os.environ.get('NUMBER_OF_YEAR_WORKERS', '3'))
NUMBER_OF_Q2_CATEGORIZER_WORKERS = int(os.environ.get('NUMBER_OF_Q2_CATEGORIZER_WORKERS', '3'))
ITEM_CATEGORIZER_QUEUE = os.environ.get('ITEM_CATEGORIZER_QUEUE', 'categorizer_q2_receiver_queue')
FILTER_YEARS = [
    int(y.strip()) for y in os.environ.get('FILTER_YEAR', '2024,2025').split(',')
]
TOPIC_EXCHANGE = os.environ.get('TOPIC_EXCHANGE', 'categorizer_q2_topic_exchange')

# Add these environment variables
CATEGORIZER_Q4_TOPIC_EXCHANGE = os.environ.get('CATEGORIZER_Q4_TOPIC_EXCHANGE', 'categorizer_q4_topic_exchange')
CATEGORIZER_Q4_FANOUT_EXCHANGE = os.environ.get('CATEGORIZER_Q4_FANOUT_EXCHANGE', 'categorizer_q4_fanout_exchange')
CATEGORIZER_Q4_WORKERS = int(os.environ.get('CATEGORIZER_Q4_WORKERS', 3))

# File to persist processed message_ids per sender
PERSISTENCE_DIR = "/app/persistence"

BACKUP_FILE = f"{PERSISTENCE_DIR}/filter_by_year_worker_transactions_{WORKER_INDEX}_backup.txt"
AUXILIARY_FILE = f"{PERSISTENCE_DIR}/filter_by_year_worker_transactions_{WORKER_INDEX}_backup.tmp"

def print_ok(msg: str):
    GREEN = "\n\033[32m"
    RESET = "\033[0m\n"
    print(f"{GREEN}{msg}{RESET}", flush=True)

def print_err(msg: str):
    RED = "\n\033[31m"
    RESET = "\033[0m\n"
    print(f"{RED}{msg}{RESET}", flush=True)

def print_warn(msg: str):
    YELLOW = "\n\033[33m"
    RESET = "\033[0m\n"
    print(f"{YELLOW}{msg}{RESET}", flush=True)

def get_persistence_file(sender, message_type):
    """Get the persistence file path for a specific sender and message type"""
    return f"{PERSISTENCE_DIR}/worker_{WORKER_INDEX}_last_message_sender_{sender}_type_{message_type}.txt"

def get_aux_persistence_file(sender, message_type):
    """Get the persistence file path for a specific sender and message type"""
    return f"{PERSISTENCE_DIR}/worker_{WORKER_INDEX}_last_message_sender_{sender}_type_{message_type}.tmp"

def load_last_processed_message_id(sender, message_type):
    """Load the last processed message_id from disk for a specific sender and message type"""
    try:
        persistence_file = get_persistence_file(sender, message_type)
        if os.path.exists(persistence_file):
            with open(persistence_file, 'r') as f:
                message_id = f.read().strip()
                print(f"[filter_by_year] Worker {WORKER_INDEX} loaded last message_id for sender {sender} type {message_type}: {message_id}", flush=True)
                return message_id
        else:
            print(f"[filter_by_year] Worker {WORKER_INDEX} no persistence file found for sender {sender} type {message_type}, starting fresh", flush=True)
            return None
    except Exception as e:
        print(f"[filter_by_year] Worker {WORKER_INDEX} ERROR loading persistence for sender {sender} type {message_type}: {e}", flush=True)
        return None

def save_last_processed_message_id(sender, message_id, message_type):
    """Save the last processed message_id to disk for a specific sender and message type"""
    try:
        # Create persistence directory if it doesn't exist
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)
        
        # serializable_data = {
        #     "client_amount_counter": dict(client_amount_counter),
        #     "client_end_messages": dict(client_end_messages),
        #     "last_message_per_sender": dict(last_message_per_sender),
        # }
        
        # with open(AUXILIARY_FILE, "w") as aux_file:
        #     json.dump(serializable_data, aux_file)  # Write JSON data
        #     aux_file.write("\n")  # Add a newline to separate JSON from messages
        # os.rename(AUXILIARY_FILE, BACKUP_FILE) # Atomic

        aux_file = get_aux_persistence_file(sender, message_type)
        persistence_file = get_persistence_file(sender, message_type)
        with open(aux_file, 'w') as f:
            f.write(message_id)
        os.rename(aux_file, persistence_file) # Atomic
        print(f"[filter_by_year] Worker {WORKER_INDEX} saved message_id to disk for sender {sender} type {message_type}: {message_id}", flush=True)
    except Exception as e:
        print(f"[filter_by_year] Worker {WORKER_INDEX} ERROR saving persistence for sender {sender} type {message_type}: {e}", flush=True)

def save_transactions_state_to_disk(transaction_last_message, client_hour_counter, duplicated_count):
    try:
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)

        serializable_data = {
            "transaction_last_message": transaction_last_message,
            "client_hour_counter": dict(client_hour_counter),
            "duplicated_count": duplicated_count,
        }
        
        with open(AUXILIARY_FILE, "w") as aux_file:
            json.dump(serializable_data, aux_file)  # Write JSON data
            aux_file.write("\n")  # Add a newline to separate JSON from messages
        os.rename(AUXILIARY_FILE, BACKUP_FILE) # Atomic
        print(f"[filter_by_hour] Worker {WORKER_INDEX} saved state to {BACKUP_FILE} atomically")
    except Exception as e:
        print(f"[filter_by_hour] ERROR saving state to disk: {e}", file=sys.stderr)

def recover_transactions_state():
    transaction_last_message = ""
    client_hour_counter = defaultdict(int)
    duplicated_count = 0

    # Check if the auxiliary file exists
    if os.path.exists(AUXILIARY_FILE):
        print(f"[filter_by_hour] Worker {WORKER_INDEX} detected auxiliary file {AUXILIARY_FILE}, discarding it")
        os.remove(AUXILIARY_FILE)  # Discard the auxiliary file

    if not os.path.exists(BACKUP_FILE):
        print(f"[filter_by_hour] Worker {WORKER_INDEX} no backup file found, starting fresh")
        return transaction_last_message, client_hour_counter, duplicated_count

    try:
        with open(BACKUP_FILE, "r") as backup_file:
            lines = backup_file.readlines()
            try:
                data = json.loads(lines[0])
                transaction_last_message = data["transaction_last_message"]
                client_hour_counter = defaultdict(int, data["client_hour_counter"])

                print(f"[filter_by_hour] Successfully recovered state from JSON in {BACKUP_FILE}")
            except json.JSONDecodeError as e:
                print(f"[filter_by_hour] ERROR parsing JSON from backup file: {e}", file=sys.stderr)
                return transaction_last_message, client_hour_counter, duplicated_count

    except Exception as e:
        print(f"[filter_by_hour] ERROR recovering state from {BACKUP_FILE}: {e}", file=sys.stderr)

    return transaction_last_message, client_hour_counter, duplicated_count

receiver_exchange_t = None
receiver_exchange_t_items = None
hour_filter_exchange = None
item_categorizer_exchange = None
item_categorizer_fanout_exchange = None
categorizer_q3_topic_exchange = None
categorizer_q4_topic_exchange = None
categorizer_q4_fanout_exchange = None

# Global counters for debugging
client_stats = defaultdict(lambda: {
    'transactions_received': 0,
    'transaction_items_received': 0,
    'transactions_rows_received': 0,
    'transaction_items_rows_received': 0,
    'transactions_rows_sent_to_hour': 0,
    'transaction_items_rows_sent_to_q2': 0,
    'transactions_rows_sent_to_q4': 0,
    'transactions_end_received': 0,
    'transaction_items_end_received': 0,
    'hour_worker_counter': 0,  # Per-client counter for hour workers
    'q4_message_counter': 0   # Per-client counter for q4 distribution
})

# Track END messages per client
end_messages_by_client = defaultdict(int)

# Track detailed stats per client

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(receiver_exchange_t)
    _close_queue(receiver_exchange_t_items)
    _close_queue(hour_filter_exchange)
    _close_queue(item_categorizer_exchange)
    _close_queue(item_categorizer_fanout_exchange)
    _close_queue(categorizer_q3_topic_exchange)
    _close_queue(categorizer_q4_topic_exchange)
    _close_queue(categorizer_q4_fanout_exchange)

print(f"[filter_by_year] Worker {WORKER_INDEX} starting with FILTER_YEARS: {FILTER_YEARS}")


def on_message_callback_transactions(message: bytes, hour_filter_exchange, categorizer_q4_topic_exchange, categorizer_q4_fanout_exchange, delivery_tag=None, channel=None):
    global client_stats
    try:
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']

        # Extract ACK information if available
        sender_id = parsed_message.get('sender', '')
        message_id = parsed_message.get('message_id', '')

        # Initialize disk persistence tracking per sender if needed
        if not hasattr(on_message_callback_transactions, '_disk_last_message_id_by_sender'):
            on_message_callback_transactions._disk_last_message_id_by_sender = {}
            on_message_callback_transactions._hour_worker_counter = {}
            on_message_callback_transactions._duplicated_count = 0
        
        # Load last processed message_id from disk for this sender if not loaded yet
        # if sender_id and sender_id not in on_message_callback_transactions._disk_last_message_id_by_sender:
            # loaded = load_last_processed_message_id(sender_id, "transactions")
        transaction_last_message, client_hour_counter, duplicated_count = recover_transactions_state()
        on_message_callback_transactions._disk_last_message_id_by_sender[sender_id] = transaction_last_message
        on_message_callback_transactions._hour_worker_counter = client_hour_counter
        on_message_callback_transactions._duplicated_count = duplicated_count
        print(f"[filter_by_year] Worker {WORKER_INDEX} initialized disk persistence for sender {sender_id} type transactions", flush=True)
        
        # Check if this is a duplicate message from this sender (message we processed before restart)
        if message_id and sender_id and message_id == on_message_callback_transactions._disk_last_message_id_by_sender.get(sender_id):
            print(f"[filter_by_year] Worker {WORKER_INDEX} DUPLICATE message detected from disk persistence for sender {sender_id}, message_id {message_id} type transactions - skipping (container restart recovery)", flush=True)
            on_message_callback_transactions._duplicated_count += 1
            save_transactions_state_to_disk(message_id, on_message_callback_transactions._hour_worker_counter, on_message_callback_transactions._duplicated_count)
            
            # ACK the duplicate message to avoid reprocessing
            if delivery_tag and channel:
                channel.basic_ack(delivery_tag=delivery_tag)
            return

        # Process message - no in-memory duplicate check by client (removed to avoid conflicts)
        if message_id:
            print(f"[filter_by_year] Worker {WORKER_INDEX} processing message_id {message_id} for client {client_id} from sender {sender_id}", flush=True)

        new_rows = []

        # Update stats
        client_stats[client_id]['transactions_received'] += 1
        client_stats[client_id]['transactions_rows_received'] += len(parsed_message['rows'])
        
        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            try:
                created_at = dic_fields_row['created_at']
                msg_year = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").year
                if msg_year in FILTER_YEARS:
                    new_rows.append(row)
            except Exception as e:
                print(f"[transactions] Worker {WORKER_INDEX} Error parsing created_at: {created_at} ({e})", file=sys.stderr)

        if new_rows or is_last: 
            if is_last:
                print(f"[filter_by_year] Worker {WORKER_INDEX} client {client_id}: {len(new_rows)} rows passed year filter, is_last={is_last}")
            
            # Send to filter_by_hour workers using per-client round robin
            if new_rows:
                # Group rows by worker to send in batches using per-client counter
                rows_by_worker = defaultdict(list)
                for i, row in enumerate(new_rows):
                    # Use per-client counter for deterministic distribution
                    # worker_index = (client_stats[client_id]['hour_worker_counter'] + i) % NUMBER_OF_HOUR_WORKERS
                    # worker_index = (on_message_callback_transactions._hour_worker_counter + i) % NUMBER_OF_HOUR_WORKERS
                    # worker_index = (on_message_callback_transactions._hour_worker_counter[client_id] + i) % NUMBER_OF_HOUR_WORKERS
                    # worker_index = (uuid.UUID(message_id.split("_")[0]).int + i) % NUMBER_OF_HOUR_WORKERS
                    # worker_index = int(client_id.split("_")[1]) % NUMBER_OF_HOUR_WORKERS #.
                    worker_index = on_message_callback_transactions._hour_worker_counter[client_id] % NUMBER_OF_HOUR_WORKERS #.
                    # worker_index = WORKER_INDEX % NUMBER_OF_HOUR_WORKERS
                    routing_key = f"hour.{worker_index}"
                    rows_by_worker[routing_key].append(row)
                
                # Update per-client counter (DON'T mod here, just increment)
                # client_stats[client_id]['hour_worker_counter'] += len(new_rows)
                on_message_callback_transactions._hour_worker_counter[client_id] += 1
                # Update stats
                client_stats[client_id]['transactions_rows_sent_to_hour'] += len(new_rows)
                
                # Send batched messages to each worker
                for routing_key, worker_rows in rows_by_worker.items():
                    if worker_rows:
                        outgoing_sender = f"filter_by_year_worker_{WORKER_INDEX}"
                        new_message, _ = build_message(client_id, type_of_message, 0, worker_rows, sender=outgoing_sender)
                        hour_filter_exchange.send(new_message, routing_key=routing_key)

            # Send to categorizer_q4 workers  
            batches = defaultdict(list)
            if new_rows:
                client_stats[client_id]['transactions_rows_sent_to_q4'] += len(new_rows)
            for row in new_rows:
                dic_fields_row = row_to_dict(row, type_of_message)
                store_id = int(dic_fields_row['store_id'])
                routing_key = f"store.{store_id % CATEGORIZER_Q4_WORKERS}"
                batches[routing_key].append(row)

            for routing_key, batch_rows in batches.items():
                if batch_rows:
                    outgoing_sender = f"filter_by_year_worker_{WORKER_INDEX}"
                    q4_message, _ = build_message(client_id, type_of_message, 0, batch_rows, sender=outgoing_sender)
                    categorizer_q4_topic_exchange.send(q4_message, routing_key=routing_key)

        if is_last:
            client_stats[client_id]['transactions_end_received'] += 1
            print(f"[filter_by_year] Worker {WORKER_INDEX} received transactions END message from client {client_id} (total END msgs: {client_stats[client_id]['transactions_end_received']})")
            
            # Send END message to all filter_by_hour workers
            outgoing_sender = f"filter_by_year_worker_{WORKER_INDEX}"
            end_message, _ = build_message(client_id, type_of_message, 1, [], sender=outgoing_sender)
            for i in range(NUMBER_OF_HOUR_WORKERS):
                routing_key = f"hour.{i}"
                hour_filter_exchange.send(end_message, routing_key=routing_key)
                print(f"[filter_by_year] Worker {WORKER_INDEX} Sent END message for client {client_id} to filter_by_hour worker {i} via topic exchange", flush=True)
            
            # Send messages for Q4 workers - ensure all workers get END message
            for worker_id in range(CATEGORIZER_Q4_WORKERS):
                routing_key = f"store.{worker_id}"
                q4_message, _ = build_message(client_id, type_of_message, 1, [], sender=outgoing_sender)
                categorizer_q4_topic_exchange.send(q4_message, routing_key=routing_key)
                print(f"[filter_by_year] Worker {WORKER_INDEX} client {client_id}: Sent END message to categorizer_q4 with routing_key={routing_key}")
                
            # Print summary stats for this client
            stats = client_stats[client_id]
            print(f"[filter_by_year] Worker {WORKER_INDEX} SUMMARY for client {client_id}: transactions_received={stats['transactions_received']}, transactions_rows_received={stats['transactions_rows_received']}, transactions_rows_sent_to_hour={stats['transactions_rows_sent_to_hour']}, transactions_rows_sent_to_q4={stats['transactions_rows_sent_to_q4']}")

    except Exception as e:
        print_err(f"[transactions] Worker {WORKER_INDEX} Error decoding message: {e}")
        if delivery_tag and channel:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
        return

    # CRITICAL: Save message_id to disk AFTER successful message sending to prevent message loss
    if message_id and sender_id:
        # Update in-memory tracking
        on_message_callback_transactions._disk_last_message_id_by_sender[sender_id] = message_id
        # Save to disk for persistence across restarts
        # save_last_processed_message_id(sender_id, message_id, on_message_callback_transactions._hour_worker_counter, "transactions")
        save_transactions_state_to_disk(message_id, on_message_callback_transactions._hour_worker_counter, on_message_callback_transactions._duplicated_count)
        print(f"[filter_by_year] Worker {WORKER_INDEX} saved message_id to disk for sender {sender_id} type transactions: {message_id}", flush=True)
    
    # Manual ACK: Only acknowledge after successful processing and persistence
    if delivery_tag and channel:
        channel.basic_ack(delivery_tag=delivery_tag)
        print(f"[filter_by_year] Worker {WORKER_INDEX} ACK sent for TRANSACTIONS message from client {client_id}, sender {sender_id}", flush=True)

def on_message_callback_t_items(message: bytes, item_categorizer_exchange, item_categorizer_fanout_exchange, delivery_tag=None, channel=None):
    global client_stats
    try:
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']

        # Extract ACK information if available
        sender_id = parsed_message.get('sender', '')
        message_id = parsed_message.get('message_id', '')

        # Initialize disk persistence tracking per sender if needed
        if not hasattr(on_message_callback_t_items, '_disk_last_message_id_by_sender'):
            on_message_callback_t_items._disk_last_message_id_by_sender = {}
        
        # Load last processed message_id from disk for this sender if not loaded yet
        if sender_id and sender_id not in on_message_callback_t_items._disk_last_message_id_by_sender:
            last_message_id = load_last_processed_message_id(sender_id, "transaction_items")
            on_message_callback_t_items._disk_last_message_id_by_sender[sender_id] = last_message_id
            print(f"[filter_by_year] Worker {WORKER_INDEX} initialized disk persistence for T_ITEMS sender {sender_id}", flush=True)
        
        # Check if this is a duplicate message from this sender (message we processed before restart)
        if message_id and sender_id and message_id == on_message_callback_t_items._disk_last_message_id_by_sender.get(sender_id):
            print_err(f"[filter_by_year] Worker {WORKER_INDEX} DUPLICATE T_ITEMS message detected from disk persistence for sender {sender_id}, message_id {message_id} type transaction_items - skipping (container restart recovery)")
            # ACK the duplicate message to avoid reprocessing
            if delivery_tag and channel:
                channel.basic_ack(delivery_tag=delivery_tag)
            return

        # Process message - no in-memory duplicate check by client (removed to avoid conflicts)
        if message_id:
            print(f"[filter_by_year] Worker {WORKER_INDEX} processing t_items message_id {message_id} for client {client_id} from sender {sender_id}", flush=True)

        # Update stats
        client_stats[client_id]['transaction_items_received'] += 1
        client_stats[client_id]['transaction_items_rows_received'] += len(parsed_message['rows'])

        rows_by_month = defaultdict(list)
        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            try:
                created_at = dic_fields_row['created_at']
                msg_year = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").year
                month = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").month
                if msg_year in FILTER_YEARS:
                    rows_by_month[month].append(row)
            except Exception as e:
                print(f"[t_items] Worker {WORKER_INDEX} Error parsing created_at: {dic_fields_row.get('created_at', '')} ({e})", file=sys.stderr)

        for month, rows in rows_by_month.items():
            if rows != []:
                client_stats[client_id]['transaction_items_rows_sent_to_q2'] += len(rows)
                outgoing_sender = f"filter_by_year_worker_{WORKER_INDEX}"
                new_message, _ = build_message(client_id, type_of_message, 0, rows, sender=outgoing_sender)
                routing_key = f"month.{month}"
                item_categorizer_exchange.send(new_message, routing_key=routing_key)

        if is_last == 1:
            client_stats[client_id]['transaction_items_end_received'] += 1
            print(f"[filter_by_year] Worker {WORKER_INDEX} received transaction_items END message from client {client_id} (total END msgs: {client_stats[client_id]['transaction_items_end_received']})")
            
            outgoing_sender = f"filter_by_year_worker_{WORKER_INDEX}"
            end_message, _ = build_message(client_id, type_of_message, is_last, [], sender=outgoing_sender)
            # Send END message to all possible months (1-12) to ensure all categorizer_q2 workers receive it
            for month in range(1, 13):
                routing_key = f"month.{month}"
                item_categorizer_exchange.send(end_message, routing_key=routing_key)
                print(f"[filter_by_year] Worker {WORKER_INDEX} client {client_id}: Sent END message to categorizer_q2 with routing key {routing_key}")
                
            # Print summary stats for this client
            stats = client_stats[client_id]
            print(f"[filter_by_year] Worker {WORKER_INDEX} T_ITEMS SUMMARY for client {client_id}: transaction_items_received={stats['transaction_items_received']}, transaction_items_rows_received={stats['transaction_items_rows_received']}, transaction_items_rows_sent_to_q2={stats['transaction_items_rows_sent_to_q2']}")

    except Exception as e:
        print_err(f"[t_items] Worker {WORKER_INDEX} Error decoding message: {e}")
        if delivery_tag and channel:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
        return

    # CRITICAL: Save message_id to disk AFTER successful message sending to prevent message loss
    if message_id and sender_id:
        # Update in-memory tracking
        on_message_callback_t_items._disk_last_message_id_by_sender[sender_id] = message_id
        # Save to disk for persistence across restarts  
        save_last_processed_message_id(sender_id, message_id, "transaction_items")
        print(f"[filter_by_year] Worker {WORKER_INDEX} saved message_id to disk for sender {sender_id} type transaction_items: {message_id}", flush=True)

    # Manual ACK
    if delivery_tag and channel:
        channel.basic_ack(delivery_tag=delivery_tag)
        print(f"[filter_by_year] Worker {WORKER_INDEX} ACK sent for T_ITEMS message from client {client_id}, sender {sender_id}", flush=True)

def main():
    global receiver_exchange_t
    global receiver_exchange_t_items
    global receiver_exchange_t
    global receiver_exchange_t_items
    global hour_filter_exchange
    global item_categorizer_exchange
    global item_categorizer_fanout_exchange
    global categorizer_q4_topic_exchange
    global categorizer_q4_fanout_exchange

    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    
    health_check_receiver = HealthCheckReceiver()
    health_check_receiver.start()

    print_ok(f"\n[filter_by_year] Worker {WORKER_INDEX} starting...")
    contents = os.listdir(PERSISTENCE_DIR)
    if not contents:
        sleep(config.MIDDLEWARE_UP_TIME)  
    print(f"[filter_by_year] Worker {WORKER_INDEX} Connecting to RabbitMQ at {RABBITMQ_HOST}, exchanges: {RECEIVER_EXCHANGE_T}, {RECEIVER_EXCHANGE_T_ITEMS}, filter years: {FILTER_YEARS}")
    
    # Track END messages for both streams
    transactions_end_received = 0
    transaction_items_end_received = 0
    
    try:
        # Create receiver exchange for transactions
        receiver_exchange_t = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=RECEIVER_EXCHANGE_T,
            exchange_type='topic',
            queue_name=f"filter_by_year_transactions_worker_{WORKER_INDEX}_queue",
            routing_keys=[f"year.{WORKER_INDEX}"],
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to transactions exchange: {RECEIVER_EXCHANGE_T}")
        
        # Create receiver exchange for transaction_items
        receiver_exchange_t_items = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=RECEIVER_EXCHANGE_T_ITEMS,
            exchange_type='topic',
            queue_name=f"filter_by_year_transaction_items_worker_{WORKER_INDEX}_queue",
            routing_keys=[f"year.{WORKER_INDEX}"],
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to transaction_items exchange: {RECEIVER_EXCHANGE_T_ITEMS}")
        
        # Create sender exchanges
        hour_filter_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=HOUR_FILTER_EXCHANGE,
            exchange_type='topic',
            queue_name="",  # Empty since we're only sending
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to hour filter exchange: {HOUR_FILTER_EXCHANGE}")

        item_categorizer_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=TOPIC_EXCHANGE,
            exchange_type='topic',
            queue_name="",  # Empty since we're only sending
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to item categorizer topic exchange: {TOPIC_EXCHANGE}")
        
        FANOUT_EXCHANGE = os.environ.get('FANOUT_EXCHANGE', 'categorizer_q2_fanout_exchange')
        item_categorizer_fanout_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=FANOUT_EXCHANGE,
            exchange_type='fanout',
            queue_name="",  # Empty since we're only sending
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to item categorizer fanout exchange: {FANOUT_EXCHANGE}")
        
        categorizer_q4_topic_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=CATEGORIZER_Q4_TOPIC_EXCHANGE,
            exchange_type='topic',
            queue_name='', 
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to categorizer_q4 topic exchange: {CATEGORIZER_Q4_TOPIC_EXCHANGE}")
        
        categorizer_q4_fanout_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=CATEGORIZER_Q4_FANOUT_EXCHANGE,
            exchange_type='fanout',
            queue_name='', 
        )
        print(f"[filter_by_year] Worker {WORKER_INDEX} Connected to categorizer_q4 fanout exchange: {CATEGORIZER_Q4_FANOUT_EXCHANGE}")
        
    except Exception as e:
        print(f"[filter_by_year] Worker {WORKER_INDEX} Error connecting to RabbitMQ: {e}", file=sys.stderr)
        return

    print(f"[filter_by_year] Worker {WORKER_INDEX} Starting consumer threads...")
    
    # Transaction processing thread
    def consume_transactions():
        nonlocal transactions_end_received
        print(f"[filter_by_year] Worker {WORKER_INDEX} Starting transactions consumer...")
        
        def wrapper(body, delivery_tag = None, channel = None):
            nonlocal transactions_end_received
            try:
                # Parse to check END flag for logging
                parsed_message = parse_message(body)
                is_last = parsed_message.get('is_last')
                if is_last:
                    transactions_end_received += 1
                    client_id = parsed_message.get('client_id')
                    print(f"[filter_by_year] Worker {WORKER_INDEX} received transactions END message {transactions_end_received} from client {client_id}")

                # Call processing function that handles ACK/NACK
                on_message_callback_transactions(body, hour_filter_exchange, categorizer_q4_topic_exchange, categorizer_q4_fanout_exchange, delivery_tag=delivery_tag, channel=channel)
            except Exception as e:
                print(f"[filter_by_year] Worker {WORKER_INDEX} Error in transactions callback: {e}", file=sys.stderr)

        try:
            receiver_exchange_t.start_consuming(wrapper, auto_ack=True)
        except Exception as e:
            print(f"[filter_by_year] Worker {WORKER_INDEX} Error consuming transactions: {e}", file=sys.stderr)
    
    # Transaction items processing thread
    def consume_transaction_items():
        nonlocal transaction_items_end_received
        print(f"[filter_by_year] Worker {WORKER_INDEX} Starting transaction_items consumer...")
        
        def wrapper(body, delivery_tag = None, channel = None):
            nonlocal transaction_items_end_received
            try:
                parsed_message = parse_message(body)
                is_last = parsed_message.get('is_last')

                # Call processing function that handles ACK/NACK
                on_message_callback_t_items(body, item_categorizer_exchange, item_categorizer_fanout_exchange, delivery_tag=delivery_tag, channel=channel)

                if is_last:
                    transaction_items_end_received += 1
                    client_id = parsed_message.get('client_id')
                    print(f"[filter_by_year] Worker {WORKER_INDEX} received transaction_items END message {transaction_items_end_received} from client {client_id}")

            except Exception as e:
                print(f"[filter_by_year] Worker {WORKER_INDEX} Error in transaction_items callback: {e}", file=sys.stderr)

        try:
            receiver_exchange_t_items.start_consuming(wrapper, auto_ack=True)
        except Exception as e:
            print(f"[filter_by_year] Worker {WORKER_INDEX} Error consuming transaction_items: {e}", file=sys.stderr)
    
    # Start both consumer threads
    t1 = threading.Thread(target=consume_transactions)
    t2 = threading.Thread(target=consume_transaction_items)
    
    print(f"[filter_by_year] Worker {WORKER_INDEX} Starting transactions thread...")
    t1.start()
    print(f"[filter_by_year] Worker {WORKER_INDEX} Starting transaction_items thread...")
    t2.start()
    
    print(f"[filter_by_year] Worker {WORKER_INDEX} All threads started, waiting for messages...")
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print(f"[filter_by_year] Worker {WORKER_INDEX} Stopping...")
        receiver_exchange_t.stop_consuming()
        receiver_exchange_t_items.stop_consuming()
        receiver_exchange_t.close()
        receiver_exchange_t_items.close()
        hour_filter_exchange.close()
        item_categorizer_exchange.close()
        item_categorizer_fanout_exchange.close()
        categorizer_q4_topic_exchange.close()
        categorizer_q4_fanout_exchange.close()

if __name__ == "__main__":
    main()
