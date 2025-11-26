import os
import signal
import sys
from collections import defaultdict
import time
from datetime import datetime
import json

from common.health_check_receiver import HealthCheckReceiver
from common.protocol import create_response_message
from common import config
# Debug imports
try:
    from common.protocol import parse_message, row_to_dict, build_message, CSV_TYPES_REVERSE
    from common.middleware import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareExchange
except ImportError as e:
    print(f"[categorizer_q2] IMPORT ERROR: {e}", flush=True)
    sys.exit(1)
except Exception as e:
    print(f"[categorizer_q2] UNEXPECTED IMPORT ERROR: {e}", flush=True)
    sys.exit(1)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
ITEMS_QUEUE = os.environ.get('ITEMS_QUEUE', 'categorizer_q2_items_queue')
RECEIVER_QUEUE = os.environ.get('RECEIVER_QUEUE', 'categorizer_q2_receiver_queue')
GATEWAY_QUEUE = os.environ.get('GATEWAY_QUEUE', 'query2_result_receiver_queue')
TOPIC_EXCHANGE = os.environ.get('TOPIC_EXCHANGE', 'categorizer_q2_topic_exchange')
FANOUT_EXCHANGE = os.environ.get('FANOUT_EXCHANGE', 'categorizer_q2_fanout_exchange')
ITEMS_FANOUT_EXCHANGE = os.environ.get('ITEMS_FANOUT_EXCHANGE', 'categorizer_q2_items_fanout_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', 0))
TOTAL_WORKERS = int(os.environ.get('TOTAL_WORKERS', 1))
NUMBER_OF_YEAR_WORKERS = int(os.environ.get('NUMBER_OF_YEAR_WORKERS', '3'))

topic_middleware = None
items_exchange = None
gateway_result_queue = None

# Track detailed stats per client
client_stats = defaultdict(lambda: {
    'transactions_messages_received': 0,
    'transaction_items_messages_received': 0,
    'transactions_rows_received': 0,
    'transaction_items_rows_received': 0,
    'transactions_end_received': 0,
    'transaction_items_end_received': 0
})

# Global variables for dictionaries
global_client_sales_stats = defaultdict(lambda: defaultdict(lambda: {'count': int(0), 'sum': float(0.0)}))
global_client_end_messages = defaultdict(int)

# File paths for backup - worker specific in persistence directory
PERSISTENCE_DIR = "/app/persistence"
BACKUP_FILE = f"{PERSISTENCE_DIR}/categorizer_q2_worker_{WORKER_INDEX}_backup.txt"
MENU_ITEMS_FILE = f"{PERSISTENCE_DIR}/menu_items_worker_{WORKER_INDEX}_backup.txt"

# Message counter for persistence triggers
message_counter = 0

# Track processed messages to avoid duplicates during recovery
processed_message_ids = set()

# Statistics for debugging
total_messages_received = 0
end_messages_by_client = defaultdict(int)  # client_id -> count of END messages
end_messages_by_sender = defaultdict(int)  # sender -> count of END messages
messages_by_client = defaultdict(int)      # client_id -> total messages

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(topic_middleware)
    _close_queue(items_exchange)
    _close_queue(gateway_result_queue)

def get_months_for_worker(worker_index, total_workers):
    months = list(range(1, 13))
    chunk_size = (len(months) + total_workers - 1) // total_workers
    start = worker_index * chunk_size
    end = start + chunk_size
    return months[start:end]

def setup_queue_and_exchanges():
    months = get_months_for_worker(WORKER_INDEX, TOTAL_WORKERS)
    topic_keys = [f"month.{month}" for month in months]

    topic_middleware = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=TOPIC_EXCHANGE,
        exchange_type='topic',
        queue_name=RECEIVER_QUEUE,
        routing_keys=topic_keys
    )

    print(f"[categorizer_q2] Worker {WORKER_INDEX} assigned months: {months}")
    print("topic_keys:", topic_keys)

    _fanout_middleware = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=FANOUT_EXCHANGE,
        exchange_type='fanout',
        queue_name=RECEIVER_QUEUE
    )

    _fanout_middleware_items = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=ITEMS_FANOUT_EXCHANGE,
        exchange_type='fanout',
        queue_name=ITEMS_QUEUE
    )

    return topic_middleware

def listen_for_items():
    items = []
    clients_ended = set()  # Track which clients have sent END messages
    expected_clients = 2  # We expect 2 clients: client_1 and client_2
    
    try:
        global items_exchange
        # Each worker needs its own unique queue for fanout exchange
        unique_items_queue = f"{ITEMS_QUEUE}_worker_{WORKER_INDEX}"
        items_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=ITEMS_FANOUT_EXCHANGE,
            exchange_type="fanout",
            queue_name=unique_items_queue  # Unique queue per worker
        )
    except Exception as e:
        print(f"[categorizer_q2] Failed to connect to RabbitMQ fanout exchange for items: {e}", file=sys.stderr)
        return items

    def on_message_callback(message: bytes):
        global client_stats
        try:
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']  
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            
            client_stats[client_id]['transaction_items_messages_received'] += 1
            client_stats[client_id]['transaction_items_rows_received'] += len(parsed_message['rows'])
            
            #print(f"[categorizer_q2] Worker {WORKER_INDEX} received items message from client {client_id} with {len(parsed_message['rows'])} rows, is_last={is_last}")
            
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                items.append({'item_id': str(dic_fields_row['item_id']), 'item_name': dic_fields_row['item_name']})

            if is_last:
                client_stats[client_id]['transaction_items_end_received'] += 1
                clients_ended.add(client_id)
                print(f"[categorizer_q2] Worker {WORKER_INDEX} received items END message from client {client_id}, clients ended: {len(clients_ended)}/{expected_clients}")
                
                # Only stop consuming when ALL clients have sent END messages
                if len(clients_ended) >= expected_clients:
                    print(f"[categorizer_q2] Worker {WORKER_INDEX} received END from all {expected_clients} clients, stopping item collection.")
                    items_exchange.stop_consuming()
        except Exception as e:
            print(f"[categorizer_q2] Error processing item message: {e}", file=sys.stderr)

    try:
        print("[categorizer_q2] Starting to consume menu items from fanout exchange...")
        items_exchange.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q2] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q2] Message error in middleware.", file=sys.stderr)
    except Exception as e:
        print(f"[categorizer_q2] Unexpected error while consuming: {e}", file=sys.stderr)
    finally:
        items_exchange.close()
    return items

def save_state_to_disk():
    """
    Saves the complete state (dictionaries) and clears any pending messages.
    This rewrites the entire file with just the current state.
    """
    global global_client_sales_stats, global_client_end_messages
    
    try:
        # Create persistence directory if it doesn't exist
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)
        
        serializable_data = {
            "sales_stats": {
                client_id: {
                    f"{item_id},{year},{month}": stats
                    for (item_id, year, month), stats in sales_stats.items()
                }
                for client_id, sales_stats in global_client_sales_stats.items()
            },
            "end_messages": dict(global_client_end_messages)
        }
        
        # Rewrite entire file with current state
        with open(BACKUP_FILE, "w") as backup_file:
            json.dump(serializable_data, backup_file, indent=4)
            # keep a newline so appended messages start on the next line
            backup_file.write("\n")

        #print(f"[categorizer_q2] Worker {WORKER_INDEX} saved complete state to {BACKUP_FILE}")
    except Exception as e:
        print(f"[categorizer_q2] ERROR saving state to disk: {e}", file=sys.stderr)
        raise e

def append_message_to_disk(parsed_message):
    """
    Appends a processed message to the backup file for recovery.
    """
    try:
        # Create persistence directory if it doesn't exist
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)
        
        with open(BACKUP_FILE, "a") as backup_file:
            json.dump(parsed_message, backup_file)
            backup_file.write("\n")
        #print(f"[categorizer_q2] Worker {WORKER_INDEX} appended message to disk")
    except Exception as e:
        print(f"[categorizer_q2] ERROR appending message to disk: {e}", file=sys.stderr)
        raise e

def save_menu_items_to_disk(items):
    """
    Saves the menu items to a separate file.
    """
    try:
        # Create persistence directory if it doesn't exist
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)
        
        with open(MENU_ITEMS_FILE, "w") as items_file:
            json.dump(items, items_file, indent=4)
        print(f"[categorizer_q2] Worker {WORKER_INDEX} saved menu items to {MENU_ITEMS_FILE}")
    except Exception as e:
        print(f"[categorizer_q2] ERROR saving menu items to disk: {e}", file=sys.stderr)

def recover_state_from_disk():
    """
    Recovers the state from disk by:
    1. Finding the last '}' to locate the end of the JSON state
    2. Loading the state dictionary 
    3. Processing any pending messages that came after the last state save
    4. Returns the items and count of processed messages
    """
    global global_client_sales_stats, global_client_end_messages, message_counter, processed_message_ids
    
    pending_messages = []
    items = []
    
    # First try to recover items
    if os.path.exists(MENU_ITEMS_FILE):
        try:
            with open(MENU_ITEMS_FILE, "r") as items_file:
                items = json.load(items_file)
                print(f"[categorizer_q2] Worker {WORKER_INDEX} recovered {len(items)} menu items")
        except Exception as e:
            print(f"[categorizer_q2] ERROR recovering menu items: {e}", file=sys.stderr)
    
    # Now recover state and pending messages
    if not os.path.exists(BACKUP_FILE):
        print(f"[categorizer_q2] Worker {WORKER_INDEX} no backup file found, starting fresh")
        return items, 0
    
    try:
        with open(BACKUP_FILE, "r") as backup_file:
            content = backup_file.read()
            
        if not content.strip():
            print(f"[categorizer_q2] Worker {WORKER_INDEX} backup file is empty, starting fresh")
            return items, 0
        
        # Find the last '}' to locate the end of the JSON state
        last_brace_idx = content.rfind('}')
        if last_brace_idx == -1:
            print(f"[categorizer_q2] Worker {WORKER_INDEX} no valid JSON state found")
            return items, 0
        
        # Extract JSON state and pending messages
        json_part = content[:last_brace_idx + 1]
        messages_part = content[last_brace_idx + 1:].strip()
        
        # Load the state
        state_data = json.loads(json_part)

        # Normalize and restore sales stats (ensure year/month are ints)
        recovered_sales = {}
        for client_id, sales_stats in state_data.get("sales_stats", {}).items():
            recovered_sales[client_id] = {}
            for key, stats in sales_stats.items():
                try:
                    item_id, year_s, month_s = key.split(",")
                    recovered_key = (str(item_id), int(year_s), int(month_s))
                except Exception:
                    # fallback: keep as tuple of strings
                    recovered_key = tuple(key.split(","))
                recovered_sales[client_id][recovered_key] = stats

        global_client_sales_stats = defaultdict(
            lambda: defaultdict(lambda: {'count': int(0), 'sum': float(0.0)}),
            recovered_sales
        )
        
        # Restore end messages
        global_client_end_messages = defaultdict(int, state_data["end_messages"])
        
        print(f"📂 [WORKER {WORKER_INDEX}] recovered state from disk - END messages: {dict(global_client_end_messages)}")
        
        # Process pending messages ONLY if there are any
        if messages_part:
            message_lines = [line.strip() for line in messages_part.split('\n') if line.strip()]
            processed_count = 0
            recovered_end_by_client = defaultdict(int)
            recovered_end_by_sender = defaultdict(int)
            
            for line in message_lines:
                try:
                    parsed_message = json.loads(line)
                    # Add to processed set to avoid reprocessing
                    message_id = parsed_message.get('message_id', '')
                    sender = parsed_message.get('sender', 'unknown')
                    client_id = parsed_message.get('client_id', 'unknown')
                    processed_message_ids.add(f"{sender}:{message_id}")
                    
                    # Count END messages during recovery
                    if parsed_message.get('is_last', False):
                        recovered_end_by_client[client_id] += 1
                        recovered_end_by_sender[sender] += 1
                    
                    process_recovered_message(parsed_message)
                    processed_count += 1
                except Exception as e:
                    print(f"[categorizer_q2] ERROR processing recovered message: {e}", file=sys.stderr)
            
            total_recovered_ends = sum(recovered_end_by_client.values())
            print(f"📦 [WORKER {WORKER_INDEX}] processed {processed_count} pending messages ({total_recovered_ends} were END messages)")
            print(f"📦 [WORKER {WORKER_INDEX}] RECOVERY STATS - END by client: {dict(recovered_end_by_client)}, END by sender: {dict(recovered_end_by_sender)}")
            
            # Reset message counter to continue from where we left off
            message_counter = processed_count
            
            # Save clean state after processing pending messages (this removes the pending messages)
            save_state_to_disk()
            
            return items, processed_count
        else:
            # No pending messages, just use the recovered state
            message_counter = 0
            print(f"📦 [WORKER {WORKER_INDEX}] no pending messages to process")
        
        return items, 0
        
    except Exception as e:
        print(f"[categorizer_q2] ERROR recovering from disk: {e}", file=sys.stderr)
        return items, 0

def process_recovered_message(parsed_message):
    """
    Process a recovered message from disk to rebuild state.
    This is similar to the main processing but without disk operations.
    """
    global global_client_sales_stats, global_client_end_messages
    
    type_of_message = parsed_message['csv_type']
    client_id = parsed_message['client_id']
    is_last = parsed_message['is_last']
    
    # Process each row in the message
    for row in parsed_message['rows']:
        dic_fields_row = row_to_dict(row, type_of_message)
        item_id = str(dic_fields_row['item_id'])
        created_at = dic_fields_row['created_at']
        dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
        year = dt.year
        month = dt.month
        profit = float(dic_fields_row.get('subtotal', 0.0))
        
        # Update sales stats
        global_client_sales_stats[client_id][(item_id, year, month)]['count'] += 1
        global_client_sales_stats[client_id][(item_id, year, month)]['sum'] += profit
    
    # NO increment END messages here - they are already included in the recovered state JSON
    # The END message count is restored from the JSON state, not from processing pending messages

def listen_for_sales(items, topic_middleware):
    global global_client_sales_stats, global_client_end_messages, message_counter

    client_end_messages = global_client_end_messages
    completed_clients = set()

    def on_message_callback(message: bytes, delivery_tag, channel):
        global message_counter, processed_message_ids, total_messages_received, end_messages_by_client, end_messages_by_sender, messages_by_client
        nonlocal completed_clients
        
        total_messages_received += 1
        
        try:
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            message_id = parsed_message.get('message_id', '')
            sender = parsed_message.get('sender', 'unknown')

            # Count messages per client
            messages_by_client[client_id] += 1

            # Skip if already processed (duplicate detection)
            duplicate_key = f"{sender}:{message_id}"
            if duplicate_key in processed_message_ids:
                # print(f"[categorizer_q2] Worker {WORKER_INDEX} DUPLICATE message detected: {duplicate_key} - skipping")
                channel.basic_ack(delivery_tag=delivery_tag)
                return

            # Skip if client already completed
            if client_id in completed_clients:
                # print(f"[categorizer_q2] Worker {WORKER_INDEX} ignoring message for already completed client {client_id}")
                channel.basic_ack(delivery_tag=delivery_tag)
                return

            # Process message: update sales statistics
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                item_id = str(dic_fields_row['item_id'])
                created_at = dic_fields_row['created_at']
                dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                year = dt.year
                month = dt.month
                profit = float(dic_fields_row.get('subtotal', 0.0))
                global_client_sales_stats[client_id][(item_id, year, month)]['count'] += 1
                global_client_sales_stats[client_id][(item_id, year, month)]['sum'] += profit

            # Handle END messages
            if is_last:
                end_messages_by_client[client_id] += 1
                end_messages_by_sender[sender] += 1
                client_end_messages[client_id] += 1
                print(f"🔴 [WORKER {WORKER_INDEX}] RECEIVED END MESSAGE for client {client_id} from sender {sender}")
                print(f"📊 [WORKER {WORKER_INDEX}] STATS - Client {client_id}: END count: {client_end_messages[client_id]}/{NUMBER_OF_YEAR_WORKERS}")
                print(f"📊 [WORKER {WORKER_INDEX}] DETAILED STATS - END by client: {dict(end_messages_by_client)}, END by sender: {dict(end_messages_by_sender)}")
                print(f"📊 [WORKER {WORKER_INDEX}] MESSAGE STATS - Total: {total_messages_received}, By client: {dict(messages_by_client)}")

            # CRITICAL PERSISTENCE FLOW:
            # 1. Mark as processed to avoid duplicates
            processed_message_ids.add(duplicate_key)
            
            # 2. Save message to disk (always)
            append_message_to_disk(parsed_message)
            
            # 3. ACK the message (only after successful disk write)
            channel.basic_ack(delivery_tag=delivery_tag)
            
            # 4. Update message counter
            message_counter += 1
            
            # 5. Save complete state every 100 messages OR on END messages
            if message_counter % 100 == 0 or is_last:
                save_state_to_disk()
                # print(f"[categorizer_q2] Worker {WORKER_INDEX} saved state at message {message_counter}")

            # 6. Check if client is complete (after state is saved)
            if is_last and client_end_messages[client_id] >= NUMBER_OF_YEAR_WORKERS:
                completed_clients.add(client_id)
                send_client_q2_results(client_id, global_client_sales_stats[client_id], items)
                print(f"✅ [WORKER {WORKER_INDEX}] Client {client_id} processing COMPLETED - sending results!")

        except Exception as e:
            print(f"[categorizer_q2] ERROR processing sales message: {e}", file=sys.stderr)
            # Don't ACK on error - message will be requeued
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

    try:
        print("[categorizer_q2] Starting to consume sales messages with manual ACK...")
        # Use manual ACK mode (auto_ack=False)
        topic_middleware.start_consuming(on_message_callback, auto_ack=False)
    except Exception as e:
        print(f"[categorizer_q2] Unexpected error while consuming sales: {e}", file=sys.stderr)
    finally:
        topic_middleware.close()

def get_top_products_per_year_month(sales_stats, items):
    print(f"🐞 [DEBUG] Processing {len(sales_stats)} sales stats with {len(items)} items")
    
    if items:
        print(f"🐞 [DEBUG] Sample item: {items[0]}")
        print(f"🐞 [DEBUG] Item keys: {list(items[0].keys()) if items else 'NO ITEMS'}")
    
    id_to_name = {str(item['item_id']): item['item_name'] for item in items}
    print(f"🐞 [DEBUG] Created mapping for {len(id_to_name)} items")
    
    if sales_stats:
        sample_key = list(sales_stats.keys())[0]
        print(f"🐞 [DEBUG] Sample sales_stats key: {sample_key}, type of item_id: {type(sample_key[0])}")
    
    # {(year, month): [ {item_id, count, sum} ]}
    year_month_stats = defaultdict(list)
    for (item_id, year, month), stats in sales_stats.items():
        year_month_stats[(year, month)].append({
            'item_id': item_id,
            'count': stats['count'],
            'sum': stats['sum']
        })

    results = []
    for (year, month), stats_list in year_month_stats.items():
        if not stats_list:
            continue
        top_count = max(stats_list, key=lambda x: x['count'])
        top_sum = max(stats_list, key=lambda x: x['sum'])
        results.append(f"{year},{month},{top_count['item_id']},{id_to_name.get(top_count['item_id'], 'Unknown')},{top_count['count']},{top_sum['item_id']},{id_to_name.get(top_sum['item_id'], 'Unknown')},{top_sum['sum']}")
    return results

def send_client_q2_results(client_id, sales_stats, items):
    """Send Q2 results for specific client to gateway"""
    try:
        global gateway_result_queue
        if gateway_result_queue is None:
            gateway_result_queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
        
        # Process results for this client
        results = get_top_products_per_year_month(sales_stats, items)
        
        # Send results with client_id
        message, _ = build_message(client_id, 2, 1, results)  # csv_type=2 for Q2
        gateway_result_queue.send(message)
        print(f"[categorizer_q2] Worker {WORKER_INDEX} sent {len(results)} results for client {client_id} to gateway")
        
    except Exception as e:
        print(f"[categorizer_q2] ERROR sending results for client {client_id}: {e}", file=sys.stderr)
        # Don't raise to avoid stopping other clients

def send_results_to_gateway(results):
    try:
        global gateway_result_queue
        gateway_result_queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
        # Send all results with is_last=1 using build_message format
        message, _ = build_message(0, 2, 1, results)  # csv_type=2 for Q2
        gateway_result_queue.send(message)
        print(f"[categorizer_q2] Worker {WORKER_INDEX} sent {len(results)} results to gateway with is_last=1")
        gateway_result_queue.close()
    except Exception as e:
        print(f"[categorizer_q2] ERROR in send_results_to_gateway: {e}")
        raise e

def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)

    health_check_receiver = HealthCheckReceiver()
    health_check_receiver.start()

    try:
        print(f"[categorizer_q2] Worker {WORKER_INDEX} starting...")
        
        # STEP 1: Recover state and process pending messages
        items, processed_count = recover_state_from_disk()
        print(f"🚀 [WORKER {WORKER_INDEX}] STARTUP STATS - END messages in state: {dict(global_client_end_messages)}")
        print(f"🚀 [WORKER {WORKER_INDEX}] STARTUP STATS - Total messages: {total_messages_received}, END by client: {dict(end_messages_by_client)}, END by sender: {dict(end_messages_by_sender)}")
        print(f"[categorizer_q2] Worker {WORKER_INDEX} recovered state, processed {processed_count} pending messages")

        # STEP 2: Wait for RabbitMQ and setup connections FIRST
        print("[categorizer_q2] Waiting for RabbitMQ to be ready...")
        time.sleep(30)  # Esperar a que RabbitMQ esté listo
        
        # Setup queues and exchanges
        global topic_middleware
        topic_middleware = setup_queue_and_exchanges()
        print("[categorizer_q2] Queues and exchanges setup completed.")

        # STEP 3: If no items were recovered, collect them AFTER RabbitMQ is ready
        if not items:
            print(f"[categorizer_q2] Worker {WORKER_INDEX} collecting menu items...")
            items = listen_for_items()
            save_menu_items_to_disk(items)
            print(f"[categorizer_q2] Worker {WORKER_INDEX} collected {len(items)} menu items")
        
        # STEP 4: Start consuming new messages with manual ACK
        listen_for_sales(items, topic_middleware)
        print("[categorizer_q2] All clients processed successfully.")
        
    except Exception as e:
        print(f"[categorizer_q2] Error in main: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()