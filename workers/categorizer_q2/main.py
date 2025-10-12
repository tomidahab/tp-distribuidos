import os
import signal
import sys
from collections import defaultdict
import time
from datetime import datetime

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

def listen_for_sales(items, topic_middleware):
    # Dictionary per client: {client_id: {(item_id, year, month): {'count': int, 'sum': float}}}
    client_sales_stats = defaultdict(lambda: defaultdict(lambda: {'count': int(0), 'sum': float(0.0)}))
    # Track END messages per client: {client_id: count}
    client_end_messages = defaultdict(int)
    completed_clients = set()
    
    try:
        print(f"[categorizer_q2] Using topic middleware for queue: {topic_middleware.queue_name}")
        queue = topic_middleware
        print(f"[categorizer_q2] Connected successfully. Listening for sales on topic exchange with routing keys: {getattr(topic_middleware, 'routing_keys', 'N/A')}")
    except Exception as e:
        print(f"[categorizer_q2] Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        return client_sales_stats

    def on_message_callback(message: bytes):
        nonlocal completed_clients
        global client_stats
        try:
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']  
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            
            # Skip if client already completed - check BEFORE any processing
            if client_id in completed_clients:
                print(f"[categorizer_q2] Worker {WORKER_INDEX} ignoring message from already completed client {client_id}")
                print(f"[categorizer_q2] Worker {WORKER_INDEX} client {client_id} message is: {parsed_message}")
                return
            
            client_stats[client_id]['transactions_messages_received'] += 1
            client_stats[client_id]['transactions_rows_received'] += len(parsed_message['rows'])
            
            #print(f"[categorizer_q2] Worker {WORKER_INDEX} received transactions message from client {client_id} with {len(parsed_message['rows'])} rows, is_last={is_last} (total msgs: {client_stats[client_id]['transactions_messages_received']}, total rows: {client_stats[client_id]['transactions_rows_received']})")
            
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                item_id = str(dic_fields_row['item_id'])  # Convert to string to ensure consistency
                created_at = dic_fields_row['created_at']
                dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                year = dt.year
                month = dt.month
                profit = float(dic_fields_row.get('subtotal', 0.0))
                client_sales_stats[client_id][(item_id, year, month)]['count'] += 1
                client_sales_stats[client_id][(item_id, year, month)]['sum'] += profit
                
            if is_last:
                client_stats[client_id]['transactions_end_received'] += 1
                client_end_messages[client_id] += 1
                print(f"[categorizer_q2] Worker {WORKER_INDEX} received END message {client_end_messages[client_id]}/{NUMBER_OF_YEAR_WORKERS} for client {client_id}")
                
                # Mark client as completed immediately when all END messages received
                if client_end_messages[client_id] >= NUMBER_OF_YEAR_WORKERS:
                    if client_id not in completed_clients:
                        print(f"[categorizer_q2] Worker {WORKER_INDEX} client {client_id} received all END messages, processing results")
                        completed_clients.add(client_id)
                        
                        # Print summary stats for this client
                        stats = client_stats[client_id]
                        print(f"[categorizer_q2] Worker {WORKER_INDEX} SUMMARY for client {client_id}: transactions_messages_received={stats['transactions_messages_received']}, transactions_rows_received={stats['transactions_rows_received']}, transaction_items_messages_received={stats['transaction_items_messages_received']}, transaction_items_rows_received={stats['transaction_items_rows_received']}")
                        
                        # Process and send results for this client
                        send_client_q2_results(client_id, client_sales_stats[client_id], items)
                        
                        # Don't delete client data yet - keep it for potential debugging
                        # but mark as completed
                        print(f"[categorizer_q2] Worker {WORKER_INDEX} client {client_id} processing completed")
                    
                    # Simple stopping condition: if we've processed some clients and no new messages 
                    # are coming for a while, we can assume all clients are done
                    # For now, let's be more conservative and only stop when explicitly told
        except Exception as e:
            print(f"[categorizer_q2] Error processing sales message: {e}", file=sys.stderr)

    try:
        print("[categorizer_q2] Starting to consume sales messages...")
        queue.start_consuming(on_message_callback)
    except MessageMiddlewareDisconnectedError:
        print("[categorizer_q2] Disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print("[categorizer_q2] Message error in middleware.", file=sys.stderr)
    except Exception as e:
        print(f"[categorizer_q2] Unexpected error while consuming sales: {e}", file=sys.stderr)
    finally:
        queue.close()
    return client_sales_stats


def get_top_products_per_year_month(sales_stats, items):
    id_to_name = {item['item_id']: item['item_name'] for item in items}
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
    try:
        print("[categorizer_q2] Waiting for RabbitMQ to be ready...")
        time.sleep(30)  # Esperar a que RabbitMQ esté listo
        print("[categorizer_q2] Starting worker...")
        
        # Setup queues and exchanges
        global topic_middleware
        topic_middleware = setup_queue_and_exchanges()
        print("[categorizer_q2] Queues and exchanges setup completed.")
        
        items = listen_for_items()
        print(f"[categorizer_q2] Collected {len(items)} items: {items}")
        
        if not items:
            print("[categorizer_q2] No items received, exiting.")
            return
            
        print(f"[categorizer_q2] Starting to consume sales messages from topic exchange...")
        # In multi-client mode, listen_for_sales handles everything including sending results per client
        client_sales_stats = listen_for_sales(items, topic_middleware)
        print("[categorizer_q2] All clients processed successfully.")
        
    except Exception as e:
        print(f"[categorizer_q2] Error in main: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()