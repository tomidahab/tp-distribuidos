import os
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
    try:
        items_exchange = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name=ITEMS_FANOUT_EXCHANGE,
            exchange_type="fanout",
            queue_name=ITEMS_QUEUE
        )
    except Exception as e:
        print(f"[categorizer_q2] Failed to connect to RabbitMQ fanout exchange for items: {e}", file=sys.stderr)
        return items

    def on_message_callback(message: bytes):
        try:
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']  
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                items.append({'item_id': str(dic_fields_row['item_id']), 'item_name': dic_fields_row['item_name']})

            if is_last:
                print("[categorizer_q2] Received end message, stopping item collection.")
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
    # Dictionary: {(item_id, month): {'count': int, 'sum': float}}
    sales_stats = defaultdict(lambda: {'count': int(0), 'sum': float(0.0)})
    end_messages_received = 0
    
    try:
        print(f"[categorizer_q2] Using topic middleware for queue: {topic_middleware.queue_name}")
        queue = topic_middleware
        print(f"[categorizer_q2] Connected successfully. Listening for sales on topic exchange with routing keys: {getattr(topic_middleware, 'routing_keys', 'N/A')}")
    except Exception as e:
        print(f"[categorizer_q2] Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        return sales_stats

    def on_message_callback(message: bytes):
        nonlocal end_messages_received
        try:
            parsed_message = parse_message(message)
            type_of_message = parsed_message['csv_type']  
            is_last = parsed_message['is_last']
            # print(f"[categorizer_q2] Received transaction message, csv_type: {type_of_message}, is_last: {is_last}, rows: {len(parsed_message['rows'])}")
            
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                item_id = str(dic_fields_row['item_id'])  # Convert to string to ensure consistency
                created_at = dic_fields_row['created_at']
                dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                year = dt.year
                month = dt.month
                profit = float(dic_fields_row.get('subtotal', 0.0))
                sales_stats[(item_id, year, month)]['count'] += 1
                sales_stats[(item_id, year, month)]['sum'] += profit
                # print(f"[categorizer_q2] Updated stats for {key}: {sales_stats[key]}")  # Too verbose
            if is_last:
                end_messages_received += 1
                print(f"[categorizer_q2] Received END message {end_messages_received}/{NUMBER_OF_YEAR_WORKERS} from filter_by_year workers")
                if end_messages_received >= NUMBER_OF_YEAR_WORKERS:
                    print(f"[categorizer_q2] Received all END messages from {NUMBER_OF_YEAR_WORKERS} filter_by_year workers, stopping sales collection.")
                    queue.stop_consuming()
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
    return sales_stats


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

def send_results_to_gateway(results):
    try:
        queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
        message, _ = build_message(0, 2, 1, results)  # csv_type=2 for Q2
        queue.send(message)
        print(f"[categorizer_q2] Worker {WORKER_INDEX} sent {len(results)} results to gateway with is_last=1")
        queue.close()
    except Exception as e:
        print(f"[categorizer_q2] ERROR in send_results_to_gateway: {e}")
        raise e

def main():
    try:
        print("[categorizer_q2] Waiting for RabbitMQ to be ready...")
        time.sleep(30)  # Esperar a que RabbitMQ esté listo
        print("[categorizer_q2] Starting worker...")
        
        # Setup queues and exchanges
        topic_middleware = setup_queue_and_exchanges()
        print("[categorizer_q2] Queues and exchanges setup completed.")
        
        items = listen_for_items()
        print(f"[categorizer_q2] Collected {len(items)} items: {items}")
        
        if not items:
            print("[categorizer_q2] No items received, exiting.")
            return
            
        print(f"[categorizer_q2] Starting to consume sales messages from topic exchange...")
        sales_stats = listen_for_sales(items, topic_middleware)
        print("[categorizer_q2] Final sales stats:")
            
        # if not sales_stats:
        #     print("[categorizer_q2] No sales data received, exiting.")
        #     return
        # NOTE: The worker must send the message even if no sales were recorded, to indicate completion.
        
        results = get_top_products_per_year_month(sales_stats, items)
        print("[categorizer_q2] Results to send to gateway:")
        for line in results:
            print(line)
        send_results_to_gateway(results)
        print("[categorizer_q2] Worker completed successfully.")
        
    except Exception as e:
        print(f"[categorizer_q2] Error in main: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()