import os
import sys
from collections import defaultdict
import time
from datetime import datetime

from common.protocol import create_response_message

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
            client_id = parsed_message['client_id']
            is_last = parsed_message['is_last']
            print(f"[categorizer_q2] Received transaction message, csv_type: {type_of_message}, is_last: {is_last}, rows: {len(parsed_message['rows'])}")
            
            for row in parsed_message['rows']:
                dic_fields_row = row_to_dict(row, type_of_message)
                # print(f"[categorizer_q2] Processing row: {dic_fields_row}")  # Too verbose for large files
                item_id = str(dic_fields_row['item_id'])  # Convert to string to ensure consistency
                created_at = dic_fields_row['created_at']
                datetime_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                month = datetime_obj.month
                price = float(dic_fields_row['subtotal'])
                if item_id in [item['item_id'] for item in items]:
                    key = (item_id, month)
                    sales_stats[key]['count'] = int(sales_stats[key]['count']) + 1
                    sales_stats[key]['sum'] = float(sales_stats[key]['sum']) + price
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

def get_top_products_per_month(sales_stats, items):
    
    id_to_name = {item['item_id']: item['item_name'] for item in items}
    # print(f"[categorizer_q2] DEBUG: id_to_name keys: {list(id_to_name.keys())}, types: {[type(k) for k in id_to_name.keys()]}")
    
    month_stats = defaultdict(list)
    for (item_id, month), stats in sales_stats.items():
        # print(f"[categorizer_q2] DEBUG: Processing item_id: {item_id}, type: {type(item_id)}, month: {month}, type: {type(month)}")
        # print(f"[categorizer_q2] DEBUG: stats count: {stats['count']}, type: {type(stats['count'])}, sum: {stats['sum']}, type: {type(stats['sum'])}")
        # Asegurar tipos correctos
        month_stats[month].append({
            'item_id': str(item_id), 
            'count': int(stats['count']), 
            'sum': float(stats['sum'])
        })

    top_by_count = {}
    top_by_sum = {}
    for month, stats_list in month_stats.items():
        # print(f"[categorizer_q2] DEBUG: Processing month: {month}, type: {type(month)}")
        # print(f"[categorizer_q2] DEBUG: stats_list length: {len(stats_list)}")
        
        # Debug each item in stats_list
        # for i, item in enumerate(stats_list):
        #     print(f"[categorizer_q2] DEBUG: stats_list[{i}]: item_id={item['item_id']} (type: {type(item['item_id'])}), count={item['count']} (type: {type(item['count'])}), sum={item['sum']} (type: {type(item['sum'])})")
        
        try:
            top_count = max(stats_list, key=lambda x: x['count'])
            # print(f"[categorizer_q2] DEBUG: top_count successful: item_id: {top_count['item_id']}, type: {type(top_count['item_id'])}")
        except Exception as e:
            print(f"[categorizer_q2] ERROR in max(count): {e}")
            # print(f"[categorizer_q2] DEBUG: All count values: {[x['count'] for x in stats_list]}")
            # print(f"[categorizer_q2] DEBUG: All count types: {[type(x['count']) for x in stats_list]}")
            raise e
            
        try:
            top_sum = max(stats_list, key=lambda x: x['sum'])
            # print(f"[categorizer_q2] DEBUG: top_sum successful: item_id: {top_sum['item_id']}")
        except Exception as e:
            print(f"[categorizer_q2] ERROR in max(sum): {e}")
            # print(f"[categorizer_q2] DEBUG: All sum values: {[x['sum'] for x in stats_list]}")
            # print(f"[categorizer_q2] DEBUG: All sum types: {[type(x['sum']) for x in stats_list]}")
            raise e
        
        # print(f"[categorizer_q2] DEBUG: About to create result dictionaries")
        # print(f"[categorizer_q2] DEBUG: month: {month}, type: {type(month)}")
        # print(f"[categorizer_q2] DEBUG: top_count['item_id']: {top_count['item_id']}, type: {type(top_count['item_id'])}")
        # print(f"[categorizer_q2] DEBUG: top_sum['item_id']: {top_sum['item_id']}, type: {type(top_sum['item_id'])}")
        
        try:
            # print("[categorizer_q2] DEBUG: Creating top_by_count dictionary")
            top_by_count[month] = {
                'item_id': top_count['item_id'],
                'name': id_to_name.get(top_count['item_id'], 'Unknown'),
                'count': top_count['count']
            }
            # print("[categorizer_q2] DEBUG: top_by_count created successfully")
            
            # print("[categorizer_q2] DEBUG: Creating top_by_sum dictionary")
            top_by_sum[month] = {
                'item_id': top_sum['item_id'],
                'name': id_to_name.get(top_sum['item_id'], 'Unknown'),
                'sum': top_sum['sum']
            }
            # print("[categorizer_q2] DEBUG: top_by_sum created successfully")
        except Exception as e:
            print(f"[categorizer_q2] ERROR creating result dictionaries: {e}")
            # print(f"[categorizer_q2] DEBUG: id_to_name: {id_to_name}")
            # print(f"[categorizer_q2] DEBUG: top_count: {top_count}")
            # print(f"[categorizer_q2] DEBUG: top_sum: {top_sum}")
            raise e
    return top_by_count, top_by_sum

def send_results_to_gateway(top_by_count, top_by_sum):
    try:
        # print("[categorizer_q2] DEBUG: Entering send_results_to_gateway")
        queue = MessageMiddlewareQueue(RABBITMQ_HOST, GATEWAY_QUEUE)
        # print("[categorizer_q2] DEBUG: Queue created successfully")
        
        # print(f"[categorizer_q2] DEBUG: top_by_count keys: {list(top_by_count.keys())}, types: {[type(k) for k in top_by_count.keys()]}")
        # print(f"[categorizer_q2] DEBUG: top_by_sum keys: {list(top_by_sum.keys())}, types: {[type(k) for k in top_by_sum.keys()]}")
        
        # Get all unique months from both dictionaries
        # print("[categorizer_q2] DEBUG: About to create set union")
        all_months = set(top_by_count.keys()).union(set(top_by_sum.keys()))
        # print(f"[categorizer_q2] DEBUG: all_months: {list(all_months)}, types: {[type(m) for m in all_months]}")
        
        for month in all_months:
            # print(f"[categorizer_q2] DEBUG: Processing month {month} of type {type(month)}")
            try:
                result = {
                    'month': month,
                    'top_count_item': {
                        'name': top_by_count[month]['name'],
                        'item_id': top_by_count[month]['item_id'],
                        'count': top_by_count[month]['count']
                    },
                    'top_sum_item': {
                        'name': top_by_sum[month]['name'],
                        'item_id': top_by_sum[month]['item_id'],
                        'sum': top_by_sum[month]['sum']
                    }
                }
                # print("[categorizer_q2] DEBUG: Result dictionary created successfully")
                
                # Aca se usa el protocolo para codificar el mensaje de resultado
                # TODO: Implementar encode_result_message en common.protocol
                # print("[categorizer_q2] DEBUG: About to call create_response_message")
                message = create_response_message(2,str(result))
                # print("[categorizer_q2] DEBUG: create_response_message completed successfully")

                queue.send(message)
                print(f"[categorizer_q2] Sent result for month {month} to gateway: {result}")
            except Exception as e:
                print(f"[categorizer_q2] ERROR in month processing loop: {e}")
                raise e
        queue.close()
        # print("[categorizer_q2] DEBUG: send_results_to_gateway completed successfully")
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
        for (item_id, month), stats in sales_stats.items():
            print(f"Item: {item_id}, Month: {month}, Count: {stats['count']}, Sum: {stats['sum']}")
            
        if not sales_stats:
            print("[categorizer_q2] No sales data received, exiting.")
            return
            
        top_by_count, top_by_sum = get_top_products_per_month(sales_stats, items)
        send_results_to_gateway(top_by_count, top_by_sum)
        print("[categorizer_q2] Worker completed successfully.")
        
    except Exception as e:
        print(f"[categorizer_q2] Error in main: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()