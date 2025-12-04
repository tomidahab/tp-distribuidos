import os
import signal
import sys
import json
from collections import defaultdict
from time import sleep
from common.health_check_receiver import HealthCheckReceiver
from common.protocol import parse_message, row_to_dict, build_message
from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
import common.config as config

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq_server')
RECEIVER_EXCHANGE = os.environ.get('RECEIVER_EXCHANGE', 'filter_by_amount_exchange')
WORKER_INDEX = int(os.environ.get('WORKER_INDEX', '0'))
MIN_AMOUNT = float(os.environ.get('MIN_AMOUNT', 15.0))
RESULT_QUEUE = os.environ.get('RESULT_QUEUE', 'query1_result_receiver_queue')
NUMBER_OF_HOUR_WORKERS = int(os.environ.get('NUMBER_OF_HOUR_WORKERS', '3'))

# File to persist processed message_ids per sender
PERSISTENCE_DIR = "/app/persistence"

def get_persistence_file(sender):
    """Get the persistence file path for a specific sender"""
    return f"{PERSISTENCE_DIR}/worker_{WORKER_INDEX}_last_message_sender_{sender}.txt"

# Global variables for statistics
rows_received = 0
rows_sent = 0
end_messages_received = 0

# Track END messages per client: {client_id: count}
client_end_messages = defaultdict(int)
completed_clients = set()

# Track detailed stats per client
client_stats = defaultdict(lambda: {
    'messages_received': 0,
    'rows_received': 0,
    'rows_sent': 0,
    'end_messages_received': 0
})

queue_result = None
topic_middleware = None

def _close_queue(queue):
    if queue:
        queue.close()

def _sigterm_handler(signum, _):
    _close_queue(queue_result)
    _close_queue(topic_middleware)

def load_last_processed_message_id(sender):
    """Load the last processed message_id from disk for a specific sender"""
    try:
        # Create persistence directory if it doesn't exist
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)
        
        persistence_file = get_persistence_file(sender)
        if os.path.exists(persistence_file):
            with open(persistence_file, 'r') as f:
                message_id = f.read().strip()
                print(f"[filter_by_amount] Worker {WORKER_INDEX} loaded last message_id for sender {sender}: {message_id}", flush=True)
                return message_id
        else:
            print(f"[filter_by_amount] Worker {WORKER_INDEX} no persistence file found for sender {sender}, starting fresh", flush=True)
            return None
    except Exception as e:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} ERROR loading persistence for sender {sender}: {e}", flush=True)
        return None

def save_last_processed_message_id(sender, message_id):
    """Save the last processed message_id to disk for a specific sender"""
    try:
        os.makedirs(PERSISTENCE_DIR, exist_ok=True)
        
        persistence_file = get_persistence_file(sender)
        with open(persistence_file, 'w') as f:
            f.write(message_id)
            f.flush()
            os.fsync(f.fileno())
        
        print(f"[filter_by_amount] Worker {WORKER_INDEX} saved message_id to disk for sender {sender}: {message_id}", flush=True)
    except Exception as e:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} ERROR saving message_id to disk for sender {sender}: {e}", flush=True)

def filter_message_by_amount(parsed_message, min_amount: float) -> list:
    try:
        type_of_message = parsed_message['csv_type']
        new_rows = []
        for row in parsed_message['rows']:
            dic_fields_row = row_to_dict(row, type_of_message)
            try:
                amount = float(dic_fields_row['final_amount'])
                if amount >= min_amount:
                    new_rows.append(row)
            except Exception as e:
                print(f"[amount] Error parsing amount: {dic_fields_row.get('amount')} ({e})", file=sys.stderr)
        return new_rows
    except Exception as e:
        print(f"[filter] Error parsing message: {e}", file=sys.stderr)
        return []

def send_ack(sender_id, message_id):
    """Send ACK response to filter_by_hour worker"""
    try:
        # Use sender_id directly to create ACK queue name
        ack_queue_name = f"ack_queue_{sender_id}"
        
        # Create or reuse ACK queue connection
        ack_queue = MessageMiddlewareQueue(RABBITMQ_HOST, ack_queue_name)
        
        # Send ACK message
        ack_message = f"ACK:{message_id}"
        ack_queue.send(ack_message.encode())
        
        # print(f"[filter_by_amount] Worker {WORKER_INDEX} sent ACK to {ack_queue_name} for message {message_id}", flush=True)
        
    except Exception as e:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} ERROR sending ACK for message {message_id}: {e}", flush=True)

def on_message_callback(message: bytes, delivery_tag=None, channel=None):
    global rows_received, rows_sent, client_end_messages, completed_clients, client_stats
    
    try:
        # print(f"[filter_by_amount] Worker {WORKER_INDEX} received a message!", flush=True)
        parsed_message = parse_message(message)
        type_of_message = parsed_message['csv_type']  
        client_id = parsed_message['client_id']
        is_last = parsed_message['is_last']
        
        # Extract message_id and sender for persistence
        message_id = parsed_message.get('message_id', '')
        sender = parsed_message.get('sender', 'unknown')
        
        # Initialize disk persistence tracking per sender if needed
        if not hasattr(on_message_callback, '_disk_last_message_id_by_sender'):
            on_message_callback._disk_last_message_id_by_sender = {}
        
        # Load last processed message_id from disk for this sender if not loaded yet
        if sender not in on_message_callback._disk_last_message_id_by_sender:
            disk_last_message_id = load_last_processed_message_id(sender)
            on_message_callback._disk_last_message_id_by_sender[sender] = disk_last_message_id
            print(f"[filter_by_amount] Worker {WORKER_INDEX} initialized disk persistence for sender {sender}", flush=True)
        
        # Check if this is a duplicate message from this sender (message we processed before restart)
        if message_id and message_id == on_message_callback._disk_last_message_id_by_sender.get(sender):
            print(f"[filter_by_amount] Worker {WORKER_INDEX} DUPLICATE message detected from disk persistence for sender {sender}, message_id {message_id} - skipping (container restart recovery)", flush=True)
            # ACK the duplicate message to avoid reprocessing
            if delivery_tag and channel:
                channel.basic_ack(delivery_tag=delivery_tag)
            return
        
        # # Check if we're stopping AFTER parsing and duplicate detection  
        # if should_stop.is_set():  # Don't process if we're stopping
        #     print(f"[filter_by_amount] Worker {WORKER_INDEX} should_stop detected, saving message_id and ACKing to prevent reprocessing", flush=True)
        #     # Save message_id to prevent reprocessing after restart
        #     if message_id:
        #         on_message_callback._disk_last_message_id_by_sender[sender] = message_id
        #         save_last_processed_message_id(sender, message_id)
        #         print(f"[filter_by_amount] Worker {WORKER_INDEX} saved message_id during shutdown for sender {sender}: {message_id}", flush=True)
        #     # ACK to avoid requeue since we've marked it as processed
        #     if delivery_tag and channel:
        #         channel.basic_ack(delivery_tag=delivery_tag)
        #     return
        
        # Process message - no in-memory duplicate check by client (removed to avoid conflicts)
        if message_id:
            print(f"[filter_by_amount] Worker {WORKER_INDEX} processing message_id {message_id} for client {client_id} from sender {sender}", flush=True)
            
        # Update client stats
        client_stats[client_id]['messages_received'] += 1
        client_stats[client_id]['rows_received'] += len(parsed_message['rows'])
        
        # Count incoming rows
        incoming_rows = len(parsed_message['rows'])
        rows_received += incoming_rows
        
        #print(f"[filter_by_amount] Worker {WORKER_INDEX} received message from client {client_id} with {incoming_rows} rows, is_last={is_last} (total msgs: {client_stats[client_id]['messages_received']}, total rows: {client_stats[client_id]['rows_received']})")
        
        if is_last:
            client_stats[client_id]['end_messages_received'] += 1
            client_end_messages[client_id] += 1
            print(f"[filter_by_amount] Worker {WORKER_INDEX} received END message {client_end_messages[client_id]}/{NUMBER_OF_HOUR_WORKERS} for client {client_id}", flush=True)
        
        # Skip if client already completed - check AFTER processing END message
        if client_id in completed_clients:
            print(f"[filter_by_amount] Worker {WORKER_INDEX} ignoring message from already completed client {client_id}")
            if delivery_tag and channel:
                channel.basic_ack(delivery_tag=delivery_tag)
            return
        
        filtered_rows = filter_message_by_amount(parsed_message, MIN_AMOUNT)
        
        #print(f"[filter_by_amount] Worker {WORKER_INDEX} client {client_id}: {len(filtered_rows)} rows passed amount filter (from {len(parsed_message['rows'])} input rows)")
        
        client_completed = client_end_messages[client_id] >= NUMBER_OF_HOUR_WORKERS
        
        #f client_id == "client_1" and is_last:
            #print(f"[filter_by_amount] Worker {WORKER_INDEX} DEBUG client_1: is_last={is_last}, client_completed={client_completed}, end_messages={client_end_messages[client_id]}")
        
        if filtered_rows or (is_last and client_completed):
            client_stats[client_id]['rows_sent'] += len(filtered_rows)
            
            global queue_result
            if queue_result is None:
                queue_result = MessageMiddlewareQueue(RABBITMQ_HOST, RESULT_QUEUE)
            
            # Only send is_last=1 if this client has received all END messages
            final_is_last = 1 if (is_last and client_completed) else 0
            outgoing_sender = f"filter_by_amount_worker_{WORKER_INDEX}"
            # CRITICAL: Use original message_id to enable duplicate detection in gateway
            new_message, _ = build_message(client_id, type_of_message, final_is_last, filtered_rows, message_id=message_id, sender=outgoing_sender)
            queue_result.send(new_message)
            # Don't close connection after each message - reuse it!
            
            # Count outgoing rows
            rows_sent += len(filtered_rows)
            #print(f"[filter_by_amount] Worker {WORKER_INDEX} sent {len(filtered_rows)} filtered rows for client {client_id}, final_is_last={final_is_last} (total sent: {client_stats[client_id]['rows_sent']})", flush=True)
            
            # Mark client as completed and print summary AFTER sending final message
            if final_is_last == 1:
                if client_id not in completed_clients:
                    completed_clients.add(client_id)
                    stats = client_stats[client_id]
                    print(f"[filter_by_amount] Worker {WORKER_INDEX} SUMMARY for client {client_id}: messages_received={stats['messages_received']}, rows_received={stats['rows_received']}, rows_sent={stats['rows_sent']}, end_messages_received={stats['end_messages_received']}")
        
        # Send ACK response to sender if sender_id and message_id are available
        # Currently disabled because filter_by_hour is not reading ACKs
        # if sender_id and message_id:
        #     send_ack(sender_id, message_id)
        
        # CRITICAL: Save message_id to disk AFTER successful message sending to prevent message loss
        if message_id and sender:
            # Update last processed message_id and save to disk
            on_message_callback._disk_last_message_id_by_sender[sender] = message_id
            save_last_processed_message_id(sender, message_id)
            print(f"[filter_by_amount] Worker {WORKER_INDEX} saved last processed message_id for sender {sender}: {message_id}", flush=True)
        
        # Manual ACK after persistence
        if delivery_tag and channel:
            channel.basic_ack(delivery_tag=delivery_tag)
            print(f"[filter_by_amount] Worker {WORKER_INDEX} ACK sent for message from client {client_id}, sender {sender}", flush=True)
            
    except Exception as e:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} ERROR processing message: {e}", flush=True)
        # NACK on error to requeue the message
        if delivery_tag and channel:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            print(f"[filter_by_amount] Worker {WORKER_INDEX} NACK sent (requeue=True) due to error", flush=True)


def make_on_message_callback(topic_middleware, should_stop):
    def wrapper(message: bytes, delivery_tag, channel):
        on_message_callback(message, topic_middleware, should_stop, delivery_tag, channel)
    return wrapper


def main():
    import threading
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)

    health_check_receiver = HealthCheckReceiver()
    health_check_receiver.start()

    # Global counters for debugging
    global rows_received, rows_sent, end_messages_received
    rows_received = 0
    rows_sent = 0
    end_messages_received = 0
    contents = os.listdir(PERSISTENCE_DIR)
    if not contents:
        sleep(config.MIDDLEWARE_UP_TIME)  # Esperar a que RabbitMQ esté listo
    print(f"[filter_by_amount] Worker {WORKER_INDEX} connecting to RabbitMQ at {RABBITMQ_HOST}, exchange: {RECEIVER_EXCHANGE}, filter by min amount: {MIN_AMOUNT}", flush=True)
    
    # Create topic exchange middleware for receiving transaction messages
    global topic_middleware
    topic_middleware = MessageMiddlewareExchange(
        host=RABBITMQ_HOST,
        exchange_name=RECEIVER_EXCHANGE,
        exchange_type='topic',
        queue_name=f"filter_by_amount_worker_{WORKER_INDEX}_queue",
        routing_keys=[f'transaction.{WORKER_INDEX}']  # Each worker listens to specific routing key
    )
    
    # Create result queue
    # queue_result = MessageMiddlewareQueue(RABBITMQ_HOST, RESULT_QUEUE)
    
    print(f"[filter_by_amount] Worker {WORKER_INDEX} listening for transactions on exchange: {RECEIVER_EXCHANGE} with routing key: transaction.{WORKER_INDEX}", flush=True)
    
    # Flag to coordinate stopping
    should_stop = threading.Event()
    
    try:        
        # Start consuming from topic exchange with manual ACK (blocking)
        # topic_callback = make_on_message_callback(topic_middleware, should_stop)
        topic_middleware.start_consuming(on_message_callback, auto_ack=True)
        
        print(f"[filter_by_amount] Worker {WORKER_INDEX} topic consuming finished", flush=True)
        
        should_stop.set()
            
    except MessageMiddlewareDisconnectedError:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} disconnected from middleware.", file=sys.stderr)
    except MessageMiddlewareMessageError:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} message error in middleware.", file=sys.stderr)
    except KeyboardInterrupt:
        print(f"[filter_by_amount] Worker {WORKER_INDEX} stopping...")
        should_stop.set()
        topic_middleware.stop_consuming()

    finally:
        try:
            topic_middleware.close()
        except Exception as e:
            print(f"[filter_by_amount] Worker {WORKER_INDEX} error closing topic middleware: {e}", file=sys.stderr)
        # try:
        #     queue_result.close()
        # except Exception as e:
        #     print(f"[filter_by_amount] Worker {WORKER_INDEX} error closing result queue: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()