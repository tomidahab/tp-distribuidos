#!/usr/bin/env python3
"""
Unit tests for MessageMiddleware
Tests cover the following communication patterns:
- Working Queue 1 to 1
- Working Queue 1 to N
- Exchange 1 to 1
- Exchange 1 to N
"""

import unittest
import threading
import time
import os
import sys
from unittest.mock import Mock, patch

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from common.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from common.middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError


class TestMessageMiddleware(unittest.TestCase):
    """Test suite for MessageMiddleware functionality"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        cls.RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
        cls.test_messages = [
            b"Test message 1",
            b"Test message 2", 
            b"Test message 3",
            b"Hello World",
            b"Final test message"
        ]
        
    def setUp(self):
        """Set up each test"""
        self.received_messages = []
        self.message_lock = threading.Lock()
        
    def tearDown(self):
        """Clean up after each test"""
        # Small delay to ensure connections are properly closed
        time.sleep(0.5)
        
    def safe_close_middleware(self, middleware_obj, name="middleware"):
        """Safely close a middleware object without throwing exceptions"""
        try:
            if hasattr(middleware_obj, 'consuming') and middleware_obj.consuming:
                middleware_obj.consuming = False
            if hasattr(middleware_obj, 'connection') and middleware_obj.connection:
                middleware_obj.connection.close()
        except Exception as e:
            print(f"Note: Safe cleanup of {name}: {e}")
    
    def safe_stop_all_consumers(self, consumers):
        """Safely stop all consumers by closing their connections"""
        for i, consumer in enumerate(consumers):
            try:
                if hasattr(consumer, 'consuming'):
                    consumer.consuming = False
                if hasattr(consumer, 'connection') and consumer.connection:
                    # Force close connection to stop consuming
                    consumer.connection.close()
            except Exception as e:
                print(f"Note: Safe stop consumer {i}: {e}")
        
    def add_received_message(self, message):
        """Thread-safe method to add received messages"""
        with self.message_lock:
            self.received_messages.append(message)
            
    def wait_for_messages(self, expected_count, timeout=10):
        """Wait for expected number of messages with timeout"""
        start_time = time.time()
        while len(self.received_messages) < expected_count:
            if time.time() - start_time > timeout:
                break
            time.sleep(0.1)
        return len(self.received_messages) >= expected_count


class TestWorkingQueue1to1(TestMessageMiddleware):
    """Test Working Queue 1 to 1 communication"""
    
    def test_single_producer_single_consumer(self):
        """Test 1 producer sending to 1 consumer via working queue"""
        print("\n=== Testing Working Queue 1 to 1 ===")
        
        queue_name = "test_queue_1to1"
        
        # Create producer
        producer = MessageMiddlewareQueue(self.RABBITMQ_HOST, queue_name)
        
        # Create consumer
        consumer = MessageMiddlewareQueue(self.RABBITMQ_HOST, queue_name)
        
        def message_callback(message):
            print(f"Consumer received: {message}")
            self.add_received_message(message)
            if len(self.received_messages) >= len(self.test_messages):
                consumer.stop_consuming()
        
        # Start consumer in separate thread
        consumer_thread = threading.Thread(
            target=lambda: consumer.start_consuming(message_callback),
            daemon=True
        )
        consumer_thread.start()
        
        # Give consumer time to start
        time.sleep(1)
        
        # Send messages
        print(f"Producer sending {len(self.test_messages)} messages...")
        for i, message in enumerate(self.test_messages):
            producer.send(message)
            print(f"Sent message {i+1}: {message}")
            time.sleep(0.1)
        
        # Wait for all messages to be received
        success = self.wait_for_messages(len(self.test_messages))
        
        # Cleanup safely
        self.safe_close_middleware(producer, "producer")
        self.safe_close_middleware(consumer, "consumer")
        
        # Assertions
        self.assertTrue(success, f"Expected {len(self.test_messages)} messages, got {len(self.received_messages)}")
        self.assertEqual(len(self.received_messages), len(self.test_messages))
        
        # Verify message content
        for i, original_message in enumerate(self.test_messages):
            self.assertEqual(self.received_messages[i], original_message)
            
        print(f"✓ Successfully received all {len(self.received_messages)} messages")


class TestWorkingQueue1toN(TestMessageMiddleware):
    """Test Working Queue 1 to N communication (load balancing)"""
    
    def test_single_producer_multiple_consumers(self):
        """Test 1 producer sending to N consumers via working queue (round-robin)"""
        print("\n=== Testing Working Queue 1 to N ===")
        
        queue_name = "test_queue_1toN"
        num_consumers = 3
        
        # Create producer
        producer = MessageMiddlewareQueue(self.RABBITMQ_HOST, queue_name)
        
        # Create multiple consumers
        consumers = []
        consumer_threads = []
        consumer_received = [[] for _ in range(num_consumers)]
        
        def create_callback(consumer_id):
            def message_callback(message):
                print(f"Consumer {consumer_id} received: {message}")
                consumer_received[consumer_id].append(message)
                self.add_received_message(message)
                
                # Don't stop in callback to avoid race conditions
            return message_callback
        
        # Start consumers
        for i in range(num_consumers):
            consumer = MessageMiddlewareQueue(self.RABBITMQ_HOST, queue_name)
            consumers.append(consumer)
            
            callback = create_callback(i)
            thread = threading.Thread(
                target=lambda c=consumer, cb=callback: c.start_consuming(cb),
                daemon=True
            )
            consumer_threads.append(thread)
            thread.start()
        
        # Give consumers time to start
        time.sleep(2)
        
        # Send messages
        print(f"Producer sending {len(self.test_messages)} messages to {num_consumers} consumers...")
        for i, message in enumerate(self.test_messages):
            producer.send(message)
            print(f"Sent message {i+1}: {message}")
            time.sleep(0.2)
        
        # Wait for all messages to be received
        success = self.wait_for_messages(len(self.test_messages))
        
        # Stop all consumers safely by closing connections
        self.safe_stop_all_consumers(consumers)
        
        # Wait for threads to finish
        time.sleep(2)
        
        # Cleanup
        self.safe_close_middleware(producer, "producer")
        for i, consumer in enumerate(consumers):
            self.safe_close_middleware(consumer, f"consumer_{i}")
        
        # Assertions
        self.assertTrue(success, f"Expected {len(self.test_messages)} messages, got {len(self.received_messages)}")
        self.assertEqual(len(self.received_messages), len(self.test_messages))
        
        # Verify load balancing (each consumer should receive at least one message)
        consumers_with_messages = sum(1 for consumer_msgs in consumer_received if len(consumer_msgs) > 0)
        self.assertGreater(consumers_with_messages, 1, "Messages should be distributed among multiple consumers")
        
        print(f"✓ Successfully distributed {len(self.received_messages)} messages among {consumers_with_messages} consumers")
        for i, msgs in enumerate(consumer_received):
            print(f"  Consumer {i}: {len(msgs)} messages")


class TestExchange1to1(TestMessageMiddleware):
    """Test Exchange 1 to 1 communication"""
    
    def test_single_producer_single_consumer_topic_exchange(self):
        """Test 1 producer sending to 1 consumer via topic exchange"""
        print("\n=== Testing Exchange 1 to 1 (Topic) ===")
        
        exchange_name = "test_exchange_1to1"
        queue_name = "test_queue_exchange_1to1"
        routing_key = "test.routing.key"
        
        # Create producer (sender)
        producer = MessageMiddlewareExchange(
            self.RABBITMQ_HOST, 
            exchange_name, 
            'topic',
            "",  # Empty queue for sender
        )
        
        # Create consumer
        consumer = MessageMiddlewareExchange(
            self.RABBITMQ_HOST,
            exchange_name,
            'topic', 
            queue_name,
            routing_keys=[routing_key]
        )
        
        def message_callback(message):
            print(f"Consumer received: {message}")
            self.add_received_message(message)
            if len(self.received_messages) >= len(self.test_messages):
                consumer.stop_consuming()
        
        # Start consumer
        consumer_thread = threading.Thread(
            target=lambda: consumer.start_consuming(message_callback),
            daemon=True
        )
        consumer_thread.start()
        
        # Give consumer time to start
        time.sleep(1)
        
        # Send messages with routing key
        print(f"Producer sending {len(self.test_messages)} messages via topic exchange...")
        for i, message in enumerate(self.test_messages):
            producer.send(message, routing_key=routing_key)
            print(f"Sent message {i+1} with routing key '{routing_key}': {message}")
            time.sleep(0.1)
        
        # Wait for messages
        success = self.wait_for_messages(len(self.test_messages))
        
        # Cleanup
        producer.close()
        consumer.close()
        
        # Assertions
        self.assertTrue(success, f"Expected {len(self.test_messages)} messages, got {len(self.received_messages)}")
        self.assertEqual(len(self.received_messages), len(self.test_messages))
        
        print(f"✓ Successfully received all {len(self.received_messages)} messages via topic exchange")
    
    def test_single_producer_single_consumer_fanout_exchange(self):
        """Test 1 producer sending to 1 consumer via fanout exchange"""
        print("\n=== Testing Exchange 1 to 1 (Fanout) ===")
        
        exchange_name = "test_exchange_fanout_1to1"
        queue_name = "test_queue_fanout_1to1"
        
        # Create producer
        producer = MessageMiddlewareExchange(
            self.RABBITMQ_HOST,
            exchange_name,
            'fanout',
            ""  # Empty queue for sender
        )
        
        # Create consumer
        consumer = MessageMiddlewareExchange(
            self.RABBITMQ_HOST,
            exchange_name,
            'fanout',
            queue_name
        )
        
        def message_callback(message):
            print(f"Consumer received: {message}")
            self.add_received_message(message)
            if len(self.received_messages) >= len(self.test_messages):
                consumer.stop_consuming()
        
        # Start consumer
        consumer_thread = threading.Thread(
            target=lambda: consumer.start_consuming(message_callback),
            daemon=True
        )
        consumer_thread.start()
        
        # Give consumer time to start
        time.sleep(1)
        
        # Send messages (no routing key needed for fanout)
        print(f"Producer sending {len(self.test_messages)} messages via fanout exchange...")
        for i, message in enumerate(self.test_messages):
            producer.send(message)
            print(f"Sent message {i+1}: {message}")
            time.sleep(0.1)
        
        # Wait for messages
        success = self.wait_for_messages(len(self.test_messages))
        
        # Cleanup
        producer.close()
        consumer.close()
        
        # Assertions
        self.assertTrue(success, f"Expected {len(self.test_messages)} messages, got {len(self.received_messages)}")
        self.assertEqual(len(self.received_messages), len(self.test_messages))
        
        print(f"✓ Successfully received all {len(self.received_messages)} messages via fanout exchange")


class TestExchange1toN(TestMessageMiddleware):
    """Test Exchange 1 to N communication (broadcast)"""
    
    def test_single_producer_multiple_consumers_fanout_exchange(self):
        """Test 1 producer broadcasting to N consumers via fanout exchange"""
        print("\n=== Testing Exchange 1 to N (Fanout Broadcast) ===")
        
        exchange_name = "test_exchange_1toN_fanout"
        num_consumers = 3
        
        # Create producer
        producer = MessageMiddlewareExchange(
            self.RABBITMQ_HOST,
            exchange_name,
            'fanout',
            ""  # Empty queue for sender
        )
        
        # Create multiple consumers with different queues
        consumers = []
        consumer_threads = []
        consumer_received = [[] for _ in range(num_consumers)]
        
        def create_callback(consumer_id):
            def message_callback(message):
                print(f"Consumer {consumer_id} received: {message}")
                consumer_received[consumer_id].append(message)
                self.add_received_message(message)
                
                # Don't stop in callback to avoid race conditions
            return message_callback
        
        # Start consumers with unique queues
        for i in range(num_consumers):
            queue_name = f"test_queue_fanout_consumer_{i}"
            consumer = MessageMiddlewareExchange(
                self.RABBITMQ_HOST,
                exchange_name,
                'fanout',
                queue_name
            )
            consumers.append(consumer)
            
            callback = create_callback(i)
            thread = threading.Thread(
                target=lambda c=consumer, cb=callback: c.start_consuming(cb),
                daemon=True
            )
            consumer_threads.append(thread)
            thread.start()
        
        # Give consumers time to start
        time.sleep(2)
        
        # Send messages (each should be received by ALL consumers)
        print(f"Producer broadcasting {len(self.test_messages)} messages to {num_consumers} consumers...")
        for i, message in enumerate(self.test_messages):
            producer.send(message)
            print(f"Broadcast message {i+1}: {message}")
            time.sleep(0.2)
        
        # Wait for all consumers to receive all messages
        expected_total = len(self.test_messages) * num_consumers
        success = self.wait_for_messages(expected_total, timeout=15)
        
        # Stop all consumers safely by closing connections
        self.safe_stop_all_consumers(consumers)
        
        # Wait for threads to finish
        time.sleep(2)
        
        # Cleanup
        self.safe_close_middleware(producer, "producer")
        for i, consumer in enumerate(consumers):
            self.safe_close_middleware(consumer, f"consumer_{i}")
        
        # Assertions
        self.assertTrue(success, f"Expected {expected_total} total messages, got {len(self.received_messages)}")
        self.assertEqual(len(self.received_messages), expected_total)
        
        # Verify each consumer received ALL messages
        for i, consumer_msgs in enumerate(consumer_received):
            self.assertEqual(len(consumer_msgs), len(self.test_messages), 
                           f"Consumer {i} should receive all {len(self.test_messages)} messages")
        
        print(f"✓ Successfully broadcast {len(self.test_messages)} messages to {num_consumers} consumers")
        print(f"  Total messages received: {len(self.received_messages)}")
        for i, msgs in enumerate(consumer_received):
            print(f"  Consumer {i}: {len(msgs)} messages")
    
    def test_single_producer_multiple_consumers_topic_exchange(self):
        """Test 1 producer sending to N consumers via topic exchange with different routing keys"""
        print("\n=== Testing Exchange 1 to N (Topic Selective) ===")
        
        exchange_name = "test_exchange_1toN_topic"
        
        # Create producer
        producer = MessageMiddlewareExchange(
            self.RABBITMQ_HOST,
            exchange_name,
            'topic',
            ""  # Empty queue for sender
        )
        
        # Create consumers with different routing keys
        consumers = []
        consumer_threads = []
        consumer_received = [[] for _ in range(3)]
        
        routing_configs = [
            (0, "orders.*", "test_queue_orders"),      # Consumer 0: all orders
            (1, "orders.urgent", "test_queue_urgent"), # Consumer 1: only urgent orders  
            (2, "*.urgent", "test_queue_all_urgent")   # Consumer 2: all urgent messages
        ]
        
        def create_callback(consumer_id):
            def message_callback(message):
                print(f"Consumer {consumer_id} received: {message}")
                consumer_received[consumer_id].append(message)
                self.add_received_message(message)
            return message_callback
        
        # Start consumers
        for consumer_id, routing_key, queue_name in routing_configs:
            consumer = MessageMiddlewareExchange(
                self.RABBITMQ_HOST,
                exchange_name,
                'topic',
                queue_name,
                routing_keys=[routing_key]
            )
            consumers.append(consumer)
            
            callback = create_callback(consumer_id)
            thread = threading.Thread(
                target=lambda c=consumer, cb=callback: c.start_consuming(cb),
                daemon=True
            )
            consumer_threads.append(thread)
            thread.start()
        
        # Give consumers time to start
        time.sleep(2)
        
        # Send messages with different routing keys
        test_routing_messages = [
            (b"Normal order message", "orders.normal"),
            (b"Urgent order message", "orders.urgent"), 
            (b"Urgent payment message", "payments.urgent"),
            (b"Regular payment message", "payments.normal"),
            (b"Another urgent order", "orders.urgent")
        ]
        
        print(f"Producer sending {len(test_routing_messages)} messages with different routing keys...")
        for i, (message, routing_key) in enumerate(test_routing_messages):
            producer.send(message, routing_key=routing_key)
            print(f"Sent message {i+1} with routing key '{routing_key}': {message}")
            time.sleep(0.3)
        
        # Wait for messages to be processed
        time.sleep(3)
        
        # Stop all consumers safely by closing connections
        self.safe_stop_all_consumers(consumers)
        
        # Wait for threads to finish
        time.sleep(2)
        
        # Cleanup
        self.safe_close_middleware(producer, "producer")
        for i, consumer in enumerate(consumers):
            self.safe_close_middleware(consumer, f"consumer_{i}")
        
        # Verify routing worked correctly
        print("\nRouting verification:")
        print(f"Consumer 0 (orders.*): {len(consumer_received[0])} messages")     # Should get 3 order messages
        print(f"Consumer 1 (orders.urgent): {len(consumer_received[1])} messages") # Should get 2 urgent order messages
        print(f"Consumer 2 (*.urgent): {len(consumer_received[2])} messages")      # Should get 3 urgent messages
        
        # Consumer 0 should receive all order messages (3)
        self.assertEqual(len(consumer_received[0]), 3, "Consumer 0 should receive all order messages")
        
        # Consumer 1 should receive only urgent order messages (2) 
        self.assertEqual(len(consumer_received[1]), 2, "Consumer 1 should receive only urgent order messages")
        
        # Consumer 2 should receive all urgent messages (3)
        self.assertEqual(len(consumer_received[2]), 3, "Consumer 2 should receive all urgent messages")
        
        print("✓ Successfully verified topic exchange selective routing")


class TestErrorHandling(TestMessageMiddleware):
    """Test error handling scenarios"""
    
    def test_connection_error_handling(self):
        """Test handling of connection errors"""
        print("\n=== Testing Error Handling ===")
        
        # Test invalid host
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            invalid_middleware = MessageMiddlewareQueue("invalid_host_12345", "test_queue")
            
        print("✓ Successfully caught connection error for invalid host")
    
    def test_message_callback_error_handling(self):
        """Test handling of errors in message callbacks"""
        print("\n=== Testing Message Callback Error Handling ===")
        
        queue_name = "test_error_queue"
        
        # Create producer and consumer
        producer = MessageMiddlewareQueue(self.RABBITMQ_HOST, queue_name)
        consumer = MessageMiddlewareQueue(self.RABBITMQ_HOST, queue_name)
        
        error_occurred = threading.Event()
        
        def failing_callback(message):
            print(f"Callback processing: {message}")
            # This should trigger error handling
            raise Exception("Intentional callback error")
        
        # Start consumer with failing callback
        def consume_with_error_handling():
            try:
                consumer.start_consuming(failing_callback)
            except MessageMiddlewareMessageError:
                error_occurred.set()
                print("✓ Caught MessageMiddlewareMessageError as expected")
        
        consumer_thread = threading.Thread(target=consume_with_error_handling, daemon=True)
        consumer_thread.start()
        
        # Give consumer time to start
        time.sleep(1)
        
        # Send a message
        producer.send(b"Test message that will cause error")
        
        # Wait for error to be caught
        success = error_occurred.wait(timeout=5)
        
        # Cleanup safely
        self.safe_close_middleware(producer, "producer")
        self.safe_close_middleware(consumer, "consumer")
        
        self.assertTrue(success, "Should have caught message callback error")
        print("✓ Successfully handled message callback error")


if __name__ == "__main__":
    print("=" * 60)
    print("MessageMiddleware Unit Tests")
    print("=" * 60)
    print("Testing communication patterns:")
    print("• Working Queue 1 to 1")
    print("• Working Queue 1 to N") 
    print("• Exchange 1 to 1")
    print("• Exchange 1 to N")
    print("=" * 60)
    
    # Run tests with verbose output
    unittest.main(verbosity=2, exit=False)