from abc import ABC, abstractmethod
import pika

class MessageMiddlewareMessageError(Exception):
    pass

class MessageMiddlewareDisconnectedError(Exception):
    pass

class MessageMiddlewareCloseError(Exception):
    pass

class MessageMiddlewareDeleteError(Exception):
    pass

class MessageMiddleware(ABC):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consuming = False

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Could not connect to RabbitMQ: {e}")

    def start_consuming(self, on_message_callback):
        if not self.channel:
            raise MessageMiddlewareDisconnectedError("No channel to consume from.")

        def callback(ch, method, properties, body):
            try:
                on_message_callback(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                raise MessageMiddlewareMessageError(f"Error in message callback: {e}")

        self.consuming = True
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Lost connection to RabbitMQ.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error while consuming: {e}")

    def stop_consuming(self):
        if self.channel and self.consuming:
            try:
                self.channel.stop_consuming()
                self.consuming = False
            except pika.exceptions.AMQPConnectionError:
                raise MessageMiddlewareDisconnectedError("Lost connection to RabbitMQ.")

    def send(self, message):
        if not self.channel:
            raise MessageMiddlewareDisconnectedError("No channel to send to.")
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Lost connection to RabbitMQ.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error sending message: {e}")

    def close(self):
        try:
            if self.channel:
                self.channel.close()
            if self.connection:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error closing connection: {e}")

    def delete(self):
        try:
            if self.channel:
                self.channel.queue_delete(queue=self.queue_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error deleting queue: {e}")

class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, host, exchange_name, exchange_type, queue_name, routing_keys=None):
        super().__init__(host, queue_name)
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.routing_keys = routing_keys or []
        # Declare exchange
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type, durable=True)
        # Bind queue to exchange with routing keys
        if self.exchange_type == 'topic':
            for key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=key)
        elif self.exchange_type == 'fanout':
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name)

    def send(self, message, routing_key=''):
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )

class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host, queue_name):
        super().__init__(host, queue_name)