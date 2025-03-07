import pika
import json
import ssl

def get_rabbitmq_connection():
    """
    Establish a connection to Amazon MQ RabbitMQ.
    """
    # Amazon MQ endpoint
    endpoint = 'b-0af8867b-97dd-47cc-aad8-b579ba6bc935.mq.us-east-1.amazonaws.com'
    port = 5671  # Default SSL port for Amazon MQ

    # Amazon MQ credentials (replace with your actual username and password)
    username = 'admin'
    password = 'Jesusislord2003.'

    # SSL context
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')

    # Connection parameters
    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(
        host=endpoint,
        port=port,
        credentials=credentials,
        ssl_options=pika.SSLOptions(context=ssl_context),
        virtual_host='/'
    )

    return pika.BlockingConnection(parameters)

def publish_book_created_event(book_data):
    """
    Publish a 'book_created' event to the 'book_queue'.
    """
    try:
        # Establish a connection to Amazon MQ RabbitMQ
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        # Declare the queue (if it doesn't exist)
        channel.queue_declare(queue='book_queue', durable=True)

        # Prepare the event
        event = {
            'event_type': 'book_created',
            'book_data': book_data,
        }

        # Publish the event to the queue
        channel.basic_publish(
            exchange='',  # Use the default exchange
            routing_key='book_queue',  
            body=json.dumps(event),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            )
        )

        print(f" [x] Published 'book_created' event: {book_data}")

        # Close the connection
        connection.close()
    except Exception as e:
        print(f"Failed to publish event: {e}")

def publish_book_deleted_event(book_data):
    """
    Publish a 'book_deleted' event to the 'book_queue'.
    """
    try:
        # Establish a connection to Amazon MQ RabbitMQ
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        # Declare the queue (if it doesn't exist)
        channel.queue_declare(queue='book_queue', durable=True)

        # Prepare the event
        event = {
            'event_type': 'book_deleted',
            'book_data': book_data,
        }

        # Publish the event to the queue
        channel.basic_publish(
            exchange='',  # Use the default exchange
            routing_key='book_queue',  # Send to the 'book_queue'
            body=json.dumps(event),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            )
        )

        print(f" [x] Published 'book_deleted' event: {book_data}")

        # Close the connection
        connection.close()
    except Exception as e:
        print(f"Failed to publish event: {e}")