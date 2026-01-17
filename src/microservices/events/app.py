import os
import json
import logging
import threading
import time
from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
PORT = int(os.getenv('PORT', 8082))
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')

logger.info(f"Events Service Configuration:")
logger.info(f"  Port: {PORT}")
logger.info(f"  Kafka Brokers: {KAFKA_BROKERS}")

# Kafka Topics
TOPICS = {
    'movie': 'movie-events',
    'user': 'user-events',
    'payment': 'payment-events'
}

# Initialize Kafka Producer
producer = None

def get_kafka_producer():
    """Get or create Kafka producer with retry logic"""
    global producer
    if producer is None:
        max_retries = 10
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1
                )
                logger.info("Successfully connected to Kafka")
                return producer
            except KafkaError as e:
                logger.error(f"Failed to connect to Kafka: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Max retries reached. Producer not available.")
                    return None
    return producer


def consume_events():
    """Consume events from all Kafka topics"""
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            logger.info(f"Starting Kafka consumer (attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                *TOPICS.values(),
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='events-service-consumer-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )

            logger.info(f"Kafka consumer started. Subscribed to topics: {list(TOPICS.values())}")

            for message in consumer:
                logger.info(f"========== EVENT RECEIVED ==========")
                logger.info(f"Topic: {message.topic}")
                logger.info(f"Partition: {message.partition}")
                logger.info(f"Offset: {message.offset}")
                logger.info(f"Event Data: {json.dumps(message.value, indent=2)}")
                logger.info(f"===================================")

        except KafkaError as e:
            logger.error(f"Kafka consumer error: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying consumer in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached for consumer")
                break
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {e}")
            time.sleep(retry_delay)


# Start consumer in background thread
consumer_thread = threading.Thread(target=consume_events, daemon=True)
consumer_thread.start()


@app.route('/api/events/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': True}), 200


@app.route('/api/events/movie', methods=['POST'])
def create_movie_event():
    """Create a movie event and send to Kafka"""
    try:
        event_data = request.get_json()

        if not event_data:
            return jsonify({'error': 'No data provided'}), 400

        logger.info(f"Received movie event: {event_data}")

        # Get Kafka producer
        kafka_producer = get_kafka_producer()
        if kafka_producer is None:
            return jsonify({'error': 'Kafka producer not available'}), 503

        # Send to Kafka
        future = kafka_producer.send(TOPICS['movie'], event_data)

        try:
            record_metadata = future.get(timeout=10)
            logger.info(f"Movie event sent to Kafka topic '{TOPICS['movie']}' "
                       f"(partition: {record_metadata.partition}, offset: {record_metadata.offset})")

            return jsonify({
                'status': 'success',
                'message': 'Movie event created',
                'event': event_data,
                'kafka': {
                    'topic': TOPICS['movie'],
                    'partition': record_metadata.partition,
                    'offset': record_metadata.offset
                }
            }), 201

        except Exception as e:
            logger.error(f"Failed to send movie event to Kafka: {e}")
            return jsonify({'error': 'Failed to send event to Kafka'}), 500

    except Exception as e:
        logger.error(f"Error creating movie event: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/events/user', methods=['POST'])
def create_user_event():
    """Create a user event and send to Kafka"""
    try:
        event_data = request.get_json()

        if not event_data:
            return jsonify({'error': 'No data provided'}), 400

        logger.info(f"Received user event: {event_data}")

        # Get Kafka producer
        kafka_producer = get_kafka_producer()
        if kafka_producer is None:
            return jsonify({'error': 'Kafka producer not available'}), 503

        # Send to Kafka
        future = kafka_producer.send(TOPICS['user'], event_data)

        try:
            record_metadata = future.get(timeout=10)
            logger.info(f"User event sent to Kafka topic '{TOPICS['user']}' "
                       f"(partition: {record_metadata.partition}, offset: {record_metadata.offset})")

            return jsonify({
                'status': 'success',
                'message': 'User event created',
                'event': event_data,
                'kafka': {
                    'topic': TOPICS['user'],
                    'partition': record_metadata.partition,
                    'offset': record_metadata.offset
                }
            }), 201

        except Exception as e:
            logger.error(f"Failed to send user event to Kafka: {e}")
            return jsonify({'error': 'Failed to send event to Kafka'}), 500

    except Exception as e:
        logger.error(f"Error creating user event: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/events/payment', methods=['POST'])
def create_payment_event():
    """Create a payment event and send to Kafka"""
    try:
        event_data = request.get_json()

        if not event_data:
            return jsonify({'error': 'No data provided'}), 400

        logger.info(f"Received payment event: {event_data}")

        # Get Kafka producer
        kafka_producer = get_kafka_producer()
        if kafka_producer is None:
            return jsonify({'error': 'Kafka producer not available'}), 503

        # Send to Kafka
        future = kafka_producer.send(TOPICS['payment'], event_data)

        try:
            record_metadata = future.get(timeout=10)
            logger.info(f"Payment event sent to Kafka topic '{TOPICS['payment']}' "
                       f"(partition: {record_metadata.partition}, offset: {record_metadata.offset})")

            return jsonify({
                'status': 'success',
                'message': 'Payment event created',
                'event': event_data,
                'kafka': {
                    'topic': TOPICS['payment'],
                    'partition': record_metadata.partition,
                    'offset': record_metadata.offset
                }
            }), 201

        except Exception as e:
            logger.error(f"Failed to send payment event to Kafka: {e}")
            return jsonify({'error': 'Failed to send event to Kafka'}), 500

    except Exception as e:
        logger.error(f"Error creating payment event: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Give consumer thread time to start
    time.sleep(2)
    app.run(host='0.0.0.0', port=PORT, debug=False)
