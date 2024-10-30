from kafka import KafkaProducer
import json
import time
import random
from kafka.errors import KafkaError
import traceback

# Initialize Kafka producer with enhanced settings
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,              # Retry up to 5 times on failure
        linger_ms=10            # Slight delay to batch messages
    )
    print("Kafka producer initialized successfully.")
except Exception as e:
    print(f"Failed to initialize Kafka producer: {e}")
    traceback.print_exc()
    exit(1)

topic = 'brain-eeg-data'

def get_fake_eeg_data():
    channels = ['Fp1', 'Fp2', 'F3', 'F4', 'C3', 'C4', 'P3', 'P4']
    return {
        'sensor_id': 'eeg-sensor-1',
        'timestamp': int(time.time()),
        'channels': {channel: round(random.uniform(-100.0, 100.0), 2) for channel in channels}
    }

try:
    while True:
        data = get_fake_eeg_data()
        future = producer.send(topic, data)
        try:
            record_metadata = future.get(timeout=10)
            print(f"Sent data at {data['timestamp']} to partition {record_metadata.partition} offset {record_metadata.offset}")
        except KafkaError as e:
            print(f"Failed to send data: {data}. Error: {e}")
            traceback.print_exc()
        time.sleep(0.1)  # Reduced to 100ms (10 Hz)
except KeyboardInterrupt:
    print("Producer stopped by user.")
except Exception as e:
    print(f"Unexpected error: {e}")
    traceback.print_exc()
finally:
    producer.close()
    print("Kafka producer closed.")
