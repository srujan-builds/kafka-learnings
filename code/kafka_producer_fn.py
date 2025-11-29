from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# create a producer instance
producer = KafkaProducer(
    bootstrap_servers = ["localhost:9092"],
    key_serializer = lambda m : m.encode("utf-8"),
    value_serializer = lambda m : json.dumps(m).encode("utf-8")
)

# messages to send
messages = [
    {"customer_id":1, "customer_name":"Alice", "country":"USA"},
    {"customer_id":2, "customer_name":"Bob", "country":"India"},
    {"customer_id":3, "customer_name":"Charlie", "country":"UK"},
    {"customer_id":4, "customer_name":"Diana", "country":"Canada"},
    {"customer_id":5, "customer_name":"Eve", "country":"Australia"},
]

# async send

def on_success(record_metadata):
    print("Message sent")
    print(f"Topic: {record_metadata.topic}, Partition:{record_metadata.partition}, Offset:{record_metadata.offset}")


def on_faiure(err):
    print(f"Message failed with error: {err}")


try:
    for msg_idx, msg_data in enumerate(messages):
        producer.send(
            topic="customer-countries",
            key=f"customer-{msg_idx}",
            value=msg_data
        ).add_callback(on_success).add_errback(on_faiure)
except KafkaError as e:
    print(f"Error while sending message{e}")
finally:

    '''
    1 flush ensures all buffered messages are actually sent before the program exits
    2 producer.send() is asynchronous: it queues the message and returns immediately
    3 the actual send + broker ACK happens in background threads
    4 flush() waits for those background sends to complete, ensuring no data is lost
    '''
    producer.flush(timeout=10)



