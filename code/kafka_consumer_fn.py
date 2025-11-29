from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json


# consumer instance
consumer = KafkaConsumer(
    bootstrap_servers = ["localhost:9092"],
    group_id = "CountryCounter",
    key_deserializer = lambda m: m.decode("utf-8"),
    value_deserializer = lambda m: json.loads(m.decode("utf-8"))  # to get the python dict from string
)


# subscribe to topic
consumer.subscribe(topics=["customer-countries"])

cust_country_map = {}

try:
    while True:
        records = consumer.poll(timeout_ms=100)
        for topic_partition, messages in records.items():
            for message in messages:
                print(f"Topic: {message.topic}, Partition:{message.partition}, Offset:{message.offset}")
                print(f"Message Key: {message.key}, Message Value: {message.value}")

                customer_country = message.value.get("country")
                if customer_country in cust_country_map:
                    cust_country_map[customer_country] += 1
                else:
                    cust_country_map[customer_country] = 1
                print(cust_country_map)
except KafkaError as e:
    print(f"Error while consuming message: {e}")
finally:
    consumer.close()