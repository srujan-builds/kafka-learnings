from kafka import KafkaConsumer
from kafka.errors import KafkaError, CommitFailedError
import json

# consumer instance
consumer = KafkaConsumer(
    bootstrap_servers = ["localhost:9092"],
    group_id = "CountryCountry",
    key_deserializer = lambda m: m.decode("utf-8"),
    value_deserializer = lambda m : json.loads(m.decode("utf-8")),
    enable_auto_commit = False
)


# subscribe to topic
consumer.subscribe(topics=["customer-countries"])

cust_country_map = {}

def commit_callback(offsets, exception):
    if exception is not None:
        print(f"Mesages failed to commit at offset: {offsets}")
    else:
        print(f"Messages commit at offset :{offsets}")

msg_count = 0


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
                msg_count += 1

                if msg_count%10 == 0:
                    # consumer.commit_async(callback=commit_callback)
                    consumer.commit()
                    print(f"Commites after {msg_count}. Current offset:{message.offset}")

                # if message.offset%2 == 0:
                #     consumer.commit_async(callback=commit_callback)

except KafkaError as e:
    print(f"Error while consuming message: {e}")
finally:
    try:
        consumer.commit()
    finally:
        consumer.close()