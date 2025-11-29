from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
import json

# Consumer instance
consumer = KafkaConsumer(
    bootstrap_servers = ["localhost:9092"],
    group_id = "CountryCountry",
    key_deserializer = lambda m: m.decode("utf-8"),
    value_deserializer = lambda m: json.loads(m.decode("utf-8")),
    enable_auto_commit = False
)

# consumer.subscribe(topics=["customer-countries"]) 
topic_partition = TopicPartition(topic='customer-countries', partition=0) # subscription is done in this line
consumer.assign(partitions=[topic_partition])
consumer.seek(partition=topic_partition, offset=13) # specifying from which offset of a partition the consumer needs to consume message

running = True

# offset = 10

try:
    while running:
        records = consumer.poll(timeout_ms=100)
        for tp, messages in records.items():
            for message in messages:
                print(f"Topic: {message.topic}, Partition:{message.partition}, Offset:{message.offset}")
                print(f"Message Key: {message.key}, Message Value: {message.value}")

                '''
                Commit the next offset to the broker manually
                For this specific partition
                Without waiting for broker response / Without callback
                '''
                consumer.commit_async({
                    tp : OffsetAndMetadata(
                        offset=message.offset+1, # next position to read
                        metadata="",             # optional notes
                        leader_epoch=0           # internal safety
                    )          
                })

except Exception as e:
    print(f"Error while consuming message: {e}")
finally:
    consumer.close()