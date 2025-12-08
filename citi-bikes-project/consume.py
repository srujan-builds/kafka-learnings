from kafka_consumer import Consumer
from constants import BIKES_STATION_INFORMATION_TOPIC, BIKES_STATION_STATUS_TOPIC

class Consume:
    def __init__(self):
        self.consumer_grp = Consumer("citi_bike_stations")
        
    def consume(self):
        self.consumer_grp.subscribe_consumer([BIKES_STATION_INFORMATION_TOPIC, BIKES_STATION_STATUS_TOPIC])
        
        for msg in self.consumer_grp.consumer:
            print(msg.topic, msg.partition, msg.offset, msg.value)
            print("+=====================================================================")

    
if __name__ == "__main__":
    consumer_instance = Consume()
    consumer_instance.consume()