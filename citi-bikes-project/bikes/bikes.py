from constants import BIKES_STATION_STATUS_TOPIC, BIKES_STATION_INFORMATION_TOPIC, BIKES_STATION_INFORMATION, BIKES_STATION_STATUS
from services import HttpService
from kafka_producer import Producer

class Bike:
    def __init__(self):
        self.http_service = HttpService()
        self.producer = Producer()

    def get_bikes_station_information(self, url, params):
        # res = self.http_service.get(url, params=params).json()

        response = self.http_service.get(url, params=params)

        if response is None:
            print("Error: API request failed. Skipped processing")
            return
        
        res = response.json()


        for msg in res["data"]["stations"]:
            # print("bike_station_information ==>", msg)
            self.producer.data_producer(topic=BIKES_STATION_INFORMATION_TOPIC, msg=msg)

    def get_bikes_status_information(self, url,params):
        # res = self.http_service.get(url, params=params).json()

        response = self.http_service.get(url, params=params)

        if response is None:
            print("Error: API request failed. Skipped processing")
            return
        
        res = response.json()
        
        for msg in res["data"]["stations"]:
            # print("bike_status_information ==>", msg)
            self.producer.data_producer(topic=BIKES_STATION_STATUS_TOPIC, msg=msg)