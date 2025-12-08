from constants import BIKES_STATION_INFORMATION, BIKES_STATION_STATUS, BIKES_STATION_INFORMATION_TOPIC, BIKES_STATION_STATUS_TOPIC
from bikes import Bike


def bikes_station_status_data(obj):
    try:
        obj.get_bikes_station_information(BIKES_STATION_STATUS, params={})
        print(f"Bike station staus data pushed to topic-{BIKES_STATION_STATUS_TOPIC}")
    except Exception as e:
        print(f"Error while getting the data: {e}")


def bikes_station_information_data(obj):
    try:
        obj.get_bikes_station_information(BIKES_STATION_INFORMATION, params={})
        print(f"Bike station staus data pushed to topic-{BIKES_STATION_INFORMATION_TOPIC}")
    except Exception as e:
        print(f"Error while getting the data: {e}")


if __name__ == "__main__":
    bike = Bike()
    bikes_station_information_data(bike)
    bikes_station_status_data(bike)


# bike = Bike()
# # bike.get_bikes_station_information(BIKES_STATION_INFORMATION, params={})

# # bike.get_bikes_status_information(BIKES_STATION_STATUS, params={})

# try:
#     bike.get_bikes_station_information(BIKES_STATION_INFORMATION, params={})
#     bike.get_bikes_status_information(BIKES_STATION_STATUS, params={})
# except Exception as e:
#     print(f"Error while getting the data: {e}")
