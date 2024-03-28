import os
import uuid

from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import time
import random


HOUSTON_COORDINATES = {"latitude": 29.7604, "longitude": 95.3698}

DALLAS_COORDINATES = {"latitude": 32.7767, "longitude": 96.7970}

# Calculating the movement
LATITUDE_INCREMENT = (DALLAS_COORDINATES['latitude'] - HOUSTON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (DALLAS_COORDINATES['longitude'] - HOUSTON_COORDINATES['longitude']) / 100

# Environment variables for configuration

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

# random.seed(42)

start_time = datetime.now()
start_location = HOUSTON_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def simulate_vehicle_movement():
    global start_location

    # move towards Dallas
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # adding randomness
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'time_stamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 50),
        'direction': 'North-East',
        'make': 'Mercedes',
        'model': "AMG CLE",
        'year': 2024,
        'fuel_type': 'Hybrid'
    }


def generate_gps_data(device_id, time_stamp, vehicle_type='private'):
    return{
        'id': uuid.uuid4(),
        'device_id': device_id,
        'time_stamp': time_stamp,
        'speed': random.uniform(10, 50),
        'direction': 'North-East',
        'vehicle_type': vehicle_type
    }


def generate_traffic_cam_data(device_id, time_stamp, location, camera_id):
    return{
        'id': uuid.uuid4(),
        'device_id': device_id,
        'camera_id': camera_id,
        'location': location,
        'time_stamp': time_stamp,
        'snapshot': 'Base64EncodedString'
    }


def generate_weather_data(device_id, time_stamp, location):
    return{
        'id': uuid.uuid4(),
        'device_id': device_id,
        'location': location,
        'time_stamp': time_stamp,
        'temperature': random.uniform(55, 83),
        'weather_condition': random.choice(['Sunny', 'Cloudy', 'Rainy', 'Snowy']),
        'precipitation': random.uniform(0, 15),
        'wind_speed': random.uniform(0, 100),
        'humidity': random.randint(0, 100), # percentage
        'air_quality_index': random.uniform(10, 80)
    }


def generate_emergency_incident_data(device_id, time_stamp, location):
    return{
        'id': uuid.uuid4(),
        'device_id': device_id,
        'incident_id': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'time_stamp': time_stamp,
        'location': location,
        'incident_status': random.choice(['Active', 'Resolved']),
        'incident_desc': 'Some description'
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def delivery_report(error, message):
    if error is not None:
        print(f"Message delivery Failed: {error}")
    else:
        print(f"Message delivered to the topic {message.topic()} [{message.partition()}]")


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['time_stamp'])
        traffic_cam_data = generate_traffic_cam_data(device_id, vehicle_data['time_stamp'], vehicle_data['location'], 'Canon 5498')
        weather_data = generate_weather_data(device_id, vehicle_data['time_stamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['time_stamp'],vehicle_data['location'])

        # print(vehicle_data)
        # print(gps_data)
        # print(traffic_cam_data)
        # print(weather_data)
        # print(emergency_incident_data)

        if (vehicle_data['location'][0] >= DALLAS_COORDINATES['latitude']
         and vehicle_data['location'][1] <= DALLAS_COORDINATES['longitude']):
            print('The vehicle has reached Dallas. Simulation ended ...')
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_cam_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(15)


if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda error: print('Kafka error: {error}')
    }

    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, "Rapos darling")
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print('Unexpected error occurred', e)





