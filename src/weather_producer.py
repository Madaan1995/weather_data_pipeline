import requests
import time
from kafka import KafkaProducer, errors
import json

# Add this delay to ensure Kafka is ready
time.sleep(10)  # Wait 10 seconds before starting the producer

API_KEY = "04e42cf83b67a00e43f8d68b03033e2b"
CITY = "Montreal"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_weather_data():
    response = requests.get(URL)
    if response.status_code == 200:
        data = response.json()
        producer.send("weather_data", value=data)
        print("Sent weather data to Kafka:", data)
    else:
        print("Failed to retrieve data", response.status_code)

while True:
    fetch_weather_data()
    time.sleep(60)
