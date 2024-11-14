import requests
import time
from kafka import KafkaProducer
import json

# Add this delay to ensure Kafka is ready
time.sleep(10)  # Wait 10 seconds before starting the producer

API_KEY = "04e42cf83b67a00e43f8d68b03033e2b"
CITIES = ["Montreal", "Toronto", "Vancouver", "Calgary", "Ottawa"]  # List of cities
URL = "http://api.openweathermap.org/data/2.5/weather"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_weather_data():
    for city in CITIES:
        try:
            params = {
                "q": city,
                "appid": API_KEY
            }
            response = requests.get(URL, params=params)
            if response.status_code == 200:
                data = response.json()
                # Add city name explicitly in data
                data["city"] = city
                # Send data to Kafka
                producer.send("weather_data", value=data)
                print(f"Sent weather data to Kafka for {city}:", data)
            else:
                print(f"Failed to retrieve data for {city}, Status code:", response.status_code)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city}: {e}")

while True:
    fetch_weather_data()
    # Fetch data every 10 minutes
    time.sleep(600)
