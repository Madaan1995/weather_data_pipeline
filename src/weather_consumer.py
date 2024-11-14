from kafka import KafkaConsumer
import json
import psycopg2
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Kafka consumer configuration
    consumer = KafkaConsumer(
        'weather_data',
        bootstrap_servers='kafka:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather-consumer-group'
    )
    logger.info("Connected to Kafka, waiting for messages...")

    # PostgreSQL connection configuration
    conn = psycopg2.connect(
        dbname="weather_data",
        user="weather_user",
        password="weather_password",
        host="postgres"
    )
    cursor = conn.cursor()
    logger.info("Connected to PostgreSQL.")

    # Ensure the table exists
    logger.info("Ensuring the weather table exists...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather (
        id SERIAL PRIMARY KEY,
        city VARCHAR(50),
        temperature FLOAT,
        feels_like FLOAT,
        temp_min FLOAT,
        temp_max FLOAT,
        humidity INT,
        description VARCHAR(50),
        wind_speed FLOAT,
        wind_deg INT,
        cloud_coverage INT,
        pressure INT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    logger.info("Weather table is ready.")

    # Consume messages from Kafka
    for message in consumer:
        try:
            logger.info(f"Message received from Kafka: {message.value}")
            weather_data = message.value
            city = weather_data.get('city', 'Unknown')
            main = weather_data.get('main', {})
            wind = weather_data.get('wind', {})
            clouds = weather_data.get('clouds', {})
            weather_desc = weather_data.get('weather', [{}])[0].get('description', 'No description')

            # Insert data into PostgreSQL
            cursor.execute('''
            INSERT INTO weather (city, temperature, feels_like, temp_min, temp_max, humidity, description,
                                 wind_speed, wind_deg, cloud_coverage, pressure)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                city,
                main.get('temp'),
                main.get('feels_like'),
                main.get('temp_min'),
                main.get('temp_max'),
                main.get('humidity'),
                weather_desc,
                wind.get('speed'),
                wind.get('deg'),
                clouds.get('all'),
                main.get('pressure')
            ))
            conn.commit()
            logger.info(f"Inserted data for city: {city}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            conn.rollback()

except Exception as e:
    logger.critical(f"Critical error: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    if consumer:
        consumer.close()
    logger.info("Consumer script has terminated.")
