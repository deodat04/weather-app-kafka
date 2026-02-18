import csv
import json
import time
import requests
from datetime import datetime, timedelta
import os
from kafka import KafkaProducer

# producer = KafkaProducer(
#     bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

def create_producer():
    while True:
        try:
            print("🔄 Tentative de connexion à Kafka...")
            producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("✅ Connecté à Kafka")
            return producer
        except Exception as e:
            print("❌ Kafka non disponible, nouvelle tentative dans 3s :", e)
            time.sleep(3)
#producer = create_producer()


API_URL = 'https://archive-api.open-meteo.com/v1/archive'

end_date = datetime.today().date()
start_date = end_date - timedelta(days=90)


def fetch_weather(lat, lon):
    parameter = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start_date,
                "end_date": end_date,
                "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
                "timezone": timezone
    }
    response = requests.get(API_URL, params=parameter)
    return response.json()

while True :
    with open('cities_101_with_coords.csv', 'r') as infile :
        reader = csv.DictReader(infile)

        for row in reader:
            city = row["city_name"]
            lat = row["lat"]
            lon = row["lon"]
            timezone = row["timezone"]

            print(f"\n=== {city} ({lat}, {lon}) ===")

            data = fetch_weather(lat, lon)

            hourly = data["hourly"]

            for i in range(0, len(hourly["time"]), 1):
                message = {
                    "city": city,
                    "time": hourly["time"][i],
                    "Temp": hourly["temperature_2m"][i],
                    "Hum:": hourly["relative_humidity_2m"][i],
                    "Vent:": hourly["wind_speed_10m"][i]
                }

                #producer.send("weather-topic", message)
                #print("Sent:888888", message)

    time.sleep(10)