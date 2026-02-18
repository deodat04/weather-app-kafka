from pathlib import Path
import csv
import json
import time
import requests
from datetime import datetime, timedelta
import os
from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / 'cities_101_with_coords.csv'

if not CSV_PATH.exists():
    print(f"Fichier introuvable: {CSV_PATH}")
    raise SystemExit(1)

API_URL = 'https://archive-api.open-meteo.com/v1/archive'

end_date = datetime.today().date()
start_date = end_date - timedelta(days=90)

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

def fetch_weather(lat, lon, timezone):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": timezone
    }
    try:
        resp = requests.get(API_URL, params=params, timeout=10)
    except Exception as e:
        print("❌ Erreur requête API :", e)
        return None

    if resp.status_code != 200:
        print(f"❌ API HTTP {resp.status_code} pour {lat},{lon} : {resp.text}")
        return None

    try:
        data = resp.json()
    except ValueError:
        print("❌ Réponse non JSON :", resp.text)
        return None

    if "hourly" not in data:
        print("❌ Clé 'hourly' absente dans la réponse :", data)
        return None

    return data

# producer = create_producer()

while True:
    with CSV_PATH.open('r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            city = row.get("city_name")
            lat = row.get("lat")
            lon = row.get("lon")
            timezone = row.get("timezone")

            if not (city and lat and lon and timezone):
                print("⚠️ Ligne CSV incomplète, skip :", row)
                continue

            print(f"\n=== {city} ({lat}, {lon}) ===")

            data = fetch_weather(lat, lon, timezone)
            if data is None:
                print("⏭️ Données non disponibles pour cette ville, on passe à la suivante.")
                continue

            hourly = data["hourly"]

            # sécurité si les listes n'ont pas la même longueur
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            hums = hourly.get("relative_humidity_2m", [])
            vents = hourly.get("wind_speed_10m", [])

            length = min(len(times), len(temps), len(hums), len(vents))

            for i in range(length):
                message = {
                    "city": city,
                    "time": times[i],
                    "Temp": temps[i],
                    "Hum": hums[i],
                    "Vent": vents[i]
                }

                # if producer:
                #     producer.send("weather-topic", message)
                print("Prepared message:", message)

    time.sleep(10)
