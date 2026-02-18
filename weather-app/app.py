# python
from pathlib import Path
import json
import time
import os

from kafka import KafkaProducer


BASE_DIR = Path(__file__).resolve().parent
JSON_PATH = BASE_DIR / 'weather.json'

if not JSON_PATH.exists():
    print(f"❌ Fichier introuvable: {JSON_PATH}")
    raise SystemExit(1)

# Attendre que Kafka soit prêt
print("⏳ Attente du démarrage de Kafka...")
time.sleep(10)

def create_producer():
    while True:
        try:
            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
            print(f"🔄 Tentative de connexion à {bootstrap_servers}...")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000
            )
            print("✅ Connecté à Kafka")
            return producer
        except Exception as e:
            print(f"❌ Erreur : {e}")
            print("🔄 Nouvelle tentative dans 3s...")
            time.sleep(3)

producer = create_producer()

with JSON_PATH.open('r', encoding='utf-8') as f:
    chunks = json.load(f)

print(f"📦 {len(chunks)} chunks chargés")

for chunk in chunks:
    city = chunk.get("city")
    data_json_str = chunk.get("data_json")

    if not (city and data_json_str):
        print(f"⚠️ Chunk incomplet, skip")
        continue

    try:
        hourly_data = json.loads(data_json_str)
    except json.JSONDecodeError:
        print(f"❌ Erreur parsing JSON pour {city}")
        continue

    print(f"\n=== {city} ({len(hourly_data)} enregistrements) ===")

    for record in hourly_data:
        message = {
            "city": city,
            "time": record.get("time"),
            "Temp": record.get("temperature_2m"),
            "Hum": record.get("relative_humidity_2m"),
            "Vent": record.get("wind_speed_10m")
        }

        producer.send("weather-topic", message)

        producer.send("weather-topic", message)
        print(f"📤 {message}")


print("\n✅ Tous les messages ont été envoyés.")
producer.close()
