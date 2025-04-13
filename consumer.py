# consumer.py
from confluent_kafka import Consumer
import psycopg2
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stock-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(["stock_prices"])

# PostgreSQL Connection
# PostgreSQL Connection
conn = psycopg2.connect(
    dbname="stream",
    user="postgres",
    password="2590",
    host="localhost"
)
cursor = conn.cursor()

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    data = json.loads(msg.value().decode('utf-8'))
    cursor.execute("""
        INSERT INTO stocks (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING           
    """, (
        data["symbol"],
        data["timestamp"],
        data["open"],
        data["high"],
        data["low"],
        data["close"],
        data["volume"]
    ))
    conn.commit()

cursor.close()
conn.close()