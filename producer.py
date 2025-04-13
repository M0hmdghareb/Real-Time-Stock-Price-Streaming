#producer 
import yfinance as yf
from confluent_kafka import Producer
import json
import time

symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]  # List of ticker symbols
# symbols = ["AAPL"]
topic = "stock_prices"

def fetch_stock_data(symbol):
    stock = yf.Ticker(symbol)
    data = stock.history(period="1d", interval="1m")
    latest = data.iloc[-1].to_dict()
    return {
        "symbol": symbol,
        "timestamp": data.index[-1].isoformat(),
        "open": latest["Open"],
        "high": latest["High"],
        "low": latest["Low"],
        "close": latest["Close"],
        "volume": latest["Volume"]
    }

# Kafka Producer
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

while True:
    for symbol in symbols:  # Loop through each ticker symbol
        try:
            data = fetch_stock_data(symbol)
            producer.produce(topic, json.dumps(data).encode('utf-8'))
            print(f"Sent: {data}")
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
    producer.flush()
    time.sleep(5)  # Fetch every 1 minute