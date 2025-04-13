from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import psycopg2

# ---------- Spark Session ----------
spark = SparkSession.builder \
    .appName("StockSymbolSeparator") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# ---------- TimescaleDB Connection Info ----------
TSDB_URL = "localhost"
TSDB_PORT = 5434
TSDB_DB = "processed"
TSDB_USER = "postgres"
TSDB_PASSWORD = "2590"

# ---------- Track last processed timestamp ----------
last_processed_time = "2024-01-01 00:00:00"

# ---------- Ensure table exists ----------
def create_hypertable_for_symbol(symbol):
    table_name = f"stock_{symbol.lower()}"
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT,
            timestamp TIMESTAMPTZ,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume BIGINT
        );
        SELECT create_hypertable('{table_name}', 'timestamp', if_not_exists => TRUE);
    """

    try:
        with psycopg2.connect(
            host=TSDB_URL,
            port=TSDB_PORT,
            dbname=TSDB_DB,
            user=TSDB_USER,
            password=TSDB_PASSWORD
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                conn.commit()
    except Exception as e:
        print(f"[ERROR] Could not create hypertable for {symbol}: {e}")
        

# ---------- Read new rows ----------
def read_new_data():
    global last_processed_time

    query = f"(SELECT * FROM stocks WHERE timestamp > '{last_processed_time}') AS tmp"

    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://localhost:5432/stream") \
        .option("dbtable", query) \
        .option("user", "postgres") \
        .option("password", "2590") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    if df.count() > 0:
        last_processed_time = df.agg({"timestamp": "max"}).collect()[0][0]

    return df

# ---------- Write each symbol's data to its own table ----------
def process_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id}...")
    if batch_df.rdd.isEmpty():
        print("Empty trigger batch.")
        return

    df = read_new_data()
    row_count = df.count()
    print(f"New rows read: {row_count}")
    if row_count == 0:
        return

    symbols = [row['symbol'] for row in df.select("symbol").distinct().collect()]
    print("Symbols to process:", symbols)

    for symbol in symbols:
        table_name = f"stock_prices_{symbol.lower()}"
        symbol_df = df.filter(col("symbol") == symbol)

        print(f"Creating hypertable (if not exists) for: {symbol}")
        create_hypertable_for_symbol(symbol)

        print(f"Writing {symbol_df.count()} rows to table: {table_name}")
        symbol_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{TSDB_URL}:{TSDB_PORT}/{TSDB_DB}") \
            .option("dbtable", table_name) \
            .option("user", TSDB_USER) \
            .option("password", TSDB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

# ---------- Trigger Structured Streaming ----------
(spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .load()
    .writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime="1 minute")
    .start()
    .awaitTermination()
)
