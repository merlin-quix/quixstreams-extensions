from quixstreams import Application
from sinks.duckdbsink import DuckDBSink
import os

app = Application(
    consumer_group="sinktest-v2a",
)
topic = app.topic(os.getenv("input", "raw_data"))

# Initialize DuckDBSink
duckdb_sink = DuckDBSink(
    database_path="testdb.db",
    table_name="user_actions",
    batch_size=500,
    schema={
        "timestamp": "TIMESTAMP",
        "user_id": "TEXT",
        "page_id": "TEXT",
        "action": "TEXT",
    }  # Dictionary of column names and their data types
)

# Create a StreamingDataFrame from the topic
sdf = app.dataframe(topic)
sdf = sdf.update(lambda message: print(f"Received message: {message}"))

# Sink data to InfluxDB
sdf.sink(duckdb_sink)

app.run(sdf)

