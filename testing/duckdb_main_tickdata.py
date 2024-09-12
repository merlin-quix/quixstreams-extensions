from quixstreams import Application
from sinks.duckdbsink import DuckDBSink
import os

app = Application(
    consumer_group="sinktest-v7",
    auto_offset_reset="earliest",
    consumer_extra_config={'max.poll.interval.ms': 300000},
    commit_every=100

)
topic = app.topic(os.getenv("input", "raw_data"))

# Initialize DuckDBSink
duckdb_sink = DuckDBSink(
    database_path="csgo_data_v5.db",
    table_name="tick_metadata",
    batch_size=100,

    schema={
        "timestamp": "TIMESTAMP",
        "tick": "INTEGER",
        "inventory": "TEXT",
        "accuracy_penalty": "FLOAT",
        "zoom_lvl": "TEXT",  # Assuming this can be null or empty
        "is_bomb_planted": "BOOLEAN",
        "ping": "INTEGER",
        "health": "INTEGER",
        "has_defuser": "BOOLEAN",
        "has_helmet": "BOOLEAN",
        "flash_duration": "INTEGER",
        "last_place_name": "TEXT",
        "which_bomb_zone": "INTEGER",
        "armor_value": "INTEGER",
        "current_equip_value": "INTEGER",
        "team_name": "TEXT",
        "team_clan_name": "TEXT",
        "game_time": "FLOAT",
        "pitch": "FLOAT",
        "yaw": "FLOAT",
        "X": "FLOAT",
        "Y": "FLOAT",
        "Z": "FLOAT",
        "steamid": "TEXT",
        "name": "TEXT",
        "round": "INTEGER"
    }    # Dictionary of column names and their data types
)

# Create a StreamingDataFrame from the topic
sdf = app.dataframe(topic)
sdf = sdf.update(lambda message: print(f"Received message: {message}"))

# Sink data to InfluxDB
sdf.sink(duckdb_sink)

app.run(sdf)

