import os
import logging
from dotenv import load_dotenv
from datetime import datetime
import sys
import time
from typing import Optional, Iterable, Any, List, Tuple, Mapping

from quixstreams.sinks.base.sink import BaseSink
from quixstreams.models import HeaderValue

try:
    import duckdb
except ImportError as exc:
    raise ImportError(
        'Package "duckdb" is missing: '
        "run pip install quixstreams[duckdb] to fix it"
    ) from exc

from quixstreams.sinks.base import BatchingSink, SinkBatch
from quixstreams.sinks.exceptions import SinkBackpressureError

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class DuckDBSink(BatchingSink):  # Changed from BaseSink to BatchingSink
    def __init__(self, database_path, table_name, schema, batch_size=1000):
        """
        :param database_path: Path to the DuckDB database file.
        :param table_name: Name of the table to write data to.
        :param schema: Dictionary where keys are column names and values are DuckDB data types.
        :param batch_size: Number of records to write in one batch.
        """
        super().__init__()
        self.database_path = database_path
        self.table_name = table_name
        self.schema = schema
        self.batch_size = batch_size
        self.buffer = []
        logger.debug("Initializing DuckDBSink with database_path=%s, table_name=%s, schema=%s", database_path, table_name, schema)
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        logger.debug("Ensuring table %s exists in database %s", self.table_name, self.database_path)
        conn = duckdb.connect(self.database_path)
        columns = ", ".join([f"{key} {dtype}" for key, dtype in self.schema.items()])
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            {columns}
        )
        """
        logger.debug("Executing query: %s", create_table_query)
        conn.execute(create_table_query)
        conn.close()
        logger.debug("Table %s ensured", self.table_name)

    def add(self, value, key, timestamp, headers, topic, partition, offset):
        logger.debug("Adding record to buffer: %s", value)
        if not isinstance(value, Mapping):
            raise TypeError(
                f'Sink "{self.__class__.__name__}" supports only dictionaries,'
                f" got {type(value)}"
            )
        return super().add(
            value=value,
            key=key,
            timestamp=timestamp,
            headers=headers,
            topic=topic,
            partition=partition,
            offset=offset,
        )

    def write(self, batch: SinkBatch):
        conn = duckdb.connect(self.database_path)
        for write_batch in batch.iter_chunks(n=self.batch_size):
            records = []
            min_timestamp = None
            max_timestamp = None

            for item in write_batch:
                value = item.value
                if 'timestamp' in value:
                    value['timestamp'] = datetime.utcfromtimestamp(value['timestamp']).isoformat()
                records.append(value)
                if min_timestamp is None or value['timestamp'] < min_timestamp:
                    min_timestamp = value['timestamp']
                if max_timestamp is None or value['timestamp'] > max_timestamp:
                    max_timestamp = value['timestamp']

            columns = ", ".join(records[0].keys())
            placeholders = ", ".join(['?' for _ in records[0].keys()])
            query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
            values = [list(record.values()) for record in records]

            try:
                _start = time.monotonic()
                conn.executemany(query, values)
                elapsed = round(time.monotonic() - _start, 2)
                logger.info(
                    f"Sent data to DuckDB; "
                    f"total_records={len(records)} "
                    f"min_timestamp={min_timestamp} "
                    f"max_timestamp={max_timestamp} "
                    f"time_elapsed={elapsed}s"
                )
            except duckdb.Error as exc:
                logger.error("Error writing to DuckDB: %s", exc)
                raise
        conn.close()

    def on_paused(self, topic, partition):
        logger.debug("Sink paused for topic %s, partition %s", topic, partition)
        # Handle backpressure if needed
        pass
