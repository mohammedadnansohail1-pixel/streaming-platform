"""Spark Structured Streaming job for event processing."""

import json
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    window,
    count,
    sum as spark_sum,
    avg,
    countDistinct,
    expr,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.avro.functions import from_avro
from core.utils.logging import get_logger

logger = get_logger(__name__)


class StreamingJob:
    """
    Spark Structured Streaming job for processing events.

    Usage:
        job = StreamingJob(config, schemas)
        job.start()
        job.await_termination()
    """

    def __init__(self, config: dict, schemas: dict):
        """
        Args:
            config: Platform + domain config
            schemas: Dict of event_name → Avro schema
        """
        self.config = config
        self.schemas = schemas

        # Kafka settings
        kafka_config = config.get("kafka", {})
        self.bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")
        self.topic_prefix = kafka_config.get("topics", {}).get("prefix", "events")

        # Schema Registry
        schema_registry = kafka_config.get("schema_registry", {})
        self.schema_registry_url = schema_registry.get("url", "http://localhost:8081")

        # Spark settings
        spark_config = config.get("spark", {})
        self.app_name = spark_config.get("app_name", "streaming-platform")
        self.master = spark_config.get("master", "local[*]")
        self.trigger_interval = spark_config.get("trigger_interval", "10 seconds")

        # Watermark for late data
        watermark = spark_config.get("watermark", {})
        self.watermark_delay = watermark.get("delay", "10 minutes")

        # Checkpoint
        self.checkpoint_path = spark_config.get("checkpoint", {}).get(
            "path", "/tmp/checkpoints"
        )

        self._spark: Optional[SparkSession] = None
        self._queries = []

    def _get_spark(self) -> SparkSession:
        """Get or create SparkSession."""
        if self._spark is None:
            logger.info(f"Creating SparkSession: {self.app_name}")
            self._spark = (
                SparkSession.builder.appName(self.app_name)
                .master(self.master)
                .config("spark.sql.streaming.schemaInference", "true")
                .config(
                    "spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "org.apache.spark:spark-avro_2.12:3.5.0",
                )
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("WARN")
        return self._spark

    def _build_schema(self, event_type: str) -> StructType:
        """Convert Avro schema to Spark StructType."""
        avro_schema = self.schemas[event_type]
        fields = []

        for field in avro_schema["fields"]:
            field_name = field["name"]
            field_type = field["type"]

            # Handle nullable types
            if isinstance(field_type, list):
                actual_type = [t for t in field_type if t != "null"][0]
            else:
                actual_type = field_type

            # Handle logical types
            if isinstance(actual_type, dict):
                actual_type = actual_type.get("type", "string")

            # Map to Spark types
            if actual_type == "string":
                spark_type = StringType()
            elif actual_type == "long":
                spark_type = LongType()
            elif actual_type == "double":
                spark_type = DoubleType()
            else:
                spark_type = StringType()

            fields.append(StructField(field_name, spark_type, True))

        return StructType(fields)

    def read_stream(self, event_types: list[str]) -> DataFrame:
        """
        Read events from Kafka topics.

        Args:
            event_types: List of event types to subscribe to

        Returns:
            Streaming DataFrame
        """
        spark = self._get_spark()
        topics = ",".join([f"{self.topic_prefix}.{et}" for et in event_types])

        logger.info(f"Reading from topics: {topics}")

        # Read raw from Kafka
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", topics)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        return df

    def parse_events(self, df: DataFrame, event_type: str) -> DataFrame:
        """
        Parse Kafka messages to structured events.

        Args:
            df: Raw Kafka DataFrame
            event_type: Type of event to parse

        Returns:
            Parsed DataFrame with schema
        """
        avro_schema_json = json.dumps(self.schemas[event_type])

        # Parse Avro value (skip first 5 bytes - schema registry header)
        parsed = df.select(
            from_avro(
                expr("substring(value, 6)"),  # Skip schema registry magic bytes
                avro_schema_json,
            ).alias("event")
        ).select("event.*")

        # Add watermark for late data handling
        parsed = parsed.withWatermark("timestamp", self.watermark_delay)

        return parsed

    def aggregate_events(self, df: DataFrame, aggregation: dict) -> DataFrame:
        """
        Apply window aggregation to events.

        Args:
            df: Parsed events DataFrame
            aggregation: Aggregation config from domain config

        Returns:
            Aggregated DataFrame
        """
        agg_name = aggregation["name"]
        agg_type = aggregation["type"]
        window_config = aggregation["window"]
        window_type = window_config["type"]
        window_size = window_config.get("size", window_config.get("duration"))

        logger.info(
            f"Applying aggregation: {agg_name} ({agg_type}) with {window_type} window"
        )

        # Convert timestamp from milliseconds to timestamp type
        df = df.withColumn("event_time", col("timestamp").cast("timestamp"))

        # Build window
        if window_type == "tumbling":
            win = window("event_time", window_size)
        elif window_type == "sliding":
            slide = window_config.get("slide", "1 minute")
            win = window("event_time", window_size, slide)
        else:
            raise ValueError(f"Unknown window type: {window_type}")

        # Apply aggregation
        if agg_type == "count":
            group_by = aggregation.get("group_by", [])
            result = df.groupBy(win, *group_by).agg(count("*").alias("count"))

        elif agg_type == "count_distinct":
            field = aggregation["field"]
            result = df.groupBy(win).agg(countDistinct(field).alias("unique_count"))

        elif agg_type == "sum":
            field = aggregation["field"]
            group_by = aggregation.get("group_by", [])
            result = df.groupBy(win, *group_by).agg(spark_sum(field).alias("total"))

        elif agg_type == "avg":
            field = aggregation["field"]
            group_by = aggregation.get("group_by", [])
            result = df.groupBy(win, *group_by).agg(avg(field).alias("average"))

        else:
            raise ValueError(f"Unknown aggregation type: {agg_type}")

        return result

    def write_console(self, df: DataFrame, query_name: str) -> None:
        """Write stream to console (for debugging)."""
        query = (
            df.writeStream.outputMode("update")
            .format("console")
            .option("truncate", "false")
            .option("checkpointLocation", f"{self.checkpoint_path}/{query_name}")
            .trigger(processingTime=self.trigger_interval)
            .queryName(query_name)
            .start()
        )
        self._queries.append(query)
        logger.info(f"Started query: {query_name}")

    def write_memory(self, df: DataFrame, query_name: str) -> None:
        """Write stream to memory table (for testing)."""
        query = (
            df.writeStream.outputMode("complete")
            .format("memory")
            .queryName(query_name)
            .start()
        )
        self._queries.append(query)
        logger.info(f"Started memory query: {query_name}")

    def write_foreach_batch(
        self, df: DataFrame, batch_func: callable, query_name: str
    ) -> None:
        """
        Write stream using foreachBatch with custom function.

        Args:
            df: Streaming DataFrame
            batch_func: Function(batch_df, batch_id) to process each batch
            query_name: Name for the streaming query
        """
        query = (
            df.writeStream.outputMode("update")
            .foreachBatch(batch_func)
            .option("checkpointLocation", f"{self.checkpoint_path}/{query_name}")
            .trigger(processingTime=self.trigger_interval)
            .queryName(query_name)
            .start()
        )
        self._queries.append(query)
        logger.info(f"Started foreachBatch query: {query_name}")

    def run_aggregation_job(self, event_type: str, aggregation_name: str) -> None:
        """
        Run a streaming aggregation job.

        Args:
            event_type: Event type to process
            aggregation_name: Name of aggregation from config
        """
        # Find aggregation config
        aggregations = self.config.get("aggregations", [])
        agg_config = next(
            (a for a in aggregations if a["name"] == aggregation_name), None
        )
        if not agg_config:
            raise ValueError(f"Aggregation not found: {aggregation_name}")

        # Build pipeline
        raw_df = self.read_stream([event_type])
        parsed_df = self.parse_events(raw_df, event_type)
        agg_df = self.aggregate_events(parsed_df, agg_config)

        # Write to console
        self.write_console(agg_df, f"{event_type}_{aggregation_name}")

    def await_termination(self, timeout: Optional[int] = None) -> None:
        """Wait for all queries to terminate."""
        spark = self._get_spark()
        if timeout:
            spark.streams.awaitAnyTermination(timeout * 1000)
        else:
            spark.streams.awaitAnyTermination()

    def stop(self) -> None:
        """Stop all queries and SparkSession."""
        for query in self._queries:
            query.stop()
        if self._spark:
            self._spark.stop()
        logger.info("Streaming job stopped")
