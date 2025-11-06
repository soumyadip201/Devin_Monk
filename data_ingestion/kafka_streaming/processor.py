"""
Kafka Streaming Processor

Handles real-time data streaming using Kafka with Spark Streaming.
"""

import os
import json
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_json, struct, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import structlog

from ..mongodb_connector.connection import get_mongodb_connector

logger = structlog.get_logger(__name__)


class KafkaStreamProcessor:
    """
    Kafka streaming processor using Spark Structured Streaming.
    """
    
    def __init__(self, 
                 app_name: str = "Kafka_Stream_Processor",
                 kafka_bootstrap_servers: str = None,
                 kafka_topics: List[str] = None):
        """
        Initialize Kafka stream processor.
        
        Args:
            app_name: Spark application name
            kafka_bootstrap_servers: Kafka bootstrap servers
            kafka_topics: List of Kafka topics to consume
        """
        self.app_name = app_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_topics = kafka_topics or ['weather-data', 'stock-data', 'csv-data']
        self.spark = None
        self.mongodb_connector = get_mongodb_connector()
        
        logger.info("Kafka stream processor initialized", 
                   app_name=app_name,
                   bootstrap_servers=self.kafka_bootstrap_servers,
                   topics=self.kafka_topics)
    
    def initialize_spark(self) -> bool:
        """
        Initialize Spark session with Kafka streaming support.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .getOrCreate()
            
            # Set log level to reduce verbosity
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark session with Kafka support initialized", 
                       spark_version=self.spark.version)
            return True
            
        except Exception as e:
            logger.error("Failed to initialize Spark session", error=str(e))
            return False
    
    def stop_spark(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")
    
    def create_kafka_stream(self, topic: str) -> Optional[DataFrame]:
        """
        Create a Kafka streaming DataFrame.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            Streaming DataFrame or None if error
        """
        if not self.spark:
            logger.error("Spark session not initialized")
            return None
        
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info("Kafka stream created", topic=topic)
            return df
            
        except Exception as e:
            logger.error("Failed to create Kafka stream", 
                        topic=topic, 
                        error=str(e))
            return None
    
    def process_weather_stream(self, df: DataFrame) -> DataFrame:
        """
        Process weather data stream.
        
        Args:
            df: Input streaming DataFrame
            
        Returns:
            Processed DataFrame
        """
        try:
            # Define schema for weather data
            weather_schema = StructType([
                StructField("location_name", StringType(), True),
                StructField("temperature", DoubleType(), True),
                StructField("humidity", IntegerType(), True),
                StructField("pressure", DoubleType(), True),
                StructField("weather_description", StringType(), True),
                StructField("wind_speed", DoubleType(), True),
                StructField("timestamp", StringType(), True)
            ])
            
            # Parse JSON data
            parsed_df = df.select(
                col("key").cast("string").alias("message_key"),
                from_json(col("value").cast("string"), weather_schema).alias("weather_data"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Flatten the structure and add metadata
            processed_df = parsed_df.select(
                col("message_key"),
                col("weather_data.*"),
                col("kafka_timestamp"),
                current_timestamp().alias("processing_timestamp")
            )
            
            logger.info("Weather stream processing configured")
            return processed_df
            
        except Exception as e:
            logger.error("Failed to process weather stream", error=str(e))
            return df
    
    def process_stock_stream(self, df: DataFrame) -> DataFrame:
        """
        Process stock data stream.
        
        Args:
            df: Input streaming DataFrame
            
        Returns:
            Processed DataFrame
        """
        try:
            # Define schema for stock data
            stock_schema = StructType([
                StructField("symbol", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("volume", IntegerType(), True),
                StructField("change", DoubleType(), True),
                StructField("change_percent", DoubleType(), True),
                StructField("timestamp", StringType(), True)
            ])
            
            # Parse JSON data
            parsed_df = df.select(
                col("key").cast("string").alias("message_key"),
                from_json(col("value").cast("string"), stock_schema).alias("stock_data"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Flatten the structure and add metadata
            processed_df = parsed_df.select(
                col("message_key"),
                col("stock_data.*"),
                col("kafka_timestamp"),
                current_timestamp().alias("processing_timestamp")
            )
            
            logger.info("Stock stream processing configured")
            return processed_df
            
        except Exception as e:
            logger.error("Failed to process stock stream", error=str(e))
            return df
    
    def process_csv_stream(self, df: DataFrame) -> DataFrame:
        """
        Process CSV data stream.
        
        Args:
            df: Input streaming DataFrame
            
        Returns:
            Processed DataFrame
        """
        try:
            # For CSV data, we expect a more flexible schema
            # Parse as generic JSON and add metadata
            parsed_df = df.select(
                col("key").cast("string").alias("message_key"),
                col("value").cast("string").alias("csv_data_json"),
                col("timestamp").alias("kafka_timestamp"),
                current_timestamp().alias("processing_timestamp")
            )
            
            logger.info("CSV stream processing configured")
            return parsed_df
            
        except Exception as e:
            logger.error("Failed to process CSV stream", error=str(e))
            return df
    
    def write_to_mongodb_sink(self, df: DataFrame, collection_name: str, checkpoint_location: str) -> bool:
        """
        Write streaming DataFrame to MongoDB using foreachBatch.
        
        Args:
            df: Streaming DataFrame
            collection_name: MongoDB collection name
            checkpoint_location: Checkpoint location for streaming
            
        Returns:
            bool: True if stream started successfully, False otherwise
        """
        try:
            def write_batch_to_mongodb(batch_df, batch_id):
                """Write batch to MongoDB."""
                try:
                    if batch_df.count() > 0:
                        # Convert to Pandas and then to list of dicts
                        pandas_df = batch_df.toPandas()
                        records = pandas_df.to_dict('records')
                        
                        # Connect and insert
                        if self.mongodb_connector.connect():
                            success = self.mongodb_connector.insert_documents(collection_name, records)
                            if success:
                                logger.info("Batch written to MongoDB", 
                                           batch_id=batch_id, 
                                           records=len(records),
                                           collection=collection_name)
                            self.mongodb_connector.disconnect()
                        else:
                            logger.error("Failed to connect to MongoDB for batch", batch_id=batch_id)
                    
                except Exception as e:
                    logger.error("Failed to write batch to MongoDB", 
                               batch_id=batch_id, 
                               error=str(e))
            
            # Start the streaming query
            query = df.writeStream \
                .foreachBatch(write_batch_to_mongodb) \
                .option("checkpointLocation", checkpoint_location) \
                .trigger(processingTime='30 seconds') \
                .start()
            
            logger.info("MongoDB sink started", 
                       collection=collection_name,
                       checkpoint=checkpoint_location)
            
            return True
            
        except Exception as e:
            logger.error("Failed to start MongoDB sink", 
                        collection=collection_name, 
                        error=str(e))
            return False
    
    def start_streaming_pipeline(self, output_mode: str = "append") -> Dict[str, Any]:
        """
        Start the complete streaming pipeline.
        
        Args:
            output_mode: Streaming output mode
            
        Returns:
            Dictionary with pipeline status
        """
        results = {
            'success': False,
            'active_streams': [],
            'error': None
        }
        
        try:
            if not self.initialize_spark():
                results['error'] = "Failed to initialize Spark session"
                return results
            
            active_queries = []
            
            # Process each topic
            for topic in self.kafka_topics:
                try:
                    # Create Kafka stream
                    raw_stream = self.create_kafka_stream(topic)
                    if raw_stream is None:
                        continue
                    
                    # Process based on topic type
                    if 'weather' in topic.lower():
                        processed_stream = self.process_weather_stream(raw_stream)
                        collection_name = "weather_stream_data"
                    elif 'stock' in topic.lower():
                        processed_stream = self.process_stock_stream(raw_stream)
                        collection_name = "stock_stream_data"
                    elif 'csv' in topic.lower():
                        processed_stream = self.process_csv_stream(raw_stream)
                        collection_name = "csv_stream_data"
                    else:
                        # Generic processing
                        processed_stream = raw_stream.select(
                            col("key").cast("string").alias("message_key"),
                            col("value").cast("string").alias("message_value"),
                            col("timestamp").alias("kafka_timestamp"),
                            current_timestamp().alias("processing_timestamp")
                        )
                        collection_name = f"{topic}_stream_data"
                    
                    # Start MongoDB sink
                    checkpoint_location = f"/tmp/spark-checkpoint/{topic}"
                    if self.write_to_mongodb_sink(processed_stream, collection_name, checkpoint_location):
                        results['active_streams'].append({
                            'topic': topic,
                            'collection': collection_name,
                            'checkpoint': checkpoint_location
                        })
                    
                except Exception as e:
                    logger.error("Failed to process topic", topic=topic, error=str(e))
                    continue
            
            if results['active_streams']:
                results['success'] = True
                logger.info("Streaming pipeline started", 
                           active_streams=len(results['active_streams']))
            else:
                results['error'] = "No streams could be started"
            
            return results
            
        except Exception as e:
            logger.error("Streaming pipeline failed", error=str(e))
            results['error'] = str(e)
            return results


class KafkaProducerWrapper:
    """
    Wrapper for Kafka producer to send data to topics.
    """
    
    def __init__(self, bootstrap_servers: str = None):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.producer = None
        
        logger.info("Kafka producer wrapper initialized", 
                   bootstrap_servers=self.bootstrap_servers)
    
    def connect(self) -> bool:
        """
        Connect to Kafka.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432
            )
            
            logger.info("Connected to Kafka")
            return True
            
        except Exception as e:
            logger.error("Failed to connect to Kafka", error=str(e))
            return False
    
    def send_message(self, topic: str, message: Dict[str, Any], key: str = None) -> bool:
        """
        Send message to Kafka topic.
        
        Args:
            topic: Kafka topic name
            message: Message data
            key: Optional message key
            
        Returns:
            bool: True if send successful, False otherwise
        """
        if not self.producer:
            logger.error("Kafka producer not connected")
            return False
        
        try:
            # Add timestamp to message
            message['_kafka_timestamp'] = datetime.utcnow().isoformat()
            
            future = self.producer.send(topic, value=message, key=key)
            record_metadata = future.get(timeout=10)
            
            logger.info("Message sent to Kafka", 
                       topic=topic,
                       partition=record_metadata.partition,
                       offset=record_metadata.offset)
            return True
            
        except Exception as e:
            logger.error("Failed to send message to Kafka", 
                        topic=topic, 
                        error=str(e))
            return False
    
    def send_batch(self, topic: str, messages: List[Dict[str, Any]]) -> int:
        """
        Send batch of messages to Kafka topic.
        
        Args:
            topic: Kafka topic name
            messages: List of message data
            
        Returns:
            int: Number of successfully sent messages
        """
        if not self.producer:
            logger.error("Kafka producer not connected")
            return 0
        
        successful_sends = 0
        
        for i, message in enumerate(messages):
            key = f"batch_{i}_{datetime.utcnow().timestamp()}"
            if self.send_message(topic, message, key):
                successful_sends += 1
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        
        logger.info("Batch sent to Kafka", 
                   topic=topic,
                   total=len(messages),
                   successful=successful_sends)
        
        return successful_sends
    
    def close(self):
        """Close Kafka producer."""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")


# Convenience functions
def start_kafka_streaming(topics: List[str] = None, 
                         bootstrap_servers: str = None) -> Dict[str, Any]:
    """
    Convenience function to start Kafka streaming pipeline.
    
    Args:
        topics: List of Kafka topics
        bootstrap_servers: Kafka bootstrap servers
        
    Returns:
        Dictionary with pipeline status
    """
    processor = KafkaStreamProcessor(
        kafka_bootstrap_servers=bootstrap_servers,
        kafka_topics=topics
    )
    return processor.start_streaming_pipeline()


def send_to_kafka_topic(topic: str, 
                       messages: List[Dict[str, Any]], 
                       bootstrap_servers: str = None) -> int:
    """
    Convenience function to send messages to Kafka topic.
    
    Args:
        topic: Kafka topic name
        messages: List of messages to send
        bootstrap_servers: Kafka bootstrap servers
        
    Returns:
        int: Number of successfully sent messages
    """
    producer = KafkaProducerWrapper(bootstrap_servers)
    if producer.connect():
        result = producer.send_batch(topic, messages)
        producer.close()
        return result
    return 0