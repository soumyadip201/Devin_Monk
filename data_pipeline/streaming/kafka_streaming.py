"""
Kafka Streaming Module using Spark Streaming

This module handles real-time data streaming from Kafka topics,
processes the data, and loads it into MongoDB.
"""

import json
import logging
from typing import Dict, List, Optional, Any, Callable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, when, 
    regexp_replace, trim, split, explode
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.streaming import StreamingQuery

from ..utils.logger import get_logger
from ..database.mongodb_client import MongoDBClient


class KafkaStreamingProcessor:
    """
    Handles Kafka streaming using Spark Structured Streaming with data processing and MongoDB integration.
    """
    
    def __init__(self, spark_session: Optional[SparkSession] = None, 
                 kafka_bootstrap_servers: str = "localhost:9092",
                 mongodb_client: Optional[MongoDBClient] = None):
        """
        Initialize the Kafka streaming processor.
        
        Args:
            spark_session: Optional PySpark session. If None, creates a new one.
            kafka_bootstrap_servers: Kafka bootstrap servers.
            mongodb_client: MongoDB client for data loading.
        """
        self.logger = get_logger(__name__)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.mongodb_client = mongodb_client
        self.active_streams: Dict[str, StreamingQuery] = {}
        
        if spark_session is None:
            self.spark = self._create_spark_session()
        else:
            self.spark = spark_session
            
        self.logger.info(f"Kafka Streaming Processor initialized with servers: {kafka_bootstrap_servers}")
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session for Kafka streaming."""
        try:
            spark = SparkSession.builder \
                .appName("Kafka_Streaming_Pipeline") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            self.logger.info("Spark session created successfully for Kafka streaming")
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            raise
    
    def create_kafka_stream(self, topic: str, starting_offsets: str = "latest") -> DataFrame:
        """
        Create a Kafka streaming DataFrame.
        
        Args:
            topic: Kafka topic name.
            starting_offsets: Starting offset strategy ("earliest" or "latest").
            
        Returns:
            Streaming DataFrame from Kafka.
        """
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", starting_offsets) \
                .option("failOnDataLoss", "false") \
                .load()
            
            self.logger.info(f"Created Kafka stream for topic: {topic}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error creating Kafka stream for topic {topic}: {str(e)}")
            raise
    
    def get_user_events_schema(self) -> StructType:
        """Get schema for user_events topic."""
        return StructType([
            StructField("user_id", IntegerType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_data", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("session_id", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("user_agent", StringType(), True)
        ])
    
    def get_transaction_events_schema(self) -> StructType:
        """Get schema for transaction_events topic."""
        return StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("merchant", StringType(), True),
            StructField("category", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("payment_method", StringType(), True)
        ])
    
    def process_user_events_stream(self, df: DataFrame) -> DataFrame:
        """
        Process user events stream data.
        
        Args:
            df: Raw Kafka streaming DataFrame.
            
        Returns:
            Processed DataFrame.
        """
        try:
            # Parse JSON data from Kafka
            schema = self.get_user_events_schema()
            
            processed_df = df.select(
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("kafka_value"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            ).withColumn(
                "parsed_data", 
                from_json(col("kafka_value"), schema)
            ).select(
                col("kafka_key"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
                col("parsed_data.*")
            )
            
            # Data cleaning and transformation
            processed_df = processed_df \
                .withColumn("processed_at", current_timestamp()) \
                .withColumn("source_type", lit("kafka_user_events")) \
                .withColumn("event_type", trim(col("event_type"))) \
                .withColumn("ip_address", 
                           when(col("ip_address").rlike("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$"), 
                                col("ip_address")).otherwise(None))
            
            # Filter out invalid records
            processed_df = processed_df.filter(
                col("user_id").isNotNull() & 
                col("event_type").isNotNull() &
                col("timestamp").isNotNull()
            )
            
            self.logger.info("User events stream processed successfully")
            return processed_df
            
        except Exception as e:
            self.logger.error(f"Error processing user events stream: {str(e)}")
            return df
    
    def process_transaction_events_stream(self, df: DataFrame) -> DataFrame:
        """
        Process transaction events stream data.
        
        Args:
            df: Raw Kafka streaming DataFrame.
            
        Returns:
            Processed DataFrame.
        """
        try:
            # Parse JSON data from Kafka
            schema = self.get_transaction_events_schema()
            
            processed_df = df.select(
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("kafka_value"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            ).withColumn(
                "parsed_data", 
                from_json(col("kafka_value"), schema)
            ).select(
                col("kafka_key"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
                col("parsed_data.*")
            )
            
            # Data cleaning and transformation
            processed_df = processed_df \
                .withColumn("processed_at", current_timestamp()) \
                .withColumn("source_type", lit("kafka_transaction_events")) \
                .withColumn("currency", trim(col("currency"))) \
                .withColumn("status", trim(col("status"))) \
                .withColumn("amount", 
                           when(col("amount") > 0, col("amount")).otherwise(None))
            
            # Filter out invalid records
            processed_df = processed_df.filter(
                col("transaction_id").isNotNull() & 
                col("user_id").isNotNull() &
                col("amount").isNotNull() &
                col("timestamp").isNotNull()
            )
            
            self.logger.info("Transaction events stream processed successfully")
            return processed_df
            
        except Exception as e:
            self.logger.error(f"Error processing transaction events stream: {str(e)}")
            return df
    
    def write_to_mongodb(self, df: DataFrame, collection_name: str, 
                        checkpoint_location: str) -> StreamingQuery:
        """
        Write streaming DataFrame to MongoDB.
        
        Args:
            df: Processed streaming DataFrame.
            collection_name: MongoDB collection name.
            checkpoint_location: Checkpoint location for streaming.
            
        Returns:
            StreamingQuery object.
        """
        try:
            def write_batch_to_mongodb(batch_df, batch_id):
                """Write each batch to MongoDB."""
                try:
                    if self.mongodb_client and batch_df.count() > 0:
                        success = self.mongodb_client.load_dataframe(batch_df, collection_name)
                        if success:
                            self.logger.info(f"Batch {batch_id} written to MongoDB collection: {collection_name}")
                        else:
                            self.logger.error(f"Failed to write batch {batch_id} to MongoDB")
                    else:
                        self.logger.warning(f"Batch {batch_id} is empty or no MongoDB client available")
                        
                except Exception as e:
                    self.logger.error(f"Error writing batch {batch_id} to MongoDB: {str(e)}")
            
            query = df.writeStream \
                .foreachBatch(write_batch_to_mongodb) \
                .option("checkpointLocation", checkpoint_location) \
                .trigger(processingTime='30 seconds') \
                .start()
            
            self.logger.info(f"Started streaming query for collection: {collection_name}")
            return query
            
        except Exception as e:
            self.logger.error(f"Error setting up MongoDB streaming write: {str(e)}")
            raise
    
    def start_user_events_stream(self, checkpoint_location: str = "/tmp/user-events-checkpoint") -> StreamingQuery:
        """
        Start processing user events stream.
        
        Args:
            checkpoint_location: Checkpoint location for the stream.
            
        Returns:
            StreamingQuery object.
        """
        try:
            # Create Kafka stream
            raw_stream = self.create_kafka_stream("user_events")
            
            # Process the stream
            processed_stream = self.process_user_events_stream(raw_stream)
            
            # Write to MongoDB
            query = self.write_to_mongodb(processed_stream, "user_events", checkpoint_location)
            
            self.active_streams["user_events"] = query
            self.logger.info("User events stream started successfully")
            return query
            
        except Exception as e:
            self.logger.error(f"Error starting user events stream: {str(e)}")
            raise
    
    def start_transaction_events_stream(self, checkpoint_location: str = "/tmp/transaction-events-checkpoint") -> StreamingQuery:
        """
        Start processing transaction events stream.
        
        Args:
            checkpoint_location: Checkpoint location for the stream.
            
        Returns:
            StreamingQuery object.
        """
        try:
            # Create Kafka stream
            raw_stream = self.create_kafka_stream("transaction_events")
            
            # Process the stream
            processed_stream = self.process_transaction_events_stream(raw_stream)
            
            # Write to MongoDB
            query = self.write_to_mongodb(processed_stream, "transaction_events", checkpoint_location)
            
            self.active_streams["transaction_events"] = query
            self.logger.info("Transaction events stream started successfully")
            return query
            
        except Exception as e:
            self.logger.error(f"Error starting transaction events stream: {str(e)}")
            raise
    
    def start_all_streams(self) -> Dict[str, StreamingQuery]:
        """
        Start all configured Kafka streams.
        
        Returns:
            Dictionary of active streaming queries.
        """
        try:
            self.logger.info("Starting all Kafka streams...")
            
            # Start user events stream
            self.start_user_events_stream()
            
            # Start transaction events stream
            self.start_transaction_events_stream()
            
            self.logger.info(f"Started {len(self.active_streams)} Kafka streams successfully")
            return self.active_streams
            
        except Exception as e:
            self.logger.error(f"Error starting Kafka streams: {str(e)}")
            raise
    
    def stop_stream(self, stream_name: str):
        """
        Stop a specific streaming query.
        
        Args:
            stream_name: Name of the stream to stop.
        """
        try:
            if stream_name in self.active_streams:
                query = self.active_streams[stream_name]
                query.stop()
                del self.active_streams[stream_name]
                self.logger.info(f"Stopped stream: {stream_name}")
            else:
                self.logger.warning(f"Stream {stream_name} not found in active streams")
                
        except Exception as e:
            self.logger.error(f"Error stopping stream {stream_name}: {str(e)}")
    
    def stop_all_streams(self):
        """Stop all active streaming queries."""
        try:
            for stream_name in list(self.active_streams.keys()):
                self.stop_stream(stream_name)
            
            self.logger.info("All streams stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error stopping streams: {str(e)}")
    
    def get_stream_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get status of all active streams.
        
        Returns:
            Dictionary with stream status information.
        """
        status = {}
        
        for stream_name, query in self.active_streams.items():
            try:
                status[stream_name] = {
                    "is_active": query.isActive,
                    "id": query.id,
                    "run_id": query.runId,
                    "name": query.name,
                    "last_progress": query.lastProgress
                }
            except Exception as e:
                status[stream_name] = {"error": str(e)}
        
        return status
    
    def wait_for_termination(self, timeout: Optional[int] = None):
        """
        Wait for all streams to terminate.
        
        Args:
            timeout: Optional timeout in seconds.
        """
        try:
            for stream_name, query in self.active_streams.items():
                self.logger.info(f"Waiting for stream {stream_name} to terminate...")
                if timeout:
                    query.awaitTermination(timeout)
                else:
                    query.awaitTermination()
                    
        except Exception as e:
            self.logger.error(f"Error waiting for stream termination: {str(e)}")
    
    def stop(self):
        """Stop all streams and Spark session."""
        try:
            self.stop_all_streams()
            
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped successfully")
                
        except Exception as e:
            self.logger.error(f"Error stopping Kafka streaming processor: {str(e)}")


def create_sample_kafka_messages():
    """
    Create sample Kafka messages for testing purposes.
    This would typically be done by a separate Kafka producer.
    """
    import json
    from datetime import datetime
    
    # Sample user events
    user_events = [
        {
            "user_id": 1,
            "event_type": "login",
            "event_data": json.dumps({"device": "mobile", "location": "New York"}),
            "timestamp": datetime.now().isoformat(),
            "session_id": "sess_123",
            "ip_address": "192.168.1.1",
            "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)"
        },
        {
            "user_id": 2,
            "event_type": "page_view",
            "event_data": json.dumps({"page": "/dashboard", "duration": 45}),
            "timestamp": datetime.now().isoformat(),
            "session_id": "sess_456",
            "ip_address": "192.168.1.2",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    ]
    
    # Sample transaction events
    transaction_events = [
        {
            "transaction_id": "txn_001",
            "user_id": 1,
            "amount": 99.99,
            "currency": "USD",
            "merchant": "Amazon",
            "category": "Shopping",
            "timestamp": datetime.now().isoformat(),
            "status": "completed",
            "payment_method": "credit_card"
        },
        {
            "transaction_id": "txn_002",
            "user_id": 2,
            "amount": 25.50,
            "currency": "USD",
            "merchant": "Starbucks",
            "category": "Food",
            "timestamp": datetime.now().isoformat(),
            "status": "completed",
            "payment_method": "debit_card"
        }
    ]
    
    print("Sample Kafka messages:")
    print("User Events:", json.dumps(user_events, indent=2))
    print("Transaction Events:", json.dumps(transaction_events, indent=2))
    
    return user_events, transaction_events


if __name__ == "__main__":
    # Create sample messages for reference
    create_sample_kafka_messages()
    
    # Initialize Kafka streaming processor
    processor = KafkaStreamingProcessor()
    
    try:
        # Start all streams
        streams = processor.start_all_streams()
        
        # Print stream status
        status = processor.get_stream_status()
        print("Stream Status:", json.dumps(status, indent=2, default=str))
        
        # Wait for termination (in production, this would run indefinitely)
        # processor.wait_for_termination(timeout=60)
        
    except KeyboardInterrupt:
        print("Stopping streams...")
    finally:
        processor.stop()