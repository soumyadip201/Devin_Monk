"""
Data Pipeline Orchestrator

Main orchestrator that coordinates all data ingestion pipeline components:
- CSV file ingestion using PySpark
- Kafka streaming for real-time data
- API data fetching for weather and stock data
- MongoDB data loading and management
"""

import os
import sys
import json
import time
import signal
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession

from ..utils.logger import get_logger, setup_pipeline_logging, PipelineLogger
from ..database.mongodb_client import MongoDBClient, setup_default_collections
from ..ingestion.csv_ingestion import CSVIngestionProcessor
from ..streaming.kafka_streaming import KafkaStreamingProcessor
from ..api_fetcher.api_fetcher import APIDataFetcher


class DataPipelineOrchestrator:
    """
    Main orchestrator for the data ingestion pipeline.
    Coordinates CSV ingestion, Kafka streaming, and API data fetching.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the data pipeline orchestrator.
        
        Args:
            config: Configuration dictionary for the pipeline.
        """
        # Set up logging first
        setup_pipeline_logging()
        self.logger = get_logger(__name__)
        
        # Load configuration
        self.config = config or self._load_default_config()
        
        # Initialize components
        self.mongodb_client = None
        self.spark_session = None
        self.csv_processor = None
        self.kafka_processor = None
        self.api_fetcher = None
        
        # Pipeline state
        self.is_running = False
        self.components_status = {
            "mongodb": False,
            "spark": False,
            "csv_processor": False,
            "kafka_processor": False,
            "api_fetcher": False
        }
        
        # Threading
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.shutdown_event = threading.Event()
        
        self.logger.info("Data Pipeline Orchestrator initialized")
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration for the pipeline."""
        return {
            "mongodb": {
                "connection_string": "mongodb://localhost:27017/",
                "database_name": "data_pipeline",
                "connection_timeout": 5000
            },
            "spark": {
                "app_name": "DataIngestionPipeline",
                "master": "local[*]",
                "config": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                }
            },
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "topics": ["user_events", "transaction_events"],
                "checkpoint_base_dir": "/tmp/kafka-checkpoints"
            },
            "api": {
                "weather": {
                    "api_key": None,
                    "cities": ["New York", "London", "Tokyo", "Paris", "Sydney"]
                },
                "stock": {
                    "api_key": None,
                    "symbols": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
                },
                "fetch_interval_hours": 1
            },
            "csv": {
                "data_directory": "/data",
                "batch_processing": True,
                "auto_discover": True
            },
            "logging": {
                "level": "INFO",
                "directory": "logs"
            }
        }
    
    def initialize_mongodb(self) -> bool:
        """
        Initialize MongoDB connection and set up collections.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            with PipelineLogger(self.logger, "MongoDB initialization"):
                mongodb_config = self.config.get("mongodb", {})
                
                self.mongodb_client = MongoDBClient(
                    connection_string=mongodb_config.get("connection_string", "mongodb://localhost:27017/"),
                    database_name=mongodb_config.get("database_name", "data_pipeline"),
                    connection_timeout=mongodb_config.get("connection_timeout", 5000)
                )
                
                if self.mongodb_client.is_connected():
                    # Set up default collections
                    setup_default_collections(self.mongodb_client)
                    self.components_status["mongodb"] = True
                    self.logger.info("MongoDB initialized successfully")
                    return True
                else:
                    self.logger.error("Failed to connect to MongoDB")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error initializing MongoDB: {str(e)}")
            return False
    
    def initialize_spark(self) -> bool:
        """
        Initialize Spark session.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            with PipelineLogger(self.logger, "Spark initialization"):
                spark_config = self.config.get("spark", {})
                
                builder = SparkSession.builder \
                    .appName(spark_config.get("app_name", "DataIngestionPipeline")) \
                    .master(spark_config.get("master", "local[*]"))
                
                # Add configuration options
                for key, value in spark_config.get("config", {}).items():
                    builder = builder.config(key, value)
                
                # Add Kafka package for streaming
                builder = builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
                
                self.spark_session = builder.getOrCreate()
                self.spark_session.sparkContext.setLogLevel("WARN")
                
                self.components_status["spark"] = True
                self.logger.info("Spark session initialized successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Error initializing Spark: {str(e)}")
            return False
    
    def initialize_csv_processor(self) -> bool:
        """
        Initialize CSV ingestion processor.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            with PipelineLogger(self.logger, "CSV processor initialization"):
                csv_config = self.config.get("csv", {})
                
                self.csv_processor = CSVIngestionProcessor(
                    spark_session=self.spark_session,
                    data_directory=csv_config.get("data_directory", "/data"),
                    mongodb_client=self.mongodb_client
                )
                
                self.components_status["csv_processor"] = True
                self.logger.info("CSV processor initialized successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Error initializing CSV processor: {str(e)}")
            return False
    
    def initialize_kafka_processor(self) -> bool:
        """
        Initialize Kafka streaming processor.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            with PipelineLogger(self.logger, "Kafka processor initialization"):
                kafka_config = self.config.get("kafka", {})
                
                self.kafka_processor = KafkaStreamingProcessor(
                    spark_session=self.spark_session,
                    kafka_bootstrap_servers=kafka_config.get("bootstrap_servers", "localhost:9092"),
                    mongodb_client=self.mongodb_client
                )
                
                self.components_status["kafka_processor"] = True
                self.logger.info("Kafka processor initialized successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Error initializing Kafka processor: {str(e)}")
            return False
    
    def initialize_api_fetcher(self) -> bool:
        """
        Initialize API data fetcher.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            with PipelineLogger(self.logger, "API fetcher initialization"):
                api_config = self.config.get("api", {})
                
                self.api_fetcher = APIDataFetcher(
                    mongodb_client=self.mongodb_client,
                    max_workers=4,
                    request_timeout=30
                )
                
                # Set API keys if provided
                weather_key = api_config.get("weather", {}).get("api_key")
                if weather_key:
                    self.api_fetcher.set_api_key("weather", weather_key)
                
                stock_key = api_config.get("stock", {}).get("api_key")
                if stock_key:
                    self.api_fetcher.set_api_key("stock", stock_key)
                
                self.components_status["api_fetcher"] = True
                self.logger.info("API fetcher initialized successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Error initializing API fetcher: {str(e)}")
            return False
    
    def initialize_all_components(self) -> bool:
        """
        Initialize all pipeline components.
        
        Returns:
            True if all components initialized successfully, False otherwise.
        """
        try:
            with PipelineLogger(self.logger, "Pipeline components initialization"):
                
                # Initialize in order of dependency
                success = True
                
                # MongoDB first (required by other components)
                if not self.initialize_mongodb():
                    success = False
                
                # Spark session (required by CSV and Kafka processors)
                if not self.initialize_spark():
                    success = False
                
                # CSV processor
                if not self.initialize_csv_processor():
                    success = False
                
                # Kafka processor
                if not self.initialize_kafka_processor():
                    success = False
                
                # API fetcher
                if not self.initialize_api_fetcher():
                    success = False
                
                if success:
                    self.logger.info("All pipeline components initialized successfully")
                else:
                    self.logger.error("Some pipeline components failed to initialize")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Error initializing pipeline components: {str(e)}")
            return False
    
    def run_csv_ingestion(self) -> bool:
        """
        Run CSV file ingestion process.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            if not self.csv_processor:
                self.logger.error("CSV processor not initialized")
                return False
            
            with PipelineLogger(self.logger, "CSV ingestion"):
                results = self.csv_processor.process_all_csv_files()
                
                successful_files = sum(1 for success in results.values() if success)
                total_files = len(results)
                
                self.logger.info(f"CSV ingestion completed: {successful_files}/{total_files} files processed successfully")
                
                return successful_files > 0
                
        except Exception as e:
            self.logger.error(f"Error in CSV ingestion: {str(e)}")
            return False
    
    def start_kafka_streaming(self) -> bool:
        """
        Start Kafka streaming processes.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            if not self.kafka_processor:
                self.logger.error("Kafka processor not initialized")
                return False
            
            with PipelineLogger(self.logger, "Kafka streaming startup"):
                streams = self.kafka_processor.start_all_streams()
                
                if streams:
                    self.logger.info(f"Started {len(streams)} Kafka streams successfully")
                    return True
                else:
                    self.logger.error("Failed to start Kafka streams")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error starting Kafka streaming: {str(e)}")
            return False
    
    def start_api_fetching(self) -> bool:
        """
        Start API data fetching processes.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            if not self.api_fetcher:
                self.logger.error("API fetcher not initialized")
                return False
            
            with PipelineLogger(self.logger, "API fetching startup"):
                self.api_fetcher.start()
                
                self.logger.info("API data fetching started successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Error starting API fetching: {str(e)}")
            return False
    
    def run_pipeline(self, run_csv: bool = True, run_kafka: bool = True, run_api: bool = True):
        """
        Run the complete data pipeline.
        
        Args:
            run_csv: Whether to run CSV ingestion.
            run_kafka: Whether to run Kafka streaming.
            run_api: Whether to run API data fetching.
        """
        try:
            with PipelineLogger(self.logger, "Data pipeline execution"):
                
                # Initialize all components
                if not self.initialize_all_components():
                    self.logger.error("Failed to initialize pipeline components")
                    return
                
                self.is_running = True
                
                # Submit tasks to thread pool
                futures = []
                
                # CSV ingestion (one-time or batch processing)
                if run_csv:
                    future = self.executor.submit(self.run_csv_ingestion)
                    futures.append(("csv_ingestion", future))
                
                # Kafka streaming (continuous)
                if run_kafka:
                    future = self.executor.submit(self.start_kafka_streaming)
                    futures.append(("kafka_streaming", future))
                
                # API data fetching (scheduled)
                if run_api:
                    future = self.executor.submit(self.start_api_fetching)
                    futures.append(("api_fetching", future))
                
                # Wait for initial startup tasks to complete
                for task_name, future in futures:
                    try:
                        result = future.result(timeout=60)  # 1 minute timeout for startup
                        self.logger.info(f"Task {task_name} startup: {'SUCCESS' if result else 'FAILED'}")
                    except Exception as e:
                        self.logger.error(f"Task {task_name} startup failed: {str(e)}")
                
                # Keep the pipeline running
                self.logger.info("Data pipeline is running. Press Ctrl+C to stop.")
                
                try:
                    while self.is_running and not self.shutdown_event.is_set():
                        # Print status every 5 minutes
                        self.print_pipeline_status()
                        
                        # Wait for shutdown signal or timeout
                        if self.shutdown_event.wait(timeout=300):  # 5 minutes
                            break
                            
                except KeyboardInterrupt:
                    self.logger.info("Received shutdown signal")
                    self.shutdown()
                
        except Exception as e:
            self.logger.error(f"Error running pipeline: {str(e)}")
        finally:
            self.shutdown()
    
    def print_pipeline_status(self):
        """Print current pipeline status."""
        try:
            status = self.get_pipeline_status()
            self.logger.info("=== Pipeline Status ===")
            self.logger.info(f"Running: {status['is_running']}")
            self.logger.info(f"Components: {status['components_status']}")
            
            if self.mongodb_client:
                collections = self.mongodb_client.list_collections()
                self.logger.info(f"MongoDB Collections: {collections}")
                
                for collection in collections:
                    count = self.mongodb_client.count_documents(collection)
                    self.logger.info(f"  {collection}: {count} documents")
            
            if self.kafka_processor:
                kafka_status = self.kafka_processor.get_stream_status()
                self.logger.info(f"Kafka Streams: {len(kafka_status)} active")
            
            if self.api_fetcher:
                api_status = self.api_fetcher.get_status()
                self.logger.info(f"API Fetcher: {api_status}")
            
        except Exception as e:
            self.logger.error(f"Error getting pipeline status: {str(e)}")
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status.
        
        Returns:
            Dictionary with pipeline status information.
        """
        status = {
            "is_running": self.is_running,
            "components_status": self.components_status.copy(),
            "timestamp": datetime.now().isoformat()
        }
        
        # Add component-specific status
        if self.mongodb_client:
            status["mongodb_connected"] = self.mongodb_client.is_connected()
            status["collections"] = self.mongodb_client.list_collections()
        
        if self.kafka_processor:
            status["kafka_streams"] = self.kafka_processor.get_stream_status()
        
        if self.api_fetcher:
            status["api_fetcher"] = self.api_fetcher.get_status()
        
        return status
    
    def shutdown(self):
        """Shutdown the pipeline gracefully."""
        try:
            with PipelineLogger(self.logger, "Pipeline shutdown"):
                self.is_running = False
                self.shutdown_event.set()
                
                # Stop API fetcher
                if self.api_fetcher:
                    self.api_fetcher.stop()
                
                # Stop Kafka streaming
                if self.kafka_processor:
                    self.kafka_processor.stop_all_streams()
                
                # Stop CSV processor
                if self.csv_processor:
                    self.csv_processor.stop()
                
                # Stop Spark session
                if self.spark_session:
                    self.spark_session.stop()
                
                # Close MongoDB connection
                if self.mongodb_client:
                    self.mongodb_client.close()
                
                # Shutdown thread pool
                self.executor.shutdown(wait=True, timeout=30)
                
                self.logger.info("Pipeline shutdown completed")
                
        except Exception as e:
            self.logger.error(f"Error during pipeline shutdown: {str(e)}")


def load_config_from_file(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        config_file: Path to the configuration file.
        
    Returns:
        Configuration dictionary.
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"Error loading config file {config_file}: {str(e)}")
        return {}


def signal_handler(signum, frame, orchestrator: DataPipelineOrchestrator):
    """Handle shutdown signals."""
    print(f"\nReceived signal {signum}. Shutting down gracefully...")
    orchestrator.shutdown()
    sys.exit(0)


def main():
    """Main entry point for the data pipeline."""
    
    # Load configuration
    config_file = os.environ.get("PIPELINE_CONFIG", "config/pipeline_config.json")
    config = load_config_from_file(config_file) if os.path.exists(config_file) else None
    
    # Create orchestrator
    orchestrator = DataPipelineOrchestrator(config)
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, orchestrator))
    signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, orchestrator))
    
    try:
        # Run the pipeline
        orchestrator.run_pipeline()
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()