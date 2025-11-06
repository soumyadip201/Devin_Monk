"""
Data Ingestion Orchestrator

Main orchestrator that coordinates all data ingestion processes including:
- CSV file processing
- Weather API data fetching
- Stock price API data fetching
- Kafka streaming
- Scheduling and monitoring
"""

import os
import asyncio
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import schedule
import time
import structlog
from concurrent.futures import ThreadPoolExecutor, as_completed

from .csv_ingestion.processor import CSVProcessor, process_csv_directory
from .weather_api.fetcher import WeatherAPIFetcher, fetch_weather_data_sync
from .stock_api.fetcher import StockAPIFetcher, fetch_stock_data_sync
from .kafka_streaming.processor import KafkaStreamProcessor, KafkaProducerWrapper
from .mongodb_connector.connection import get_mongodb_connector

logger = structlog.get_logger(__name__)


class DataIngestionOrchestrator:
    """
    Main orchestrator for the data ingestion pipeline.
    Coordinates all data sources and processing workflows.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the data ingestion orchestrator.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or self._load_default_config()
        self.mongodb_connector = get_mongodb_connector()
        self.is_running = False
        self.scheduler_thread = None
        self.kafka_stream_processor = None
        self.kafka_producer = None
        
        # Initialize components
        self.csv_processor = CSVProcessor()
        self.weather_fetcher = WeatherAPIFetcher()
        self.stock_fetcher = StockAPIFetcher()
        
        # Statistics tracking
        self.stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'last_run_time': None,
            'last_success_time': None,
            'csv_files_processed': 0,
            'weather_data_points': 0,
            'stock_data_points': 0,
            'kafka_messages_sent': 0
        }
        
        logger.info("Data ingestion orchestrator initialized", 
                   config_keys=list(self.config.keys()))
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration."""
        return {
            # Data sources
            'csv_data_directory': os.getenv('CSV_DATA_DIRECTORY', './data'),
            'weather_api_key': os.getenv('WEATHER_API_KEY'),
            'stock_api_key': os.getenv('STOCK_API_KEY'),
            
            # MongoDB settings
            'mongodb_host': os.getenv('MONKDB_HOST', 'localhost'),
            'mongodb_port': int(os.getenv('MONKDB_PORT', '27017')),
            'mongodb_database': os.getenv('MONKDB_DATABASE', 'monkdb'),
            
            # Kafka settings
            'kafka_bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'kafka_topics': ['weather-data', 'stock-data', 'csv-data'],
            
            # Scheduling settings
            'api_fetch_interval_minutes': int(os.getenv('API_FETCH_INTERVAL', '60')),
            'csv_processing_interval_minutes': int(os.getenv('CSV_PROCESSING_INTERVAL', '30')),
            
            # Processing settings
            'enable_kafka_streaming': os.getenv('ENABLE_KAFKA_STREAMING', 'true').lower() == 'true',
            'enable_api_fetching': os.getenv('ENABLE_API_FETCHING', 'true').lower() == 'true',
            'enable_csv_processing': os.getenv('ENABLE_CSV_PROCESSING', 'true').lower() == 'true',
            
            # Stock symbols and weather locations
            'stock_symbols': ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA'],
            'weather_locations': [
                {'name': 'New York', 'lat': 40.7128, 'lon': -74.0060},
                {'name': 'London', 'lat': 51.5074, 'lon': -0.1278},
                {'name': 'Tokyo', 'lat': 35.6762, 'lon': 139.6503}
            ]
        }
    
    def initialize_kafka_components(self) -> bool:
        """
        Initialize Kafka components.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            if self.config.get('enable_kafka_streaming', False):
                # Initialize Kafka stream processor
                self.kafka_stream_processor = KafkaStreamProcessor(
                    kafka_bootstrap_servers=self.config['kafka_bootstrap_servers'],
                    kafka_topics=self.config['kafka_topics']
                )
                
                # Initialize Kafka producer
                self.kafka_producer = KafkaProducerWrapper(
                    bootstrap_servers=self.config['kafka_bootstrap_servers']
                )
                
                if not self.kafka_producer.connect():
                    logger.warning("Failed to connect Kafka producer")
                    return False
                
                logger.info("Kafka components initialized successfully")
                return True
            else:
                logger.info("Kafka streaming disabled in configuration")
                return True
                
        except Exception as e:
            logger.error("Failed to initialize Kafka components", error=str(e))
            return False
    
    def process_csv_files(self) -> Dict[str, Any]:
        """
        Process CSV files from the configured directory.
        
        Returns:
            Dictionary with processing results
        """
        try:
            logger.info("Starting CSV file processing")
            
            data_directory = self.config['csv_data_directory']
            results = process_csv_directory(data_directory, "csv_data")
            
            if results['success']:
                self.stats['csv_files_processed'] += results.get('files_processed', 0)
                
                # Send to Kafka if enabled
                if self.kafka_producer and results.get('records_processed', 0) > 0:
                    kafka_message = {
                        'source': 'csv_processing',
                        'files_processed': results['files_processed'],
                        'records_processed': results['records_processed'],
                        'validation_metrics': results.get('validation_metrics', {}),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    
                    if self.kafka_producer.send_message('csv-data', kafka_message):
                        self.stats['kafka_messages_sent'] += 1
                
                logger.info("CSV processing completed successfully", 
                           files=results['files_processed'],
                           records=results['records_processed'])
            else:
                logger.error("CSV processing failed", error=results.get('error'))
            
            return results
            
        except Exception as e:
            logger.error("CSV processing failed with exception", error=str(e))
            return {'success': False, 'error': str(e)}
    
    def fetch_weather_data(self) -> Dict[str, Any]:
        """
        Fetch weather data from API.
        
        Returns:
            Dictionary with fetch results
        """
        try:
            logger.info("Starting weather data fetch")
            
            locations = self.config.get('weather_locations', [])
            results = fetch_weather_data_sync(
                api_key=self.config.get('weather_api_key'),
                locations=locations
            )
            
            if results['success']:
                self.stats['weather_data_points'] += results.get('data_points_fetched', 0)
                
                # Send to Kafka if enabled
                if self.kafka_producer and results.get('data_points_fetched', 0) > 0:
                    kafka_message = {
                        'source': 'weather_api',
                        'locations_requested': results['locations_requested'],
                        'data_points_fetched': results['data_points_fetched'],
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    
                    if self.kafka_producer.send_message('weather-data', kafka_message):
                        self.stats['kafka_messages_sent'] += 1
                
                logger.info("Weather data fetch completed successfully", 
                           locations=results['locations_requested'],
                           data_points=results['data_points_fetched'])
            else:
                logger.error("Weather data fetch failed", error=results.get('error'))
            
            return results
            
        except Exception as e:
            logger.error("Weather data fetch failed with exception", error=str(e))
            return {'success': False, 'error': str(e)}
    
    def fetch_stock_data(self) -> Dict[str, Any]:
        """
        Fetch stock price data from API.
        
        Returns:
            Dictionary with fetch results
        """
        try:
            logger.info("Starting stock data fetch")
            
            symbols = self.config.get('stock_symbols', [])
            results = fetch_stock_data_sync(
                api_key=self.config.get('stock_api_key'),
                symbols=symbols
            )
            
            if results['success']:
                self.stats['stock_data_points'] += results.get('data_points_fetched', 0)
                
                # Send to Kafka if enabled
                if self.kafka_producer and results.get('data_points_fetched', 0) > 0:
                    kafka_message = {
                        'source': 'stock_api',
                        'symbols_requested': results['symbols_requested'],
                        'data_points_fetched': results['data_points_fetched'],
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    
                    if self.kafka_producer.send_message('stock-data', kafka_message):
                        self.stats['kafka_messages_sent'] += 1
                
                logger.info("Stock data fetch completed successfully", 
                           symbols=results['symbols_requested'],
                           data_points=results['data_points_fetched'])
            else:
                logger.error("Stock data fetch failed", error=results.get('error'))
            
            return results
            
        except Exception as e:
            logger.error("Stock data fetch failed with exception", error=str(e))
            return {'success': False, 'error': str(e)}
    
    def run_api_data_fetch(self):
        """Run API data fetching (weather and stock) in parallel."""
        try:
            logger.info("Starting API data fetch job")
            self.stats['total_runs'] += 1
            self.stats['last_run_time'] = datetime.utcnow()
            
            results = []
            
            # Use ThreadPoolExecutor for parallel execution
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = []
                
                if self.config.get('enable_api_fetching', True):
                    # Submit weather and stock fetch tasks
                    if self.config.get('weather_api_key'):
                        futures.append(executor.submit(self.fetch_weather_data))
                    
                    if self.config.get('stock_api_key'):
                        futures.append(executor.submit(self.fetch_stock_data))
                
                # Collect results
                for future in as_completed(futures):
                    try:
                        result = future.result(timeout=300)  # 5 minute timeout
                        results.append(result)
                    except Exception as e:
                        logger.error("API fetch task failed", error=str(e))
                        results.append({'success': False, 'error': str(e)})
            
            # Check if any fetch was successful
            successful_fetches = sum(1 for r in results if r.get('success', False))
            
            if successful_fetches > 0:
                self.stats['successful_runs'] += 1
                self.stats['last_success_time'] = datetime.utcnow()
                logger.info("API data fetch job completed", 
                           successful_fetches=successful_fetches,
                           total_attempts=len(results))
            else:
                self.stats['failed_runs'] += 1
                logger.error("All API fetch attempts failed")
            
        except Exception as e:
            logger.error("API data fetch job failed", error=str(e))
            self.stats['failed_runs'] += 1
    
    def run_csv_processing(self):
        """Run CSV file processing."""
        try:
            logger.info("Starting CSV processing job")
            
            if self.config.get('enable_csv_processing', True):
                result = self.process_csv_files()
                
                if result.get('success', False):
                    logger.info("CSV processing job completed successfully")
                else:
                    logger.error("CSV processing job failed", error=result.get('error'))
            else:
                logger.info("CSV processing disabled in configuration")
                
        except Exception as e:
            logger.error("CSV processing job failed", error=str(e))
    
    def setup_scheduler(self):
        """Setup scheduled jobs."""
        try:
            # Schedule API data fetching
            api_interval = self.config.get('api_fetch_interval_minutes', 60)
            schedule.every(api_interval).minutes.do(self.run_api_data_fetch)
            
            # Schedule CSV processing
            csv_interval = self.config.get('csv_processing_interval_minutes', 30)
            schedule.every(csv_interval).minutes.do(self.run_csv_processing)
            
            logger.info("Scheduler configured", 
                       api_interval=api_interval,
                       csv_interval=csv_interval)
            
        except Exception as e:
            logger.error("Failed to setup scheduler", error=str(e))
    
    def run_scheduler(self):
        """Run the scheduler in a separate thread."""
        logger.info("Starting scheduler thread")
        
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error("Scheduler error", error=str(e))
                time.sleep(5)  # Wait before retrying
        
        logger.info("Scheduler thread stopped")
    
    def start_kafka_streaming(self) -> bool:
        """
        Start Kafka streaming pipeline.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        if not self.config.get('enable_kafka_streaming', False):
            logger.info("Kafka streaming disabled")
            return True
        
        if not self.kafka_stream_processor:
            logger.error("Kafka stream processor not initialized")
            return False
        
        try:
            results = self.kafka_stream_processor.start_streaming_pipeline()
            
            if results['success']:
                logger.info("Kafka streaming pipeline started", 
                           active_streams=len(results['active_streams']))
                return True
            else:
                logger.error("Failed to start Kafka streaming", 
                           error=results.get('error'))
                return False
                
        except Exception as e:
            logger.error("Kafka streaming startup failed", error=str(e))
            return False
    
    def start(self) -> bool:
        """
        Start the data ingestion orchestrator.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            logger.info("Starting data ingestion orchestrator")
            
            # Initialize Kafka components
            if not self.initialize_kafka_components():
                logger.warning("Kafka initialization failed, continuing without Kafka")
            
            # Setup scheduler
            self.setup_scheduler()
            
            # Start Kafka streaming if enabled
            if not self.start_kafka_streaming():
                logger.warning("Kafka streaming failed to start")
            
            # Start scheduler thread
            self.is_running = True
            self.scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
            self.scheduler_thread.start()
            
            # Run initial data fetch
            logger.info("Running initial data fetch")
            self.run_api_data_fetch()
            self.run_csv_processing()
            
            logger.info("Data ingestion orchestrator started successfully")
            return True
            
        except Exception as e:
            logger.error("Failed to start orchestrator", error=str(e))
            return False
    
    def stop(self):
        """Stop the data ingestion orchestrator."""
        try:
            logger.info("Stopping data ingestion orchestrator")
            
            # Stop scheduler
            self.is_running = False
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=5)
            
            # Close Kafka components
            if self.kafka_producer:
                self.kafka_producer.close()
            
            if self.kafka_stream_processor:
                self.kafka_stream_processor.stop_spark()
            
            # Stop CSV processor
            self.csv_processor.stop_spark()
            
            logger.info("Data ingestion orchestrator stopped")
            
        except Exception as e:
            logger.error("Error stopping orchestrator", error=str(e))
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current status of the orchestrator.
        
        Returns:
            Dictionary with status information
        """
        return {
            'is_running': self.is_running,
            'configuration': {
                'enable_kafka_streaming': self.config.get('enable_kafka_streaming', False),
                'enable_api_fetching': self.config.get('enable_api_fetching', False),
                'enable_csv_processing': self.config.get('enable_csv_processing', False),
                'api_fetch_interval_minutes': self.config.get('api_fetch_interval_minutes', 60),
                'csv_processing_interval_minutes': self.config.get('csv_processing_interval_minutes', 30)
            },
            'statistics': self.stats.copy(),
            'next_scheduled_jobs': [
                {
                    'job': str(job.job_func.__name__),
                    'next_run': job.next_run.isoformat() if job.next_run else None
                }
                for job in schedule.jobs
            ]
        }
    
    def run_manual_fetch(self, source: str = 'all') -> Dict[str, Any]:
        """
        Run manual data fetch for testing.
        
        Args:
            source: Data source ('weather', 'stock', 'csv', or 'all')
            
        Returns:
            Dictionary with results
        """
        results = {}
        
        try:
            if source in ['weather', 'all']:
                results['weather'] = self.fetch_weather_data()
            
            if source in ['stock', 'all']:
                results['stock'] = self.fetch_stock_data()
            
            if source in ['csv', 'all']:
                results['csv'] = self.process_csv_files()
            
            logger.info("Manual fetch completed", source=source)
            return results
            
        except Exception as e:
            logger.error("Manual fetch failed", source=source, error=str(e))
            return {'error': str(e)}


# Global orchestrator instance
_orchestrator = None

def get_orchestrator(config: Dict[str, Any] = None) -> DataIngestionOrchestrator:
    """
    Get singleton orchestrator instance.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        DataIngestionOrchestrator: Singleton orchestrator instance
    """
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = DataIngestionOrchestrator(config)
    return _orchestrator


def start_data_ingestion_pipeline(config: Dict[str, Any] = None) -> bool:
    """
    Convenience function to start the data ingestion pipeline.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        bool: True if started successfully, False otherwise
    """
    orchestrator = get_orchestrator(config)
    return orchestrator.start()


def stop_data_ingestion_pipeline():
    """Convenience function to stop the data ingestion pipeline."""
    global _orchestrator
    if _orchestrator:
        _orchestrator.stop()
        _orchestrator = None