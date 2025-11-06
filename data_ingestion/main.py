"""
Data Ingestion Pipeline Main Entry Point

Main script to run the consolidated data ingestion pipeline.
"""

import os
import sys
import signal
import argparse
from typing import Dict, Any
import structlog
from dotenv import load_dotenv

from .orchestrator import get_orchestrator, start_data_ingestion_pipeline, stop_data_ingestion_pipeline
from .logging_config import setup_logging

# Load environment variables
load_dotenv()

logger = structlog.get_logger(__name__)


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Received shutdown signal", signal=signum)
    stop_data_ingestion_pipeline()
    sys.exit(0)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Data Ingestion Pipeline')
    
    parser.add_argument('--mode', 
                       choices=['run', 'test', 'status'], 
                       default='run',
                       help='Operation mode')
    
    parser.add_argument('--config-file', 
                       type=str,
                       help='Path to configuration file')
    
    parser.add_argument('--log-level', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO',
                       help='Logging level')
    
    parser.add_argument('--enable-kafka', 
                       action='store_true',
                       help='Enable Kafka streaming')
    
    parser.add_argument('--enable-apis', 
                       action='store_true',
                       help='Enable API data fetching')
    
    parser.add_argument('--enable-csv', 
                       action='store_true',
                       help='Enable CSV processing')
    
    parser.add_argument('--csv-directory', 
                       type=str,
                       help='CSV data directory path')
    
    parser.add_argument('--api-interval', 
                       type=int,
                       default=60,
                       help='API fetch interval in minutes')
    
    parser.add_argument('--csv-interval', 
                       type=int,
                       default=30,
                       help='CSV processing interval in minutes')
    
    return parser.parse_args()


def load_config_from_file(config_file: str) -> Dict[str, Any]:
    """Load configuration from file."""
    try:
        import yaml
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        logger.info("Configuration loaded from file", file=config_file)
        return config
    except Exception as e:
        logger.error("Failed to load configuration file", file=config_file, error=str(e))
        return {}


def build_config(args) -> Dict[str, Any]:
    """Build configuration from arguments and environment."""
    config = {}
    
    # Load from file if specified
    if args.config_file:
        config.update(load_config_from_file(args.config_file))
    
    # Override with command line arguments
    if args.csv_directory:
        config['csv_data_directory'] = args.csv_directory
    
    if args.enable_kafka:
        config['enable_kafka_streaming'] = True
    
    if args.enable_apis:
        config['enable_api_fetching'] = True
    
    if args.enable_csv:
        config['enable_csv_processing'] = True
    
    config['api_fetch_interval_minutes'] = args.api_interval
    config['csv_processing_interval_minutes'] = args.csv_interval
    
    return config


def run_pipeline(config: Dict[str, Any]):
    """Run the data ingestion pipeline."""
    try:
        logger.info("Starting data ingestion pipeline")
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start the pipeline
        if start_data_ingestion_pipeline(config):
            logger.info("Pipeline started successfully")
            
            # Keep the main thread alive
            try:
                while True:
                    import time
                    time.sleep(60)  # Check every minute
                    
                    # Log status periodically
                    orchestrator = get_orchestrator()
                    status = orchestrator.get_status()
                    logger.info("Pipeline status", 
                               is_running=status['is_running'],
                               total_runs=status['statistics']['total_runs'],
                               successful_runs=status['statistics']['successful_runs'])
                    
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
        else:
            logger.error("Failed to start pipeline")
            sys.exit(1)
            
    except Exception as e:
        logger.error("Pipeline execution failed", error=str(e))
        sys.exit(1)
    finally:
        stop_data_ingestion_pipeline()


def test_pipeline(config: Dict[str, Any]):
    """Test the data ingestion pipeline."""
    try:
        logger.info("Testing data ingestion pipeline")
        
        orchestrator = get_orchestrator(config)
        
        # Test MongoDB connection
        logger.info("Testing MongoDB connection")
        if orchestrator.mongodb_connector.connect():
            logger.info("MongoDB connection successful")
            orchestrator.mongodb_connector.disconnect()
        else:
            logger.error("MongoDB connection failed")
            return False
        
        # Test manual data fetch
        logger.info("Testing manual data fetch")
        results = orchestrator.run_manual_fetch('all')
        
        success_count = 0
        for source, result in results.items():
            if result.get('success', False):
                success_count += 1
                logger.info(f"{source} fetch test successful", 
                           data_points=result.get('data_points_fetched', 0))
            else:
                logger.error(f"{source} fetch test failed", 
                           error=result.get('error'))
        
        if success_count > 0:
            logger.info("Pipeline test completed", 
                       successful_sources=success_count,
                       total_sources=len(results))
            return True
        else:
            logger.error("All pipeline tests failed")
            return False
            
    except Exception as e:
        logger.error("Pipeline test failed", error=str(e))
        return False


def show_status(config: Dict[str, Any]):
    """Show pipeline status."""
    try:
        orchestrator = get_orchestrator(config)
        status = orchestrator.get_status()
        
        print("\n=== Data Ingestion Pipeline Status ===")
        print(f"Running: {status['is_running']}")
        print(f"Total Runs: {status['statistics']['total_runs']}")
        print(f"Successful Runs: {status['statistics']['successful_runs']}")
        print(f"Failed Runs: {status['statistics']['failed_runs']}")
        print(f"Last Run: {status['statistics']['last_run_time']}")
        print(f"Last Success: {status['statistics']['last_success_time']}")
        
        print("\n=== Configuration ===")
        for key, value in status['configuration'].items():
            print(f"{key}: {value}")
        
        print("\n=== Statistics ===")
        stats = status['statistics']
        print(f"CSV Files Processed: {stats['csv_files_processed']}")
        print(f"Weather Data Points: {stats['weather_data_points']}")
        print(f"Stock Data Points: {stats['stock_data_points']}")
        print(f"Kafka Messages Sent: {stats['kafka_messages_sent']}")
        
        if status['next_scheduled_jobs']:
            print("\n=== Scheduled Jobs ===")
            for job in status['next_scheduled_jobs']:
                print(f"{job['job']}: {job['next_run']}")
        
        print("\n" + "="*50)
        
    except Exception as e:
        logger.error("Failed to get status", error=str(e))


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Setup logging
    setup_logging(level=args.log_level)
    
    # Build configuration
    config = build_config(args)
    
    logger.info("Data ingestion pipeline starting", 
               mode=args.mode,
               config_keys=list(config.keys()))
    
    try:
        if args.mode == 'run':
            run_pipeline(config)
        elif args.mode == 'test':
            success = test_pipeline(config)
            sys.exit(0 if success else 1)
        elif args.mode == 'status':
            show_status(config)
        else:
            logger.error("Invalid mode", mode=args.mode)
            sys.exit(1)
            
    except Exception as e:
        logger.error("Main execution failed", error=str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()