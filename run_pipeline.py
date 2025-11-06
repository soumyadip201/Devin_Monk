#!/usr/bin/env python3
"""
Data Pipeline Runner

Main entry point for running the consolidated data ingestion pipeline.
"""

import os
import sys
import argparse
import json
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from data_pipeline.orchestrator.pipeline_orchestrator import DataPipelineOrchestrator, load_config_from_file
from data_pipeline.utils.logger import setup_pipeline_logging


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Data Ingestion Pipeline")
    
    parser.add_argument(
        "--config", 
        type=str, 
        default="data_pipeline/config/pipeline_config.json",
        help="Path to pipeline configuration file"
    )
    
    parser.add_argument(
        "--csv-only", 
        action="store_true",
        help="Run only CSV ingestion"
    )
    
    parser.add_argument(
        "--kafka-only", 
        action="store_true",
        help="Run only Kafka streaming"
    )
    
    parser.add_argument(
        "--api-only", 
        action="store_true",
        help="Run only API data fetching"
    )
    
    parser.add_argument(
        "--log-level", 
        type=str, 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level"
    )
    
    parser.add_argument(
        "--create-sample-data", 
        action="store_true",
        help="Create sample CSV files for testing"
    )
    
    parser.add_argument(
        "--status", 
        action="store_true",
        help="Show pipeline status and exit"
    )
    
    return parser.parse_args()


def create_sample_data():
    """Create sample data for testing."""
    from data_pipeline.ingestion.csv_ingestion import create_sample_csv_files
    
    print("Creating sample CSV files...")
    create_sample_csv_files("data")
    print("Sample CSV files created in 'data' directory")


def show_status(orchestrator):
    """Show pipeline status."""
    try:
        status = orchestrator.get_pipeline_status()
        print("\n=== Data Pipeline Status ===")
        print(json.dumps(status, indent=2, default=str))
    except Exception as e:
        print(f"Error getting status: {str(e)}")


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Set up logging
    setup_pipeline_logging(log_level=args.log_level)
    
    # Create sample data if requested
    if args.create_sample_data:
        create_sample_data()
        return
    
    # Load configuration
    config = None
    if os.path.exists(args.config):
        config = load_config_from_file(args.config)
        print(f"Loaded configuration from: {args.config}")
    else:
        print(f"Configuration file not found: {args.config}")
        print("Using default configuration")
    
    # Create orchestrator
    orchestrator = DataPipelineOrchestrator(config)
    
    # Show status if requested
    if args.status:
        show_status(orchestrator)
        return
    
    # Determine what to run
    run_csv = not (args.kafka_only or args.api_only)
    run_kafka = not (args.csv_only or args.api_only)
    run_api = not (args.csv_only or args.kafka_only)
    
    if args.csv_only:
        run_csv, run_kafka, run_api = True, False, False
    elif args.kafka_only:
        run_csv, run_kafka, run_api = False, True, False
    elif args.api_only:
        run_csv, run_kafka, run_api = False, False, True
    
    print(f"Running pipeline with: CSV={run_csv}, Kafka={run_kafka}, API={run_api}")
    
    try:
        # Run the pipeline
        orchestrator.run_pipeline(
            run_csv=run_csv,
            run_kafka=run_kafka,
            run_api=run_api
        )
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)
    finally:
        orchestrator.shutdown()


if __name__ == "__main__":
    main()