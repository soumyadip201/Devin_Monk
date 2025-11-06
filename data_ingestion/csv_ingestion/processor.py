"""
CSV Ingestion Processor

Handles CSV file ingestion using PySpark with data cleaning and transformation.
"""

import os
import glob
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, trim, regexp_replace, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import structlog

from ..mongodb_connector.connection import get_mongodb_connector

logger = structlog.get_logger(__name__)


class CSVProcessor:
    """
    CSV file processor using PySpark for scalable data processing.
    """
    
    def __init__(self, app_name: str = "CSV_Ingestion_Pipeline"):
        """
        Initialize CSV processor with Spark session.
        
        Args:
            app_name: Name for the Spark application
        """
        self.app_name = app_name
        self.spark = None
        self.mongodb_connector = get_mongodb_connector()
        
        logger.info("CSV processor initialized", app_name=app_name)
    
    def initialize_spark(self) -> bool:
        """
        Initialize Spark session with optimized configuration.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            # Set log level to reduce verbosity
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark session initialized successfully", 
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
    
    def read_csv_files(self, data_directory: str, file_pattern: str = "*.csv") -> Optional[DataFrame]:
        """
        Read CSV files from directory using PySpark.
        
        Args:
            data_directory: Directory containing CSV files
            file_pattern: File pattern to match (default: *.csv)
            
        Returns:
            PySpark DataFrame or None if error
        """
        if not self.spark:
            logger.error("Spark session not initialized")
            return None
        
        try:
            # Find CSV files
            csv_files = glob.glob(os.path.join(data_directory, file_pattern))
            
            if not csv_files:
                logger.warning("No CSV files found", 
                             directory=data_directory, 
                             pattern=file_pattern)
                return None
            
            logger.info("Found CSV files", 
                       count=len(csv_files), 
                       files=csv_files)
            
            # Read CSV files with automatic schema inference
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiline", "true") \
                .option("escape", '"') \
                .csv(csv_files)
            
            # Add metadata columns
            df = df.withColumn("_file_source", regexp_replace(col("_metadata.file_path"), ".*/(.*)", "$1")) \
                   .withColumn("_ingestion_timestamp", current_timestamp())
            
            logger.info("CSV files loaded successfully", 
                       row_count=df.count(), 
                       columns=len(df.columns))
            
            return df
            
        except Exception as e:
            logger.error("Failed to read CSV files", 
                        directory=data_directory, 
                        error=str(e))
            return None
    
    def clean_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Clean and transform DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        try:
            logger.info("Starting data cleaning", 
                       initial_count=df.count())
            
            # Get string columns for cleaning
            string_columns = [field.name for field in df.schema.fields 
                            if isinstance(field.dataType, StringType)]
            
            # Clean string columns
            for col_name in string_columns:
                df = df.withColumn(col_name, 
                                 when(col(col_name).isNull() | 
                                      (trim(col(col_name)) == ""), None)
                                 .otherwise(trim(col(col_name))))
            
            # Handle numeric columns - replace invalid values with null
            numeric_columns = [field.name for field in df.schema.fields 
                             if isinstance(field.dataType, (IntegerType, DoubleType))]
            
            for col_name in numeric_columns:
                df = df.withColumn(col_name, 
                                 when(isnan(col(col_name)) | isnull(col(col_name)), None)
                                 .otherwise(col(col_name)))
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            logger.info("Data cleaning completed", 
                       final_count=df.count())
            
            return df
            
        except Exception as e:
            logger.error("Failed to clean DataFrame", error=str(e))
            return df
    
    def validate_dataframe(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate DataFrame and return quality metrics.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with validation metrics
        """
        try:
            total_rows = df.count()
            total_columns = len(df.columns)
            
            # Calculate null counts for each column
            null_counts = {}
            for col_name in df.columns:
                if col_name.startswith('_'):  # Skip metadata columns
                    continue
                null_count = df.filter(col(col_name).isNull()).count()
                null_counts[col_name] = {
                    'null_count': null_count,
                    'null_percentage': (null_count / total_rows * 100) if total_rows > 0 else 0
                }
            
            # Calculate duplicate rows
            duplicate_count = total_rows - df.dropDuplicates().count()
            
            validation_metrics = {
                'total_rows': total_rows,
                'total_columns': total_columns,
                'duplicate_rows': duplicate_count,
                'null_analysis': null_counts,
                'data_quality_score': self._calculate_quality_score(null_counts, duplicate_count, total_rows)
            }
            
            logger.info("DataFrame validation completed", 
                       metrics=validation_metrics)
            
            return validation_metrics
            
        except Exception as e:
            logger.error("Failed to validate DataFrame", error=str(e))
            return {}
    
    def _calculate_quality_score(self, null_counts: Dict, duplicate_count: int, total_rows: int) -> float:
        """Calculate data quality score (0-100)."""
        if total_rows == 0:
            return 0.0
        
        # Penalize for nulls and duplicates
        avg_null_percentage = sum(col_data['null_percentage'] for col_data in null_counts.values()) / len(null_counts) if null_counts else 0
        duplicate_percentage = (duplicate_count / total_rows) * 100
        
        quality_score = 100 - (avg_null_percentage * 0.5) - (duplicate_percentage * 0.8)
        return max(0.0, min(100.0, quality_score))
    
    def save_to_mongodb(self, df: DataFrame, collection_name: str, batch_size: int = 1000) -> bool:
        """
        Save DataFrame to MongoDB in batches.
        
        Args:
            df: DataFrame to save
            collection_name: MongoDB collection name
            batch_size: Number of records per batch
            
        Returns:
            bool: True if save successful, False otherwise
        """
        try:
            # Connect to MongoDB
            if not self.mongodb_connector.connect():
                return False
            
            # Convert to Pandas for MongoDB insertion
            pandas_df = df.toPandas()
            
            # Convert to list of dictionaries
            records = pandas_df.to_dict('records')
            
            # Insert in batches
            total_records = len(records)
            successful_batches = 0
            
            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                
                if self.mongodb_connector.insert_documents(collection_name, batch):
                    successful_batches += 1
                else:
                    logger.error("Failed to insert batch", 
                               batch_start=i, 
                               batch_size=len(batch))
            
            logger.info("Data saved to MongoDB", 
                       collection=collection_name, 
                       total_records=total_records, 
                       successful_batches=successful_batches)
            
            return successful_batches > 0
            
        except Exception as e:
            logger.error("Failed to save to MongoDB", 
                        collection=collection_name, 
                        error=str(e))
            return False
        finally:
            self.mongodb_connector.disconnect()
    
    def process_csv_files(self, data_directory: str, collection_name: str, 
                         file_pattern: str = "*.csv") -> Dict[str, Any]:
        """
        Complete CSV processing pipeline.
        
        Args:
            data_directory: Directory containing CSV files
            collection_name: MongoDB collection name
            file_pattern: File pattern to match
            
        Returns:
            Dictionary with processing results
        """
        results = {
            'success': False,
            'files_processed': 0,
            'records_processed': 0,
            'validation_metrics': {},
            'error': None
        }
        
        try:
            # Initialize Spark
            if not self.initialize_spark():
                results['error'] = "Failed to initialize Spark session"
                return results
            
            # Read CSV files
            df = self.read_csv_files(data_directory, file_pattern)
            if df is None:
                results['error'] = "No CSV files found or failed to read"
                return results
            
            # Clean data
            df_cleaned = self.clean_dataframe(df)
            
            # Validate data
            validation_metrics = self.validate_dataframe(df_cleaned)
            results['validation_metrics'] = validation_metrics
            results['records_processed'] = validation_metrics.get('total_rows', 0)
            
            # Save to MongoDB
            if self.save_to_mongodb(df_cleaned, collection_name):
                results['success'] = True
                results['files_processed'] = len(glob.glob(os.path.join(data_directory, file_pattern)))
            else:
                results['error'] = "Failed to save to MongoDB"
            
            return results
            
        except Exception as e:
            logger.error("CSV processing pipeline failed", error=str(e))
            results['error'] = str(e)
            return results
        finally:
            self.stop_spark()
    
    def __enter__(self):
        """Context manager entry."""
        self.initialize_spark()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_spark()


def process_csv_directory(data_directory: str, collection_name: str = "csv_data") -> Dict[str, Any]:
    """
    Convenience function to process CSV files in a directory.
    
    Args:
        data_directory: Directory containing CSV files
        collection_name: MongoDB collection name
        
    Returns:
        Dictionary with processing results
    """
    processor = CSVProcessor()
    return processor.process_csv_files(data_directory, collection_name)