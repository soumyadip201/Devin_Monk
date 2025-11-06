"""
CSV Data Ingestion Module using PySpark

This module handles the ingestion of CSV files from the /data directory,
performs data cleaning and transformation, and prepares data for MongoDB loading.
"""

import os
import logging
from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, trim, 
    current_timestamp, lit, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

from ..utils.logger import get_logger
from ..database.mongodb_client import MongoDBClient


class CSVIngestionProcessor:
    """
    Handles CSV file ingestion using PySpark with data cleaning and transformation.
    """
    
    def __init__(self, spark_session: Optional[SparkSession] = None, 
                 data_directory: str = "/data", mongodb_client: Optional[MongoDBClient] = None):
        """
        Initialize the CSV ingestion processor.
        
        Args:
            spark_session: Optional PySpark session. If None, creates a new one.
            data_directory: Directory containing CSV files to process.
            mongodb_client: MongoDB client for data loading.
        """
        self.logger = get_logger(__name__)
        self.data_directory = data_directory
        self.mongodb_client = mongodb_client
        
        if spark_session is None:
            self.spark = self._create_spark_session()
        else:
            self.spark = spark_session
            
        self.logger.info(f"CSV Ingestion Processor initialized with data directory: {data_directory}")
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session for CSV processing."""
        try:
            spark = SparkSession.builder \
                .appName("CSV_Data_Ingestion") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            self.logger.info("Spark session created successfully")
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            raise
    
    def discover_csv_files(self) -> List[str]:
        """
        Discover all CSV files in the data directory.
        
        Returns:
            List of CSV file paths.
        """
        try:
            csv_files = []
            if os.path.exists(self.data_directory):
                for root, dirs, files in os.walk(self.data_directory):
                    for file in files:
                        if file.lower().endswith('.csv'):
                            csv_files.append(os.path.join(root, file))
            
            self.logger.info(f"Discovered {len(csv_files)} CSV files")
            return csv_files
            
        except Exception as e:
            self.logger.error(f"Error discovering CSV files: {str(e)}")
            return []
    
    def read_csv_file(self, file_path: str, schema: Optional[StructType] = None) -> Optional[DataFrame]:
        """
        Read a CSV file into a Spark DataFrame.
        
        Args:
            file_path: Path to the CSV file.
            schema: Optional schema for the DataFrame.
            
        Returns:
            Spark DataFrame or None if reading fails.
        """
        try:
            df_reader = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true" if schema is None else "false") \
                .option("multiline", "true") \
                .option("escape", '"')
            
            if schema:
                df_reader = df_reader.schema(schema)
            
            df = df_reader.csv(file_path)
            
            self.logger.info(f"Successfully read CSV file: {file_path} with {df.count()} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading CSV file {file_path}: {str(e)}")
            return None
    
    def clean_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Clean the DataFrame by handling missing values, duplicates, and data quality issues.
        
        Args:
            df: Input DataFrame to clean.
            
        Returns:
            Cleaned DataFrame.
        """
        try:
            # Add processing timestamp
            df_cleaned = df.withColumn("processed_at", current_timestamp())
            
            # Handle string columns - trim whitespace and replace empty strings with null
            string_columns = [field.name for field in df.schema.fields if field.dataType == StringType()]
            for col_name in string_columns:
                df_cleaned = df_cleaned.withColumn(
                    col_name, 
                    when(trim(col(col_name)) == "", None).otherwise(trim(col(col_name)))
                )
            
            # Handle numeric columns - replace NaN with null
            numeric_columns = [field.name for field in df.schema.fields 
                             if field.dataType in [IntegerType(), DoubleType()]]
            for col_name in numeric_columns:
                df_cleaned = df_cleaned.withColumn(
                    col_name,
                    when(isnan(col(col_name)), None).otherwise(col(col_name))
                )
            
            # Remove completely duplicate rows
            initial_count = df_cleaned.count()
            df_cleaned = df_cleaned.dropDuplicates()
            final_count = df_cleaned.count()
            
            if initial_count != final_count:
                self.logger.info(f"Removed {initial_count - final_count} duplicate rows")
            
            self.logger.info(f"DataFrame cleaned successfully. Final row count: {final_count}")
            return df_cleaned
            
        except Exception as e:
            self.logger.error(f"Error cleaning DataFrame: {str(e)}")
            return df
    
    def transform_dataframe(self, df: DataFrame, transformations: Optional[Dict[str, Any]] = None) -> DataFrame:
        """
        Apply custom transformations to the DataFrame.
        
        Args:
            df: Input DataFrame to transform.
            transformations: Dictionary of transformation rules.
            
        Returns:
            Transformed DataFrame.
        """
        try:
            df_transformed = df
            
            if transformations:
                # Apply column renaming
                if "rename_columns" in transformations:
                    for old_name, new_name in transformations["rename_columns"].items():
                        if old_name in df_transformed.columns:
                            df_transformed = df_transformed.withColumnRenamed(old_name, new_name)
                
                # Apply data type conversions
                if "cast_columns" in transformations:
                    for col_name, data_type in transformations["cast_columns"].items():
                        if col_name in df_transformed.columns:
                            df_transformed = df_transformed.withColumn(col_name, col(col_name).cast(data_type))
                
                # Apply custom column expressions
                if "add_columns" in transformations:
                    for col_name, expression in transformations["add_columns"].items():
                        df_transformed = df_transformed.withColumn(col_name, expression)
            
            # Add metadata columns
            df_transformed = df_transformed.withColumn("source_type", lit("csv"))
            df_transformed = df_transformed.withColumn("ingestion_timestamp", current_timestamp())
            
            self.logger.info("DataFrame transformation completed successfully")
            return df_transformed
            
        except Exception as e:
            self.logger.error(f"Error transforming DataFrame: {str(e)}")
            return df
    
    def process_csv_file(self, file_path: str, collection_name: Optional[str] = None,
                        transformations: Optional[Dict[str, Any]] = None) -> bool:
        """
        Process a single CSV file: read, clean, transform, and load to MongoDB.
        
        Args:
            file_path: Path to the CSV file.
            collection_name: MongoDB collection name. If None, uses filename.
            transformations: Optional transformation rules.
            
        Returns:
            True if processing successful, False otherwise.
        """
        try:
            # Read CSV file
            df = self.read_csv_file(file_path)
            if df is None:
                return False
            
            # Clean the data
            df_cleaned = self.clean_dataframe(df)
            
            # Transform the data
            df_transformed = self.transform_dataframe(df_cleaned, transformations)
            
            # Load to MongoDB if client is available
            if self.mongodb_client:
                if collection_name is None:
                    collection_name = os.path.splitext(os.path.basename(file_path))[0]
                
                success = self.mongodb_client.load_dataframe(df_transformed, collection_name)
                if success:
                    self.logger.info(f"Successfully processed and loaded CSV file: {file_path}")
                    return True
                else:
                    self.logger.error(f"Failed to load data to MongoDB for file: {file_path}")
                    return False
            else:
                self.logger.warning("No MongoDB client available. Data processed but not loaded.")
                return True
                
        except Exception as e:
            self.logger.error(f"Error processing CSV file {file_path}: {str(e)}")
            return False
    
    def process_all_csv_files(self, transformations: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, bool]:
        """
        Process all CSV files in the data directory.
        
        Args:
            transformations: Dictionary mapping file patterns to transformation rules.
            
        Returns:
            Dictionary mapping file paths to processing success status.
        """
        results = {}
        csv_files = self.discover_csv_files()
        
        if not csv_files:
            self.logger.warning("No CSV files found to process")
            return results
        
        for file_path in csv_files:
            try:
                # Determine transformations for this file
                file_transformations = None
                if transformations:
                    filename = os.path.basename(file_path)
                    for pattern, trans in transformations.items():
                        if pattern in filename:
                            file_transformations = trans
                            break
                
                success = self.process_csv_file(file_path, transformations=file_transformations)
                results[file_path] = success
                
            except Exception as e:
                self.logger.error(f"Unexpected error processing file {file_path}: {str(e)}")
                results[file_path] = False
        
        successful_files = sum(1 for success in results.values() if success)
        self.logger.info(f"Processed {len(csv_files)} CSV files. {successful_files} successful, {len(csv_files) - successful_files} failed.")
        
        return results
    
    def stop(self):
        """Stop the Spark session."""
        try:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped successfully")
        except Exception as e:
            self.logger.error(f"Error stopping Spark session: {str(e)}")


def create_sample_csv_files(data_directory: str = "/data"):
    """
    Create sample CSV files for testing purposes.
    
    Args:
        data_directory: Directory to create sample files in.
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    os.makedirs(data_directory, exist_ok=True)
    
    # Sample users data
    users_data = {
        'user_id': range(1, 101),
        'username': [f'user_{i}' for i in range(1, 101)],
        'email': [f'user_{i}@example.com' for i in range(1, 101)],
        'age': np.random.randint(18, 80, 100),
        'city': np.random.choice(['New York', 'London', 'Tokyo', 'Paris', 'Sydney'], 100),
        'signup_date': [datetime.now() - timedelta(days=np.random.randint(1, 365)) for _ in range(100)]
    }
    
    users_df = pd.DataFrame(users_data)
    users_df.to_csv(os.path.join(data_directory, 'users.csv'), index=False)
    
    # Sample transactions data
    transactions_data = {
        'transaction_id': range(1, 501),
        'user_id': np.random.randint(1, 101, 500),
        'amount': np.round(np.random.uniform(10.0, 1000.0, 500), 2),
        'currency': np.random.choice(['USD', 'EUR', 'GBP', 'JPY'], 500),
        'transaction_date': [datetime.now() - timedelta(days=np.random.randint(1, 30)) for _ in range(500)],
        'status': np.random.choice(['completed', 'pending', 'failed'], 500)
    }
    
    transactions_df = pd.DataFrame(transactions_data)
    transactions_df.to_csv(os.path.join(data_directory, 'transactions.csv'), index=False)
    
    print(f"Sample CSV files created in {data_directory}")


if __name__ == "__main__":
    # Create sample data for testing
    create_sample_csv_files()
    
    # Initialize and run CSV ingestion
    processor = CSVIngestionProcessor()
    results = processor.process_all_csv_files()
    
    print("CSV Processing Results:")
    for file_path, success in results.items():
        print(f"  {file_path}: {'SUCCESS' if success else 'FAILED'}")
    
    processor.stop()