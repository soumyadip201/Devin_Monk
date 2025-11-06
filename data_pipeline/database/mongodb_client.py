"""
MongoDB Client Module

This module handles MongoDB connections and data loading operations
for the data ingestion pipeline.
"""

import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import pandas as pd
from pymongo import MongoClient, errors
from pymongo.collection import Collection
from pymongo.database import Database
from pyspark.sql import DataFrame as SparkDataFrame

from ..utils.logger import get_logger


class MongoDBClient:
    """
    MongoDB client for handling database operations in the data ingestion pipeline.
    """
    
    def __init__(self, connection_string: str = "mongodb://localhost:27017/", 
                 database_name: str = "data_pipeline", 
                 connection_timeout: int = 5000,
                 server_selection_timeout: int = 5000):
        """
        Initialize MongoDB client.
        
        Args:
            connection_string: MongoDB connection string.
            database_name: Name of the database to use.
            connection_timeout: Connection timeout in milliseconds.
            server_selection_timeout: Server selection timeout in milliseconds.
        """
        self.logger = get_logger(__name__)
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.database = None
        
        self.connection_config = {
            "connectTimeoutMS": connection_timeout,
            "serverSelectionTimeoutMS": server_selection_timeout,
            "socketTimeoutMS": connection_timeout,
            "maxPoolSize": 50,
            "minPoolSize": 5,
            "maxIdleTimeMS": 30000,
            "waitQueueTimeoutMS": 5000
        }
        
        self._connect()
    
    def _connect(self):
        """Establish connection to MongoDB."""
        try:
            self.client = MongoClient(self.connection_string, **self.connection_config)
            
            # Test the connection
            self.client.admin.command('ping')
            
            self.database = self.client[self.database_name]
            self.logger.info(f"Successfully connected to MongoDB database: {self.database_name}")
            
        except errors.ServerSelectionTimeoutError as e:
            self.logger.error(f"Failed to connect to MongoDB: Server selection timeout - {str(e)}")
            self.client = None
            self.database = None
        except errors.ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: Connection failure - {str(e)}")
            self.client = None
            self.database = None
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to MongoDB: {str(e)}")
            self.client = None
            self.database = None
    
    def is_connected(self) -> bool:
        """
        Check if the client is connected to MongoDB.
        
        Returns:
            True if connected, False otherwise.
        """
        try:
            if self.client is None:
                return False
            
            self.client.admin.command('ping')
            return True
            
        except Exception:
            return False
    
    def reconnect(self):
        """Attempt to reconnect to MongoDB."""
        try:
            if self.client:
                self.client.close()
            
            self._connect()
            
        except Exception as e:
            self.logger.error(f"Error during reconnection: {str(e)}")
    
    def get_collection(self, collection_name: str) -> Optional[Collection]:
        """
        Get a MongoDB collection.
        
        Args:
            collection_name: Name of the collection.
            
        Returns:
            MongoDB collection object or None if not connected.
        """
        if not self.is_connected():
            self.logger.error("Not connected to MongoDB")
            return None
        
        return self.database[collection_name]
    
    def create_indexes(self, collection_name: str, indexes: List[Dict[str, Any]]):
        """
        Create indexes on a collection.
        
        Args:
            collection_name: Name of the collection.
            indexes: List of index specifications.
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return False
            
            for index_spec in indexes:
                collection.create_index(
                    index_spec.get("keys"),
                    **{k: v for k, v in index_spec.items() if k != "keys"}
                )
            
            self.logger.info(f"Created {len(indexes)} indexes on collection: {collection_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating indexes on {collection_name}: {str(e)}")
            return False
    
    def load_documents(self, documents: List[Dict[str, Any]], collection_name: str, 
                      batch_size: int = 1000) -> bool:
        """
        Load documents into MongoDB collection.
        
        Args:
            documents: List of documents to insert.
            collection_name: Name of the target collection.
            batch_size: Number of documents to insert in each batch.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            if not documents:
                self.logger.warning("No documents to load")
                return True
            
            collection = self.get_collection(collection_name)
            if collection is None:
                return False
            
            # Add metadata to documents
            for doc in documents:
                doc["_inserted_at"] = datetime.now()
                if "_id" not in doc:
                    # Let MongoDB generate the _id
                    pass
            
            # Insert documents in batches
            total_inserted = 0
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                
                try:
                    result = collection.insert_many(batch, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    
                except errors.BulkWriteError as e:
                    # Handle partial failures
                    total_inserted += e.details.get("nInserted", 0)
                    self.logger.warning(f"Bulk write error in batch {i//batch_size + 1}: {len(e.details.get('writeErrors', []))} errors")
            
            self.logger.info(f"Successfully loaded {total_inserted}/{len(documents)} documents to {collection_name}")
            return total_inserted > 0
            
        except Exception as e:
            self.logger.error(f"Error loading documents to {collection_name}: {str(e)}")
            return False
    
    def load_dataframe_from_pandas(self, df: pd.DataFrame, collection_name: str, 
                                  batch_size: int = 1000) -> bool:
        """
        Load pandas DataFrame into MongoDB collection.
        
        Args:
            df: Pandas DataFrame to load.
            collection_name: Name of the target collection.
            batch_size: Number of documents to insert in each batch.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            if df.empty:
                self.logger.warning("DataFrame is empty")
                return True
            
            # Convert DataFrame to list of dictionaries
            documents = df.to_dict('records')
            
            # Handle datetime columns
            for doc in documents:
                for key, value in doc.items():
                    if pd.isna(value):
                        doc[key] = None
                    elif isinstance(value, pd.Timestamp):
                        doc[key] = value.to_pydatetime()
            
            return self.load_documents(documents, collection_name, batch_size)
            
        except Exception as e:
            self.logger.error(f"Error loading pandas DataFrame to {collection_name}: {str(e)}")
            return False
    
    def load_dataframe(self, df: SparkDataFrame, collection_name: str, 
                      batch_size: int = 1000) -> bool:
        """
        Load Spark DataFrame into MongoDB collection.
        
        Args:
            df: Spark DataFrame to load.
            collection_name: Name of the target collection.
            batch_size: Number of documents to insert in each batch.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            if df.count() == 0:
                self.logger.warning("Spark DataFrame is empty")
                return True
            
            # Convert Spark DataFrame to pandas DataFrame
            pandas_df = df.toPandas()
            
            return self.load_dataframe_from_pandas(pandas_df, collection_name, batch_size)
            
        except Exception as e:
            self.logger.error(f"Error loading Spark DataFrame to {collection_name}: {str(e)}")
            return False
    
    def query_documents(self, collection_name: str, query: Dict[str, Any] = None, 
                       projection: Dict[str, Any] = None, limit: int = None) -> List[Dict[str, Any]]:
        """
        Query documents from a collection.
        
        Args:
            collection_name: Name of the collection to query.
            query: MongoDB query filter.
            projection: Fields to include/exclude.
            limit: Maximum number of documents to return.
            
        Returns:
            List of matching documents.
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return []
            
            cursor = collection.find(query or {}, projection)
            
            if limit:
                cursor = cursor.limit(limit)
            
            documents = list(cursor)
            
            # Convert ObjectId to string for JSON serialization
            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
            
            self.logger.info(f"Retrieved {len(documents)} documents from {collection_name}")
            return documents
            
        except Exception as e:
            self.logger.error(f"Error querying {collection_name}: {str(e)}")
            return []
    
    def count_documents(self, collection_name: str, query: Dict[str, Any] = None) -> int:
        """
        Count documents in a collection.
        
        Args:
            collection_name: Name of the collection.
            query: MongoDB query filter.
            
        Returns:
            Number of matching documents.
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return 0
            
            count = collection.count_documents(query or {})
            return count
            
        except Exception as e:
            self.logger.error(f"Error counting documents in {collection_name}: {str(e)}")
            return 0
    
    def delete_documents(self, collection_name: str, query: Dict[str, Any]) -> int:
        """
        Delete documents from a collection.
        
        Args:
            collection_name: Name of the collection.
            query: MongoDB query filter for documents to delete.
            
        Returns:
            Number of deleted documents.
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return 0
            
            result = collection.delete_many(query)
            deleted_count = result.deleted_count
            
            self.logger.info(f"Deleted {deleted_count} documents from {collection_name}")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Error deleting documents from {collection_name}: {str(e)}")
            return 0
    
    def create_collection_with_schema(self, collection_name: str, 
                                    schema: Dict[str, Any], indexes: List[Dict[str, Any]] = None):
        """
        Create a collection with schema validation and indexes.
        
        Args:
            collection_name: Name of the collection to create.
            schema: JSON schema for document validation.
            indexes: List of index specifications.
        """
        try:
            # Create collection with schema validation
            self.database.create_collection(
                collection_name,
                validator={"$jsonSchema": schema}
            )
            
            self.logger.info(f"Created collection {collection_name} with schema validation")
            
            # Create indexes if provided
            if indexes:
                self.create_indexes(collection_name, indexes)
            
        except errors.CollectionInvalid:
            self.logger.info(f"Collection {collection_name} already exists")
            
            # Create indexes if provided
            if indexes:
                self.create_indexes(collection_name, indexes)
                
        except Exception as e:
            self.logger.error(f"Error creating collection {collection_name}: {str(e)}")
    
    def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """
        Get statistics for a collection.
        
        Args:
            collection_name: Name of the collection.
            
        Returns:
            Collection statistics.
        """
        try:
            collection = self.get_collection(collection_name)
            if collection is None:
                return {}
            
            stats = self.database.command("collStats", collection_name)
            
            return {
                "document_count": stats.get("count", 0),
                "size_bytes": stats.get("size", 0),
                "average_document_size": stats.get("avgObjSize", 0),
                "storage_size": stats.get("storageSize", 0),
                "indexes": stats.get("nindexes", 0),
                "total_index_size": stats.get("totalIndexSize", 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting stats for {collection_name}: {str(e)}")
            return {}
    
    def list_collections(self) -> List[str]:
        """
        List all collections in the database.
        
        Returns:
            List of collection names.
        """
        try:
            if not self.is_connected():
                return []
            
            collections = self.database.list_collection_names()
            return collections
            
        except Exception as e:
            self.logger.error(f"Error listing collections: {str(e)}")
            return []
    
    def close(self):
        """Close the MongoDB connection."""
        try:
            if self.client:
                self.client.close()
                self.logger.info("MongoDB connection closed")
                
        except Exception as e:
            self.logger.error(f"Error closing MongoDB connection: {str(e)}")


def setup_default_collections(mongodb_client: MongoDBClient):
    """
    Set up default collections with schemas and indexes for the data pipeline.
    
    Args:
        mongodb_client: MongoDB client instance.
    """
    
    # User events collection
    user_events_schema = {
        "bsonType": "object",
        "required": ["user_id", "event_type", "timestamp"],
        "properties": {
            "user_id": {"bsonType": "int"},
            "event_type": {"bsonType": "string"},
            "event_data": {"bsonType": "string"},
            "timestamp": {"bsonType": "date"},
            "session_id": {"bsonType": "string"},
            "ip_address": {"bsonType": "string"},
            "user_agent": {"bsonType": "string"},
            "processed_at": {"bsonType": "date"},
            "source_type": {"bsonType": "string"}
        }
    }
    
    user_events_indexes = [
        {"keys": [("user_id", 1), ("timestamp", -1)]},
        {"keys": [("event_type", 1)]},
        {"keys": [("timestamp", -1)]},
        {"keys": [("session_id", 1)]}
    ]
    
    # Transaction events collection
    transaction_events_schema = {
        "bsonType": "object",
        "required": ["transaction_id", "user_id", "amount", "timestamp"],
        "properties": {
            "transaction_id": {"bsonType": "string"},
            "user_id": {"bsonType": "int"},
            "amount": {"bsonType": "double"},
            "currency": {"bsonType": "string"},
            "merchant": {"bsonType": "string"},
            "category": {"bsonType": "string"},
            "timestamp": {"bsonType": "date"},
            "status": {"bsonType": "string"},
            "payment_method": {"bsonType": "string"},
            "processed_at": {"bsonType": "date"},
            "source_type": {"bsonType": "string"}
        }
    }
    
    transaction_events_indexes = [
        {"keys": [("user_id", 1), ("timestamp", -1)]},
        {"keys": [("transaction_id", 1)], "unique": True},
        {"keys": [("timestamp", -1)]},
        {"keys": [("status", 1)]},
        {"keys": [("merchant", 1)]}
    ]
    
    # Weather data collection
    weather_data_schema = {
        "bsonType": "object",
        "required": ["city", "timestamp"],
        "properties": {
            "city": {"bsonType": "string"},
            "country": {"bsonType": "string"},
            "temperature": {"bsonType": "double"},
            "humidity": {"bsonType": "int"},
            "timestamp": {"bsonType": "date"},
            "source_type": {"bsonType": "string"}
        }
    }
    
    weather_data_indexes = [
        {"keys": [("city", 1), ("timestamp", -1)]},
        {"keys": [("timestamp", -1)]},
        {"keys": [("country", 1)]}
    ]
    
    # Stock data collection
    stock_data_schema = {
        "bsonType": "object",
        "required": ["symbol", "price", "timestamp"],
        "properties": {
            "symbol": {"bsonType": "string"},
            "price": {"bsonType": "double"},
            "timestamp": {"bsonType": "date"},
            "currency": {"bsonType": "string"},
            "source_type": {"bsonType": "string"}
        }
    }
    
    stock_data_indexes = [
        {"keys": [("symbol", 1), ("timestamp", -1)]},
        {"keys": [("timestamp", -1)]},
        {"keys": [("symbol", 1)]}
    ]
    
    # Create collections
    collections_config = [
        ("user_events", user_events_schema, user_events_indexes),
        ("transaction_events", transaction_events_schema, transaction_events_indexes),
        ("weather_data", weather_data_schema, weather_data_indexes),
        ("stock_data", stock_data_schema, stock_data_indexes)
    ]
    
    for collection_name, schema, indexes in collections_config:
        mongodb_client.create_collection_with_schema(collection_name, schema, indexes)


if __name__ == "__main__":
    # Test MongoDB client
    client = MongoDBClient()
    
    if client.is_connected():
        print("MongoDB connection successful!")
        
        # Set up default collections
        setup_default_collections(client)
        
        # List collections
        collections = client.list_collections()
        print(f"Collections: {collections}")
        
        # Test data insertion
        test_data = [
            {"test_field": "test_value", "timestamp": datetime.now()},
            {"test_field": "test_value_2", "timestamp": datetime.now()}
        ]
        
        success = client.load_documents(test_data, "test_collection")
        print(f"Test data insertion: {'SUCCESS' if success else 'FAILED'}")
        
        # Query test data
        documents = client.query_documents("test_collection")
        print(f"Retrieved {len(documents)} test documents")
        
        # Clean up test data
        deleted = client.delete_documents("test_collection", {})
        print(f"Deleted {deleted} test documents")
        
    else:
        print("Failed to connect to MongoDB")
    
    client.close()