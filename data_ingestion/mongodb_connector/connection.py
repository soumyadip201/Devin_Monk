"""
MongoDB Connection Module

Handles connection to MonkDB and provides data loading functionality.
"""

import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import structlog

logger = structlog.get_logger(__name__)


class MonkDBConnector:
    """
    MongoDB connector for data ingestion pipeline.
    Handles connection, data insertion, and basic operations.
    """
    
    def __init__(self, 
                 host: str = None, 
                 port: int = None, 
                 database: str = None,
                 username: str = None,
                 password: str = None):
        """
        Initialize MongoDB connector.
        
        Args:
            host: MongoDB host (defaults to env MONKDB_HOST)
            port: MongoDB port (defaults to env MONKDB_PORT)
            database: Database name (defaults to env MONKDB_DATABASE)
            username: Username (defaults to env MONKDB_USERNAME)
            password: Password (defaults to env MONKDB_PASSWORD)
        """
        self.host = host or os.getenv('MONKDB_HOST', 'localhost')
        self.port = port or int(os.getenv('MONKDB_PORT', '27017'))
        self.database_name = database or os.getenv('MONKDB_DATABASE', 'monkdb')
        self.username = username or os.getenv('MONKDB_USERNAME')
        self.password = password or os.getenv('MONKDB_PASSWORD')
        
        self.client = None
        self.database = None
        
        logger.info("MonkDB connector initialized", 
                   host=self.host, 
                   port=self.port, 
                   database=self.database_name)
    
    def connect(self) -> bool:
        """
        Establish connection to MongoDB.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Build connection string
            if self.username and self.password:
                connection_string = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"
            else:
                connection_string = f"mongodb://{self.host}:{self.port}/"
            
            # Create client with timeout
            self.client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=10000,         # 10 second connection timeout
                socketTimeoutMS=20000           # 20 second socket timeout
            )
            
            # Test connection
            self.client.admin.command('ping')
            self.database = self.client[self.database_name]
            
            logger.info("Successfully connected to MonkDB", 
                       database=self.database_name)
            return True
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error("Failed to connect to MonkDB", 
                        error=str(e), 
                        host=self.host, 
                        port=self.port)
            return False
        except Exception as e:
            logger.error("Unexpected error connecting to MonkDB", 
                        error=str(e))
            return False
    
    def disconnect(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MonkDB")
    
    def insert_documents(self, collection_name: str, documents: List[Dict[str, Any]]) -> bool:
        """
        Insert multiple documents into a collection.
        
        Args:
            collection_name: Name of the collection
            documents: List of documents to insert
            
        Returns:
            bool: True if insertion successful, False otherwise
        """
        if not self.database:
            logger.error("Database connection not established")
            return False
        
        try:
            collection = self.database[collection_name]
            
            # Add metadata to each document
            for doc in documents:
                doc['_ingestion_timestamp'] = datetime.utcnow()
                doc['_source'] = 'data_ingestion_pipeline'
            
            result = collection.insert_many(documents)
            
            logger.info("Documents inserted successfully", 
                       collection=collection_name, 
                       count=len(result.inserted_ids))
            return True
            
        except Exception as e:
            logger.error("Failed to insert documents", 
                        collection=collection_name, 
                        error=str(e))
            return False
    
    def insert_document(self, collection_name: str, document: Dict[str, Any]) -> bool:
        """
        Insert a single document into a collection.
        
        Args:
            collection_name: Name of the collection
            document: Document to insert
            
        Returns:
            bool: True if insertion successful, False otherwise
        """
        return self.insert_documents(collection_name, [document])
    
    def upsert_document(self, collection_name: str, document: Dict[str, Any], 
                       filter_key: str) -> bool:
        """
        Upsert a document (update if exists, insert if not).
        
        Args:
            collection_name: Name of the collection
            document: Document to upsert
            filter_key: Key to use for filtering existing documents
            
        Returns:
            bool: True if operation successful, False otherwise
        """
        if not self.database:
            logger.error("Database connection not established")
            return False
        
        try:
            collection = self.database[collection_name]
            
            # Add metadata
            document['_ingestion_timestamp'] = datetime.utcnow()
            document['_source'] = 'data_ingestion_pipeline'
            
            filter_dict = {filter_key: document[filter_key]}
            
            result = collection.replace_one(
                filter_dict, 
                document, 
                upsert=True
            )
            
            action = "updated" if result.matched_count > 0 else "inserted"
            logger.info(f"Document {action} successfully", 
                       collection=collection_name, 
                       filter_key=filter_key)
            return True
            
        except Exception as e:
            logger.error("Failed to upsert document", 
                        collection=collection_name, 
                        error=str(e))
            return False
    
    def create_index(self, collection_name: str, index_spec: List[tuple]) -> bool:
        """
        Create an index on a collection.
        
        Args:
            collection_name: Name of the collection
            index_spec: Index specification as list of (field, direction) tuples
            
        Returns:
            bool: True if index creation successful, False otherwise
        """
        if not self.database:
            logger.error("Database connection not established")
            return False
        
        try:
            collection = self.database[collection_name]
            collection.create_index(index_spec)
            
            logger.info("Index created successfully", 
                       collection=collection_name, 
                       index=index_spec)
            return True
            
        except Exception as e:
            logger.error("Failed to create index", 
                        collection=collection_name, 
                        error=str(e))
            return False
    
    def get_collection_stats(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """
        Get statistics for a collection.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            Dict with collection statistics or None if error
        """
        if not self.database:
            logger.error("Database connection not established")
            return None
        
        try:
            collection = self.database[collection_name]
            stats = self.database.command("collStats", collection_name)
            
            return {
                'count': stats.get('count', 0),
                'size': stats.get('size', 0),
                'avgObjSize': stats.get('avgObjSize', 0),
                'storageSize': stats.get('storageSize', 0),
                'indexes': stats.get('nindexes', 0)
            }
            
        except Exception as e:
            logger.error("Failed to get collection stats", 
                        collection=collection_name, 
                        error=str(e))
            return None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


# Singleton instance for global use
_mongodb_connector = None

def get_mongodb_connector() -> MonkDBConnector:
    """
    Get singleton MongoDB connector instance.
    
    Returns:
        MonkDBConnector: Singleton connector instance
    """
    global _mongodb_connector
    if _mongodb_connector is None:
        _mongodb_connector = MonkDBConnector()
    return _mongodb_connector