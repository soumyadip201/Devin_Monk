#!/usr/bin/env python3
"""
Simple test script for the data ingestion pipeline.
"""

import os
import sys
import tempfile
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from data_ingestion.logging_config import setup_logging
from data_ingestion.csv_ingestion.processor import process_csv_directory
from data_ingestion.mongodb_connector.connection import MonkDBConnector

def test_csv_processing():
    """Test CSV processing functionality."""
    print("Testing CSV processing...")
    
    # Use the existing data directory
    data_dir = project_root / "data"
    
    if not data_dir.exists():
        print(f"❌ Data directory not found: {data_dir}")
        return False
    
    try:
        # Test CSV processing
        results = process_csv_directory(str(data_dir), "test_csv_data")
        
        if results.get('success', False):
            print(f"✅ CSV processing successful!")
            print(f"   Files processed: {results.get('files_processed', 0)}")
            print(f"   Records processed: {results.get('records_processed', 0)}")
            return True
        else:
            print(f"❌ CSV processing failed: {results.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ CSV processing exception: {e}")
        return False

def test_mongodb_connection():
    """Test MongoDB connection."""
    print("Testing MongoDB connection...")
    
    try:
        connector = MonkDBConnector()
        
        # Try to connect
        if connector.connect():
            print("✅ MongoDB connection successful!")
            
            # Test basic operations
            test_doc = {"test": "data", "timestamp": "2024-01-01"}
            if connector.insert_document("test_collection", test_doc):
                print("✅ MongoDB insert test successful!")
            else:
                print("❌ MongoDB insert test failed!")
                
            connector.disconnect()
            return True
        else:
            print("❌ MongoDB connection failed!")
            return False
            
    except Exception as e:
        print(f"❌ MongoDB connection exception: {e}")
        return False

def main():
    """Run all tests."""
    print("🧪 Starting Data Ingestion Pipeline Tests")
    print("=" * 50)
    
    # Setup logging
    setup_logging(level="INFO")
    
    # Run tests
    tests = [
        ("MongoDB Connection", test_mongodb_connection),
        ("CSV Processing", test_csv_processing),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🔍 Running {test_name} test...")
        try:
            if test_func():
                passed += 1
            else:
                print(f"❌ {test_name} test failed!")
        except Exception as e:
            print(f"❌ {test_name} test exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("💥 Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())