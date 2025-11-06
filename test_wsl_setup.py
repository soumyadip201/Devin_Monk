#!/usr/bin/env python3
"""
WSL Setup Validation Script

Tests all components of the data ingestion pipeline on WSL.
"""

import os
import sys
import subprocess
import time
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def check_java():
    """Check if Java is installed and JAVA_HOME is set."""
    print("🔍 Checking Java installation...")
    
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Java is installed")
            java_home = os.environ.get('JAVA_HOME')
            if java_home:
                print(f"✅ JAVA_HOME is set: {java_home}")
                return True
            else:
                print("❌ JAVA_HOME is not set")
                return False
        else:
            print("❌ Java is not installed")
            return False
    except FileNotFoundError:
        print("❌ Java is not installed")
        return False

def check_docker():
    """Check if Docker is running."""
    print("🔍 Checking Docker...")
    
    try:
        result = subprocess.run(['docker', 'ps'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Docker is running")
            return True
        else:
            print("❌ Docker is not running")
            return False
    except FileNotFoundError:
        print("❌ Docker is not installed")
        return False

def check_infrastructure():
    """Check if MongoDB and Kafka are running."""
    print("🔍 Checking infrastructure services...")
    
    # Check MongoDB
    try:
        import pymongo
        client = pymongo.MongoClient('mongodb://admin:password@localhost:27017/')
        client.admin.command('ping')
        print("✅ MongoDB is accessible")
        mongodb_ok = True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        mongodb_ok = False
    
    # Check Kafka
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        producer.close()
        print("✅ Kafka is accessible")
        kafka_ok = True
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        kafka_ok = False
    
    return mongodb_ok and kafka_ok

def test_pipeline_components():
    """Test individual pipeline components."""
    print("🔍 Testing pipeline components...")
    
    try:
        # Test imports
        from data_ingestion.logging_config import setup_logging
        from data_ingestion.mongodb_connector.connection import MonkDBConnector
        from data_ingestion.csv_ingestion.processor import CSVProcessor
        print("✅ All imports successful")
        
        # Test logging
        setup_logging()
        print("✅ Logging setup successful")
        
        # Test MongoDB connector
        connector = MonkDBConnector()
        if connector.connect():
            print("✅ MongoDB connector working")
            connector.disconnect()
        else:
            print("❌ MongoDB connector failed")
            return False
        
        # Test CSV processor (without Spark for now)
        processor = CSVProcessor()
        print("✅ CSV processor created")
        
        return True
        
    except Exception as e:
        print(f"❌ Pipeline component test failed: {e}")
        return False

def test_sample_data():
    """Test with sample data."""
    print("🔍 Testing with sample data...")
    
    try:
        # Check if sample data exists
        data_dir = project_root / "data"
        csv_files = list(data_dir.glob("*.csv"))
        
        if csv_files:
            print(f"✅ Found {len(csv_files)} sample CSV files")
            for csv_file in csv_files:
                print(f"   - {csv_file.name}")
            return True
        else:
            print("❌ No sample CSV files found")
            return False
            
    except Exception as e:
        print(f"❌ Sample data test failed: {e}")
        return False

def main():
    """Run all validation tests."""
    print("🧪 WSL Data Ingestion Pipeline Validation")
    print("=" * 50)
    
    tests = [
        ("Java Installation", check_java),
        ("Docker Service", check_docker),
        ("Infrastructure Services", check_infrastructure),
        ("Pipeline Components", test_pipeline_components),
        ("Sample Data", test_sample_data),
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
        print("🎉 All tests passed! Your WSL setup is ready!")
        print("\n📋 Next steps:")
        print("1. Get API keys from OpenWeatherMap and Alpha Vantage")
        print("2. Update .env.wsl with your API keys")
        print("3. Run: python -m data_ingestion.main --mode test")
        return 0
    else:
        print("💥 Some tests failed! Please check the setup.")
        print("\n🔧 Troubleshooting:")
        if not check_java():
            print("- Run: sudo apt install openjdk-11-jdk")
            print("- Set JAVA_HOME in ~/.bashrc")
        if not check_docker():
            print("- Start Docker: sudo service docker start")
            print("- Or restart WSL")
        return 1

if __name__ == "__main__":
    sys.exit(main())