#!/usr/bin/env python3
"""
Pipeline Monitoring Script

Real-time monitoring of the data ingestion pipeline.
"""

import os
import sys
import time
import json
from datetime import datetime
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def check_infrastructure():
    """Check infrastructure services."""
    print("🔍 Infrastructure Status")
    print("-" * 30)
    
    # Check MongoDB
    try:
        import pymongo
        client = pymongo.MongoClient('mongodb://admin:password@localhost:27017/', serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ MongoDB: Connected")
        
        # Get database stats
        db = client.monkdb
        collections = db.list_collection_names()
        print(f"   Collections: {len(collections)}")
        
        for collection in ['csv_data', 'weather_data', 'stock_data']:
            if collection in collections:
                count = db[collection].count_documents({})
                print(f"   {collection}: {count} documents")
        
        client.close()
        
    except Exception as e:
        print(f"❌ MongoDB: {e}")
    
    # Check Kafka
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], consumer_timeout_ms=5000)
        topics = consumer.topics()
        print(f"✅ Kafka: Connected ({len(topics)} topics)")
        consumer.close()
    except Exception as e:
        print(f"❌ Kafka: {e}")

def check_pipeline_status():
    """Check pipeline status."""
    print("\n🚀 Pipeline Status")
    print("-" * 30)
    
    try:
        from data_ingestion.orchestrator import get_orchestrator
        
        orchestrator = get_orchestrator()
        status = orchestrator.get_status()
        
        print(f"Running: {'✅' if status['is_running'] else '❌'}")
        print(f"Total Runs: {status['statistics']['total_runs']}")
        print(f"Successful Runs: {status['statistics']['successful_runs']}")
        print(f"Failed Runs: {status['statistics']['failed_runs']}")
        
        if status['statistics']['last_run_time']:
            print(f"Last Run: {status['statistics']['last_run_time']}")
        
        if status['statistics']['last_success_time']:
            print(f"Last Success: {status['statistics']['last_success_time']}")
        
        # Show next scheduled jobs
        if status.get('next_scheduled_jobs'):
            print("\n📅 Scheduled Jobs:")
            for job in status['next_scheduled_jobs']:
                print(f"   {job['job']}: {job['next_run']}")
        
    except Exception as e:
        print(f"❌ Pipeline Status Error: {e}")

def check_recent_logs():
    """Check recent log entries."""
    print("\n📋 Recent Logs")
    print("-" * 30)
    
    log_dir = project_root / "logs"
    if not log_dir.exists():
        print("❌ No log directory found")
        return
    
    # Find the most recent log file
    log_files = list(log_dir.glob("data_ingestion_*.log"))
    if not log_files:
        print("❌ No log files found")
        return
    
    latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
    
    try:
        with open(latest_log, 'r') as f:
            lines = f.readlines()
            recent_lines = lines[-10:] if len(lines) > 10 else lines
            
            print(f"📄 {latest_log.name} (last 10 lines):")
            for line in recent_lines:
                line = line.strip()
                if line:
                    # Color code based on log level
                    if '"level": "error"' in line.lower() or 'error' in line.lower():
                        print(f"🔴 {line}")
                    elif '"level": "warning"' in line.lower() or 'warning' in line.lower():
                        print(f"🟡 {line}")
                    elif '"level": "info"' in line.lower() or 'info' in line.lower():
                        print(f"🔵 {line}")
                    else:
                        print(f"⚪ {line}")
    
    except Exception as e:
        print(f"❌ Error reading logs: {e}")

def run_quick_test():
    """Run a quick test of the pipeline."""
    print("\n🧪 Quick Test")
    print("-" * 30)
    
    try:
        from data_ingestion.orchestrator import get_orchestrator
        
        orchestrator = get_orchestrator()
        
        # Test CSV processing
        print("Testing CSV processing...")
        csv_result = orchestrator.run_manual_fetch('csv')
        if csv_result.get('csv', {}).get('success', False):
            print("✅ CSV processing: OK")
        else:
            print(f"❌ CSV processing: {csv_result.get('csv', {}).get('error', 'Failed')}")
        
        # Note: API tests would require valid API keys
        print("ℹ️  API tests require valid API keys in .env file")
        
    except Exception as e:
        print(f"❌ Quick test error: {e}")

def show_system_info():
    """Show system information."""
    print("\n💻 System Information")
    print("-" * 30)
    
    # Python version
    print(f"Python: {sys.version.split()[0]}")
    
    # Java version
    try:
        import subprocess
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            java_version = result.stderr.split('\n')[0].split('"')[1]
            print(f"Java: {java_version}")
        else:
            print("Java: Not found")
    except:
        print("Java: Not found")
    
    # JAVA_HOME
    java_home = os.environ.get('JAVA_HOME', 'Not set')
    print(f"JAVA_HOME: {java_home}")
    
    # Docker
    try:
        import subprocess
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            docker_version = result.stdout.strip()
            print(f"Docker: {docker_version}")
        else:
            print("Docker: Not found")
    except:
        print("Docker: Not found")

def main():
    """Main monitoring function."""
    print("🔍 Data Ingestion Pipeline Monitor")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    show_system_info()
    check_infrastructure()
    check_pipeline_status()
    check_recent_logs()
    run_quick_test()
    
    print("\n" + "=" * 50)
    print("Monitor completed! 🎉")

def continuous_monitor():
    """Run continuous monitoring."""
    print("🔄 Starting continuous monitoring (Ctrl+C to stop)...")
    
    try:
        while True:
            os.system('clear' if os.name == 'posix' else 'cls')
            main()
            print(f"\n⏰ Next update in 30 seconds...")
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        continuous_monitor()
    else:
        main()
        print("\n💡 Tip: Use --continuous for real-time monitoring")