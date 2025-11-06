# 🚀 WSL Quick Start Guide

## Step-by-Step Setup

### 1. **Initial Setup**
```bash
# Clone the repository
git clone https://github.com/soumyadip201/Devin_Monk.git
cd Devin_Monk
git checkout sm-api

# Run the setup script
chmod +x setup_wsl.sh
./setup_wsl.sh

# Restart your terminal or run:
source ~/.bashrc
```

### 2. **Start Infrastructure Services**
```bash
# Start MongoDB and Kafka using Docker Compose
docker-compose -f docker-compose.dev.yml up -d

# Check if services are running
docker-compose -f docker-compose.dev.yml ps
```

### 3. **Setup Python Environment**
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 4. **Configure Environment**
```bash
# Copy WSL environment file
cp .env.wsl .env

# Edit the .env file to add your API keys
nano .env
```

### 5. **Get API Keys**

#### OpenWeatherMap API Key:
1. Go to https://openweathermap.org/api
2. Sign up for a free account
3. Get your API key from the dashboard
4. Add it to `.env` file: `WEATHER_API_KEY=your_key_here`

#### Alpha Vantage API Key:
1. Go to https://www.alphavantage.co/support/#api-key
2. Get a free API key
3. Add it to `.env` file: `STOCK_API_KEY=your_key_here`

### 6. **Validate Setup**
```bash
# Run validation tests
python test_wsl_setup.py
```

### 7. **Test the Pipeline**
```bash
# Test all components
python -m data_ingestion.main --mode test

# Run the pipeline
python -m data_ingestion.main --mode run

# Check status
python -m data_ingestion.main --mode status
```

## 🔍 How to Check if Everything is Working

### 1. **Infrastructure Health Check**
```bash
# Check Docker containers
docker-compose -f docker-compose.dev.yml ps

# Should show:
# - monkdb-dev (MongoDB)
# - kafka-dev (Kafka)
# - zookeeper-dev (Zookeeper)
# - kafka-ui-dev (Kafka UI - optional)
# - mongo-express-dev (MongoDB UI - optional)
```

### 2. **Database Verification**
```bash
# Connect to MongoDB
docker exec -it monkdb-dev mongosh -u admin -p password

# In MongoDB shell:
use monkdb
show collections
db.csv_data.find().limit(5)
exit
```

### 3. **Web Interfaces**
- **MongoDB Express**: http://localhost:8081
- **Kafka UI**: http://localhost:8080

### 4. **Pipeline Testing**
```bash
# Test individual components
python -c "
from data_ingestion.orchestrator import get_orchestrator
orchestrator = get_orchestrator()
results = orchestrator.run_manual_fetch('csv')
print('CSV Results:', results)
"
```

### 5. **Log Monitoring**
```bash
# Watch logs in real-time
tail -f logs/data_ingestion_$(date +%Y%m%d).log

# Or check recent logs
ls -la logs/
cat logs/data_ingestion_*.log | tail -50
```

## 🐛 Troubleshooting

### Common Issues:

#### 1. **Java Not Found**
```bash
# Install Java
sudo apt install openjdk-11-jdk

# Set JAVA_HOME
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

#### 2. **Docker Permission Denied**
```bash
# Add user to docker group
sudo usermod -aG docker $USER

# Restart WSL or logout/login
```

#### 3. **MongoDB Connection Failed**
```bash
# Check if MongoDB container is running
docker ps | grep mongo

# Restart MongoDB
docker-compose -f docker-compose.dev.yml restart mongodb
```

#### 4. **Kafka Connection Issues**
```bash
# Check Kafka logs
docker logs kafka-dev

# Restart Kafka services
docker-compose -f docker-compose.dev.yml restart kafka zookeeper
```

#### 5. **PySpark Issues**
```bash
# Check Java version
java -version

# Check JAVA_HOME
echo $JAVA_HOME

# Test Spark
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('test').getOrCreate(); print('Spark OK')"
```

## 📊 Monitoring and Verification

### 1. **Real-time Monitoring**
```bash
# Monitor pipeline logs
tail -f logs/data_ingestion_*.log

# Monitor Docker containers
docker stats

# Monitor system resources
htop
```

### 2. **Data Verification**
```bash
# Check data in MongoDB
python -c "
import pymongo
client = pymongo.MongoClient('mongodb://admin:password@localhost:27017/')
db = client.monkdb
print('Collections:', db.list_collection_names())
print('CSV Data Count:', db.csv_data.count_documents({}))
print('Weather Data Count:', db.weather_data.count_documents({}))
print('Stock Data Count:', db.stock_data.count_documents({}))
"
```

### 3. **Performance Testing**
```bash
# Run performance test
python -c "
import time
from data_ingestion.orchestrator import get_orchestrator

start_time = time.time()
orchestrator = get_orchestrator()
results = orchestrator.run_manual_fetch('all')
end_time = time.time()

print(f'Execution time: {end_time - start_time:.2f} seconds')
print('Results:', results)
"
```

## 🎯 Success Indicators

✅ **All containers running**: `docker ps` shows all services  
✅ **MongoDB accessible**: Can connect and query data  
✅ **Kafka accessible**: Can produce/consume messages  
✅ **Pipeline tests pass**: `python -m data_ingestion.main --mode test`  
✅ **Data flowing**: New records appearing in MongoDB collections  
✅ **Logs clean**: No error messages in log files  
✅ **APIs responding**: Weather and stock data being fetched  

## 🚀 Production Deployment

Once everything works in WSL, you can:

1. **Deploy to cloud**: Use the same Docker Compose setup
2. **Scale services**: Add more Kafka brokers, MongoDB replicas
3. **Add monitoring**: Prometheus, Grafana, ELK stack
4. **Implement CI/CD**: GitHub Actions, automated deployments

---

**Your data ingestion pipeline is now ready to run on WSL! 🎉**