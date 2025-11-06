# Data Ingestion Pipeline

A comprehensive data ingestion pipeline that consolidates data from multiple sources including CSV files, weather APIs, and stock price APIs, processes them using PySpark, and stores them in MonkDB with real-time streaming capabilities via Kafka.

## 🚀 Features

- **CSV File Processing**: Automated ingestion of CSV files using PySpark with data cleaning and validation
- **Weather API Integration**: Hourly fetching of weather data from OpenWeatherMap API
- **Stock Price API Integration**: Real-time stock price data from Alpha Vantage API
- **Real-time Streaming**: Kafka-based streaming with Spark Structured Streaming
- **MonkDB Storage**: All processed data stored in MonkDB with proper indexing
- **Comprehensive Logging**: Structured logging with error tracking and monitoring
- **Scheduled Processing**: Automated scheduling using the `schedule` library
- **Configurable Pipeline**: YAML-based configuration with environment variable support

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CSV Files     │───▶│                 │    │                 │
│                 │    │                 │    │                 │
├─────────────────┤    │   Data          │    │   PySpark       │
│  Weather API    │───▶│   Ingestion     │───▶│   Processing    │
│                 │    │   Orchestrator  │    │                 │
├─────────────────┤    │                 │    │                 │
│   Stock API     │───▶│                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
┌─────────────────┐             │                        │
│     Kafka       │◀────────────┘                        │
│   (Streaming)   │                                      │
└─────────────────┘                                      │
         │                                               │
         ▼                                               ▼
┌─────────────────┐                            ┌─────────────────┐
│    MonkDB       │◀───────────────────────────│   Spark         │
│   (Storage)     │                            │   Streaming     │
└─────────────────┘                            └─────────────────┘
```

## 📋 Prerequisites

- Python 3.11+
- Apache Spark 3.5+
- Apache Kafka (optional, for streaming)
- MonkDB instance
- API keys for weather and stock data services

## 🚀 Quick Start

### 1. Environment Setup

```bash
# Copy environment template
cp .env.data_ingestion .env

# Edit .env file with your configuration
# - Add your API keys
# - Configure MongoDB connection
# - Set Kafka bootstrap servers
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configuration

Edit `config/data_ingestion.yaml` to customize:
- Data source locations
- Processing intervals
- Stock symbols to track
- Weather locations to monitor

### 4. Run the Pipeline

```bash
# Run the complete pipeline
python -m data_ingestion.main --mode run

# Test the pipeline
python -m data_ingestion.main --mode test

# Check pipeline status
python -m data_ingestion.main --mode status
```

## 📁 Module Structure

```
data_ingestion/
├── __init__.py                 # Package initialization
├── main.py                     # Main entry point
├── orchestrator.py             # Main orchestrator
├── logging_config.py           # Logging configuration
├── README.md                   # This file
├── csv_ingestion/              # CSV processing module
│   ├── __init__.py
│   └── processor.py            # PySpark CSV processor
├── weather_api/                # Weather API module
│   ├── __init__.py
│   └── fetcher.py              # Weather data fetcher
├── stock_api/                  # Stock API module
│   ├── __init__.py
│   └── fetcher.py              # Stock data fetcher
├── kafka_streaming/            # Kafka streaming module
│   ├── __init__.py
│   └── processor.py            # Kafka stream processor
└── mongodb_connector/          # MongoDB module
    ├── __init__.py
    └── connection.py           # MongoDB connector
```

## 🔧 Configuration Options

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MONKDB_HOST` | MongoDB host | localhost |
| `MONKDB_PORT` | MongoDB port | 27017 |
| `MONKDB_DATABASE` | Database name | monkdb |
| `WEATHER_API_KEY` | OpenWeatherMap API key | - |
| `STOCK_API_KEY` | Alpha Vantage API key | - |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka servers | localhost:9092 |
| `CSV_DATA_DIRECTORY` | CSV files directory | ./data |
| `API_FETCH_INTERVAL` | API fetch interval (minutes) | 60 |
| `CSV_PROCESSING_INTERVAL` | CSV processing interval (minutes) | 30 |

### Command Line Options

```bash
python -m data_ingestion.main [OPTIONS]

Options:
  --mode {run,test,status}     Operation mode (default: run)
  --config-file PATH           Path to configuration file
  --log-level {DEBUG,INFO,WARNING,ERROR}  Logging level
  --enable-kafka               Enable Kafka streaming
  --enable-apis                Enable API data fetching
  --enable-csv                 Enable CSV processing
  --csv-directory PATH         CSV data directory path
  --api-interval INT           API fetch interval in minutes
  --csv-interval INT           CSV processing interval in minutes
```

## 📊 Data Sources

### 1. CSV Files
- **Location**: Configurable directory (default: `./data`)
- **Format**: Standard CSV with headers
- **Processing**: PySpark-based with data cleaning and validation
- **Storage**: MonkDB collection `csv_data`

### 2. Weather API
- **Provider**: OpenWeatherMap
- **Frequency**: Hourly (configurable)
- **Locations**: Configurable list of coordinates
- **Storage**: MonkDB collection `weather_data`

### 3. Stock Price API
- **Provider**: Alpha Vantage
- **Frequency**: Hourly (configurable)
- **Symbols**: Configurable list of stock symbols
- **Storage**: MonkDB collection `stock_data`

## 🔄 Real-time Streaming

The pipeline supports real-time data streaming using Kafka and Spark Structured Streaming:

- **Kafka Topics**: `weather-data`, `stock-data`, `csv-data`
- **Processing**: Spark Structured Streaming
- **Storage**: Separate collections for streaming data

## 📈 Monitoring and Logging

### Logging Features
- **Structured Logging**: JSON-formatted logs with contextual information
- **Multiple Outputs**: Console and file logging
- **Error Tracking**: Centralized error tracking and reporting
- **Performance Metrics**: Execution time tracking

### Log Files
- **Location**: `./logs/`
- **Format**: JSON for file logs, colored console output
- **Rotation**: Daily log files with timestamps

### Monitoring Endpoints
- Pipeline status and statistics
- Error summaries and recent failures
- Processing metrics and performance data

## 🧪 Testing

### Run Tests
```bash
# Test all components
python -m data_ingestion.main --mode test

# Test specific components
python -c "
from data_ingestion.orchestrator import get_orchestrator
orchestrator = get_orchestrator()
results = orchestrator.run_manual_fetch('weather')  # or 'stock', 'csv', 'all'
print(results)
"
```

### Sample Data
The pipeline includes sample CSV files in the `data/` directory:
- `sample_sales.csv`: Sales transaction data
- `sample_inventory.csv`: Product inventory data

## 🔒 Security Considerations

- **API Keys**: Store in environment variables, never in code
- **Database Credentials**: Use environment variables or secure configuration
- **Network Security**: Configure appropriate firewall rules for Kafka and MongoDB
- **Data Validation**: Input validation and sanitization for all data sources

## 🚨 Error Handling

The pipeline includes comprehensive error handling:
- **Retry Logic**: Automatic retries with exponential backoff
- **Graceful Degradation**: Continue processing other sources if one fails
- **Error Tracking**: Centralized error logging and reporting
- **Recovery**: Automatic recovery from transient failures

## 📚 API Documentation

### Weather Data Schema
```json
{
  "location_name": "string",
  "temperature": "float",
  "humidity": "integer",
  "pressure": "float",
  "weather_description": "string",
  "wind_speed": "float",
  "fetch_timestamp": "datetime"
}
```

### Stock Data Schema
```json
{
  "symbol": "string",
  "price": "float",
  "volume": "integer",
  "change": "float",
  "change_percent": "float",
  "fetch_timestamp": "datetime"
}
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is part of the MonkDB Data Platform and follows the same licensing terms.

## 🆘 Troubleshooting

### Common Issues

1. **Spark Session Initialization Fails**
   - Check Java installation and JAVA_HOME
   - Verify Spark dependencies are installed
   - Check available memory and adjust Spark configuration

2. **MongoDB Connection Issues**
   - Verify MongoDB is running
   - Check connection credentials
   - Ensure network connectivity

3. **API Rate Limiting**
   - Check API key validity
   - Verify rate limit settings
   - Consider upgrading API plan for higher limits

4. **Kafka Connection Issues**
   - Verify Kafka is running
   - Check bootstrap server configuration
   - Ensure topics exist or can be auto-created

### Getting Help

- Check the logs in `./logs/` for detailed error information
- Use `--mode test` to diagnose specific component issues
- Enable DEBUG logging for more detailed information
- Review the configuration file for correct settings

---

**Built with ❤️ for the MonkDB Data Platform**