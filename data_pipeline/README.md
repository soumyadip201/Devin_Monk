# Data Ingestion Pipeline

A consolidated data ingestion pipeline that handles multiple data sources and loads them into MongoDB with comprehensive data processing, cleaning, and transformation capabilities.

## Features

- **CSV File Ingestion**: Process CSV files from local directories using PySpark
- **Real-time Streaming**: Kafka streaming for user events and transaction events
- **API Data Fetching**: Scheduled fetching from weather and stock price APIs
- **Data Processing**: Comprehensive data cleaning, transformation, and validation
- **MongoDB Integration**: Efficient data loading with schema validation and indexing
- **Logging & Monitoring**: Comprehensive logging and error handling
- **Orchestration**: Centralized orchestrator for coordinating all pipeline components

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CSV Files     │    │  Kafka Topics    │    │  External APIs  │
│   (/data)       │    │  - user_events   │    │  - Weather API  │
│                 │    │  - transactions  │    │  - Stock API    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ CSV Ingestion   │    │ Kafka Streaming  │    │ API Fetcher     │
│ (PySpark)       │    │ (Spark Stream)   │    │ (Scheduled)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌──────────────────────┐
                    │   Data Processing    │
                    │   - Cleaning         │
                    │   - Transformation   │
                    │   - Validation       │
                    └──────────────────────┘
                                 │
                                 ▼
                    ┌──────────────────────┐
                    │      MongoDB         │
                    │   - Collections      │
                    │   - Indexes          │
                    │   - Schema Validation│
                    └──────────────────────┘
```

## Components

### 1. CSV Ingestion Module (`ingestion/csv_ingestion.py`)
- Discovers and processes CSV files from specified directories
- Uses PySpark for distributed processing
- Handles data cleaning and transformation
- Supports custom transformation rules per file type

### 2. Kafka Streaming Module (`streaming/kafka_streaming.py`)
- Connects to Kafka topics for real-time data streaming
- Processes user events and transaction events
- Uses Spark Structured Streaming for fault-tolerant processing
- Supports schema validation and data quality checks

### 3. API Data Fetcher (`api_fetcher/api_fetcher.py`)
- Scheduled fetching from external APIs (weather, stock prices)
- Concurrent API requests with rate limiting
- Automatic retry logic and error handling
- Mock data generation for testing without API keys

### 4. MongoDB Client (`database/mongodb_client.py`)
- Handles MongoDB connections and operations
- Supports both Spark and Pandas DataFrame loading
- Automatic schema validation and index creation
- Batch processing for efficient data loading

### 5. Pipeline Orchestrator (`orchestrator/pipeline_orchestrator.py`)
- Coordinates all pipeline components
- Manages component lifecycle and dependencies
- Provides centralized configuration and monitoring
- Graceful shutdown handling

### 6. Utilities (`utils/`)
- Centralized logging configuration
- Error handling and monitoring utilities
- Configuration management

## Installation

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up MongoDB**:
   ```bash
   # Using Docker
   docker run -d -p 27017:27017 --name mongodb mongo:latest
   
   # Or install MongoDB locally
   # Follow MongoDB installation guide for your OS
   ```

3. **Set up Kafka** (Optional, for streaming):
   ```bash
   # Using Docker Compose
   docker-compose up -d kafka zookeeper
   
   # Or install Kafka locally
   # Follow Kafka installation guide
   ```

4. **Configure Environment**:
   ```bash
   # Copy and edit environment file
   cp data_pipeline/config/environment.env .env
   
   # Edit .env file with your configuration
   # Set API keys, database connections, etc.
   ```

## Configuration

### Pipeline Configuration (`data_pipeline/config/pipeline_config.json`)

The pipeline uses a JSON configuration file to specify:
- MongoDB connection settings
- Spark configuration
- Kafka settings
- API configurations
- Data transformation rules
- Logging settings

### Environment Variables (`data_pipeline/config/environment.env`)

Set environment variables for:
- Database connections
- API keys
- File paths
- Logging levels

## Usage

### Basic Usage

1. **Create Sample Data** (for testing):
   ```bash
   python run_pipeline.py --create-sample-data
   ```

2. **Run Complete Pipeline**:
   ```bash
   python run_pipeline.py
   ```

3. **Run Specific Components**:
   ```bash
   # CSV ingestion only
   python run_pipeline.py --csv-only
   
   # Kafka streaming only
   python run_pipeline.py --kafka-only
   
   # API fetching only
   python run_pipeline.py --api-only
   ```

4. **Check Pipeline Status**:
   ```bash
   python run_pipeline.py --status
   ```

### Advanced Usage

1. **Custom Configuration**:
   ```bash
   python run_pipeline.py --config /path/to/custom/config.json
   ```

2. **Debug Mode**:
   ```bash
   python run_pipeline.py --log-level DEBUG
   ```

3. **Using Environment Variables**:
   ```bash
   export MONGODB_CONNECTION_STRING="mongodb://localhost:27017/"
   export WEATHER_API_KEY="your_api_key"
   python run_pipeline.py
   ```

## Data Sources

### CSV Files
Place CSV files in the `data/` directory. The pipeline will automatically discover and process them.

Supported formats:
- Standard CSV with headers
- Various data types (strings, numbers, dates)
- Custom transformation rules per file type

### Kafka Topics
Configure Kafka topics in the pipeline configuration:
- `user_events`: User activity events
- `transaction_events`: Financial transaction events

### External APIs
- **Weather API**: OpenWeatherMap API for weather data
- **Stock API**: Twelve Data API for stock prices

## Data Processing

### Cleaning Operations
- Remove duplicate records
- Handle missing values
- Trim whitespace from strings
- Validate data types
- Filter invalid records

### Transformations
- Column renaming
- Data type conversions
- Custom expressions
- Metadata addition (timestamps, source tracking)

### Validation
- Schema validation
- Data quality checks
- Business rule validation
- Error reporting

## MongoDB Collections

The pipeline creates the following collections:

1. **user_events**: User activity data from Kafka
2. **transaction_events**: Transaction data from Kafka
3. **weather_data**: Weather information from API
4. **stock_data**: Stock price data from API
5. **users**: User data from CSV files
6. **transactions**: Transaction data from CSV files

Each collection includes:
- Automatic indexing for query performance
- Schema validation
- Metadata fields (insertion timestamps, source tracking)

## Monitoring and Logging

### Logging
- Centralized logging configuration
- Multiple log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Rotating log files to prevent disk space issues
- Separate error logs for critical issues

### Monitoring
- Pipeline status monitoring
- Component health checks
- Data processing metrics
- Error tracking and alerting

## Error Handling

- Graceful error handling at all levels
- Automatic retry logic for transient failures
- Detailed error logging and reporting
- Partial failure recovery (continue processing other data)

## Performance Optimization

- Spark optimizations for large-scale data processing
- Batch processing for efficient MongoDB writes
- Connection pooling and reuse
- Concurrent API requests
- Configurable batch sizes and timeouts

## Testing

1. **Unit Tests**:
   ```bash
   pytest tests/
   ```

2. **Integration Tests**:
   ```bash
   pytest tests/integration/
   ```

3. **Sample Data Testing**:
   ```bash
   python run_pipeline.py --create-sample-data
   python run_pipeline.py --csv-only
   ```

## Troubleshooting

### Common Issues

1. **MongoDB Connection Failed**:
   - Check MongoDB is running
   - Verify connection string
   - Check network connectivity

2. **Spark Initialization Failed**:
   - Verify Java is installed (Java 8 or 11)
   - Check Spark configuration
   - Ensure sufficient memory

3. **Kafka Connection Issues**:
   - Verify Kafka is running
   - Check bootstrap servers configuration
   - Ensure topics exist

4. **API Rate Limiting**:
   - Check API key validity
   - Verify rate limits
   - Adjust request intervals

### Log Files
Check log files in `data_pipeline/logs/` for detailed error information:
- `data_pipeline.log`: Main pipeline log
- `*_errors.log`: Error-specific logs
- Component-specific logs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.