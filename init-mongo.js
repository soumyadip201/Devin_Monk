// MongoDB initialization script
db = db.getSiblingDB('monkdb');

// Create collections with indexes
db.createCollection('csv_data');
db.csv_data.createIndex({ "_ingestion_timestamp": -1 });
db.csv_data.createIndex({ "_file_source": 1 });

db.createCollection('weather_data');
db.weather_data.createIndex({ "location_name": 1, "fetch_timestamp": -1 });
db.weather_data.createIndex({ "fetch_timestamp": -1 });

db.createCollection('stock_data');
db.stock_data.createIndex({ "symbol": 1, "fetch_timestamp": -1 });
db.stock_data.createIndex({ "fetch_timestamp": -1 });

db.createCollection('weather_stream_data');
db.weather_stream_data.createIndex({ "processing_timestamp": -1 });

db.createCollection('stock_stream_data');
db.stock_stream_data.createIndex({ "processing_timestamp": -1 });

db.createCollection('csv_stream_data');
db.csv_stream_data.createIndex({ "processing_timestamp": -1 });

print('MonkDB collections and indexes created successfully!');