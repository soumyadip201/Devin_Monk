"""
API Data Fetching Module

This module handles scheduled fetching of data from external APIs,
processes the data, and loads it into MongoDB.
"""

import json
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
import requests
import pandas as pd
import schedule
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..utils.logger import get_logger
from ..database.mongodb_client import MongoDBClient


class APIDataFetcher:
    """
    Handles scheduled fetching of data from external APIs with data processing and MongoDB integration.
    """
    
    def __init__(self, mongodb_client: Optional[MongoDBClient] = None, 
                 max_workers: int = 4, request_timeout: int = 30):
        """
        Initialize the API data fetcher.
        
        Args:
            mongodb_client: MongoDB client for data loading.
            max_workers: Maximum number of concurrent API requests.
            request_timeout: Request timeout in seconds.
        """
        self.logger = get_logger(__name__)
        self.mongodb_client = mongodb_client
        self.max_workers = max_workers
        self.request_timeout = request_timeout
        self.is_running = False
        self.scheduler_thread = None
        
        # API configurations
        self.api_configs = {
            "weather": {
                "base_url": "https://api.openweathermap.org/data/2.5",
                "api_key": None,  # Should be set via configuration
                "cities": ["New York", "London", "Tokyo", "Paris", "Sydney"],
                "collection": "weather_data"
            },
            "stock": {
                "base_url": "https://api.twelvedata.com/v1",
                "api_key": None,  # Should be set via configuration
                "symbols": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
                "collection": "stock_data"
            }
        }
        
        self.logger.info("API Data Fetcher initialized")
    
    def set_api_key(self, api_type: str, api_key: str):
        """
        Set API key for a specific API type.
        
        Args:
            api_type: Type of API ("weather" or "stock").
            api_key: API key to set.
        """
        if api_type in self.api_configs:
            self.api_configs[api_type]["api_key"] = api_key
            self.logger.info(f"API key set for {api_type}")
        else:
            self.logger.error(f"Unknown API type: {api_type}")
    
    def make_api_request(self, url: str, params: Optional[Dict[str, Any]] = None, 
                        headers: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        """
        Make an API request with error handling and retries.
        
        Args:
            url: API endpoint URL.
            params: Query parameters.
            headers: Request headers.
            
        Returns:
            API response data or None if request fails.
        """
        try:
            response = requests.get(
                url, 
                params=params, 
                headers=headers, 
                timeout=self.request_timeout
            )
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed for {url}: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response from {url}: {str(e)}")
            return None
    
    def fetch_weather_data(self, city: str) -> Optional[Dict[str, Any]]:
        """
        Fetch weather data for a specific city.
        
        Args:
            city: City name to fetch weather data for.
            
        Returns:
            Weather data or None if fetch fails.
        """
        try:
            config = self.api_configs["weather"]
            
            if not config["api_key"]:
                self.logger.warning("Weather API key not configured, using mock data")
                return self._generate_mock_weather_data(city)
            
            url = f"{config['base_url']}/weather"
            params = {
                "q": city,
                "appid": config["api_key"],
                "units": "metric"
            }
            
            data = self.make_api_request(url, params)
            
            if data:
                # Process and clean the weather data
                processed_data = {
                    "city": city,
                    "country": data.get("sys", {}).get("country"),
                    "temperature": data.get("main", {}).get("temp"),
                    "feels_like": data.get("main", {}).get("feels_like"),
                    "humidity": data.get("main", {}).get("humidity"),
                    "pressure": data.get("main", {}).get("pressure"),
                    "weather_main": data.get("weather", [{}])[0].get("main"),
                    "weather_description": data.get("weather", [{}])[0].get("description"),
                    "wind_speed": data.get("wind", {}).get("speed"),
                    "wind_direction": data.get("wind", {}).get("deg"),
                    "cloudiness": data.get("clouds", {}).get("all"),
                    "visibility": data.get("visibility"),
                    "timestamp": datetime.now(),
                    "source_type": "weather_api",
                    "api_timestamp": datetime.fromtimestamp(data.get("dt", time.time()))
                }
                
                self.logger.info(f"Successfully fetched weather data for {city}")
                return processed_data
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching weather data for {city}: {str(e)}")
            return None
    
    def fetch_stock_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch stock price data for a specific symbol.
        
        Args:
            symbol: Stock symbol to fetch data for.
            
        Returns:
            Stock data or None if fetch fails.
        """
        try:
            config = self.api_configs["stock"]
            
            if not config["api_key"]:
                self.logger.warning("Stock API key not configured, using mock data")
                return self._generate_mock_stock_data(symbol)
            
            url = f"{config['base_url']}/price"
            params = {
                "symbol": symbol,
                "apikey": config["api_key"]
            }
            
            data = self.make_api_request(url, params)
            
            if data:
                # Process and clean the stock data
                processed_data = {
                    "symbol": symbol,
                    "price": float(data.get("price", 0)),
                    "timestamp": datetime.now(),
                    "source_type": "stock_api",
                    "currency": "USD"
                }
                
                # Fetch additional data (quote)
                quote_url = f"{config['base_url']}/quote"
                quote_data = self.make_api_request(quote_url, params)
                
                if quote_data:
                    processed_data.update({
                        "open": float(quote_data.get("open", 0)),
                        "high": float(quote_data.get("high", 0)),
                        "low": float(quote_data.get("low", 0)),
                        "close": float(quote_data.get("close", 0)),
                        "volume": int(quote_data.get("volume", 0)),
                        "previous_close": float(quote_data.get("previous_close", 0)),
                        "change": float(quote_data.get("change", 0)),
                        "percent_change": float(quote_data.get("percent_change", 0))
                    })
                
                self.logger.info(f"Successfully fetched stock data for {symbol}")
                return processed_data
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching stock data for {symbol}: {str(e)}")
            return None
    
    def _generate_mock_weather_data(self, city: str) -> Dict[str, Any]:
        """Generate mock weather data for testing purposes."""
        import random
        
        return {
            "city": city,
            "country": "US",
            "temperature": round(random.uniform(-10, 35), 1),
            "feels_like": round(random.uniform(-10, 35), 1),
            "humidity": random.randint(30, 90),
            "pressure": random.randint(980, 1030),
            "weather_main": random.choice(["Clear", "Clouds", "Rain", "Snow"]),
            "weather_description": random.choice(["clear sky", "few clouds", "light rain", "snow"]),
            "wind_speed": round(random.uniform(0, 15), 1),
            "wind_direction": random.randint(0, 360),
            "cloudiness": random.randint(0, 100),
            "visibility": random.randint(1000, 10000),
            "timestamp": datetime.now(),
            "source_type": "weather_api_mock",
            "api_timestamp": datetime.now()
        }
    
    def _generate_mock_stock_data(self, symbol: str) -> Dict[str, Any]:
        """Generate mock stock data for testing purposes."""
        import random
        
        base_price = random.uniform(50, 500)
        change = random.uniform(-10, 10)
        
        return {
            "symbol": symbol,
            "price": round(base_price, 2),
            "open": round(base_price * random.uniform(0.98, 1.02), 2),
            "high": round(base_price * random.uniform(1.0, 1.05), 2),
            "low": round(base_price * random.uniform(0.95, 1.0), 2),
            "close": round(base_price, 2),
            "volume": random.randint(1000000, 50000000),
            "previous_close": round(base_price - change, 2),
            "change": round(change, 2),
            "percent_change": round((change / base_price) * 100, 2),
            "timestamp": datetime.now(),
            "source_type": "stock_api_mock",
            "currency": "USD"
        }
    
    def fetch_all_weather_data(self) -> List[Dict[str, Any]]:
        """
        Fetch weather data for all configured cities concurrently.
        
        Returns:
            List of weather data dictionaries.
        """
        weather_data = []
        cities = self.api_configs["weather"]["cities"]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_city = {executor.submit(self.fetch_weather_data, city): city for city in cities}
            
            for future in as_completed(future_to_city):
                city = future_to_city[future]
                try:
                    data = future.result()
                    if data:
                        weather_data.append(data)
                except Exception as e:
                    self.logger.error(f"Error fetching weather data for {city}: {str(e)}")
        
        self.logger.info(f"Fetched weather data for {len(weather_data)} cities")
        return weather_data
    
    def fetch_all_stock_data(self) -> List[Dict[str, Any]]:
        """
        Fetch stock data for all configured symbols concurrently.
        
        Returns:
            List of stock data dictionaries.
        """
        stock_data = []
        symbols = self.api_configs["stock"]["symbols"]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_symbol = {executor.submit(self.fetch_stock_data, symbol): symbol for symbol in symbols}
            
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    data = future.result()
                    if data:
                        stock_data.append(data)
                except Exception as e:
                    self.logger.error(f"Error fetching stock data for {symbol}: {str(e)}")
        
        self.logger.info(f"Fetched stock data for {len(stock_data)} symbols")
        return stock_data
    
    def process_and_load_weather_data(self):
        """Fetch weather data and load it into MongoDB."""
        try:
            self.logger.info("Starting weather data fetch and load process")
            
            weather_data = self.fetch_all_weather_data()
            
            if weather_data and self.mongodb_client:
                # Convert to DataFrame for processing
                df = pd.DataFrame(weather_data)
                
                # Load to MongoDB
                collection_name = self.api_configs["weather"]["collection"]
                success = self.mongodb_client.load_dataframe_from_pandas(df, collection_name)
                
                if success:
                    self.logger.info(f"Successfully loaded {len(weather_data)} weather records to MongoDB")
                else:
                    self.logger.error("Failed to load weather data to MongoDB")
            else:
                self.logger.warning("No weather data to load or MongoDB client not available")
                
        except Exception as e:
            self.logger.error(f"Error in weather data process: {str(e)}")
    
    def process_and_load_stock_data(self):
        """Fetch stock data and load it into MongoDB."""
        try:
            self.logger.info("Starting stock data fetch and load process")
            
            stock_data = self.fetch_all_stock_data()
            
            if stock_data and self.mongodb_client:
                # Convert to DataFrame for processing
                df = pd.DataFrame(stock_data)
                
                # Load to MongoDB
                collection_name = self.api_configs["stock"]["collection"]
                success = self.mongodb_client.load_dataframe_from_pandas(df, collection_name)
                
                if success:
                    self.logger.info(f"Successfully loaded {len(stock_data)} stock records to MongoDB")
                else:
                    self.logger.error("Failed to load stock data to MongoDB")
            else:
                self.logger.warning("No stock data to load or MongoDB client not available")
                
        except Exception as e:
            self.logger.error(f"Error in stock data process: {str(e)}")
    
    def setup_schedules(self):
        """Set up scheduled jobs for API data fetching."""
        try:
            # Schedule weather data fetching every hour
            schedule.every().hour.do(self.process_and_load_weather_data)
            
            # Schedule stock data fetching every hour
            schedule.every().hour.do(self.process_and_load_stock_data)
            
            # For testing, also schedule every 5 minutes
            # schedule.every(5).minutes.do(self.process_and_load_weather_data)
            # schedule.every(5).minutes.do(self.process_and_load_stock_data)
            
            self.logger.info("Scheduled jobs set up successfully")
            
        except Exception as e:
            self.logger.error(f"Error setting up schedules: {str(e)}")
    
    def run_scheduler(self):
        """Run the scheduler in a separate thread."""
        self.logger.info("Starting API data fetcher scheduler")
        
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(60)
    
    def start(self):
        """Start the API data fetcher with scheduled jobs."""
        try:
            if self.is_running:
                self.logger.warning("API data fetcher is already running")
                return
            
            self.is_running = True
            self.setup_schedules()
            
            # Run initial data fetch
            self.logger.info("Running initial data fetch...")
            self.process_and_load_weather_data()
            self.process_and_load_stock_data()
            
            # Start scheduler in a separate thread
            self.scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
            self.scheduler_thread.start()
            
            self.logger.info("API data fetcher started successfully")
            
        except Exception as e:
            self.logger.error(f"Error starting API data fetcher: {str(e)}")
            self.is_running = False
    
    def stop(self):
        """Stop the API data fetcher."""
        try:
            self.is_running = False
            
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=5)
            
            # Clear scheduled jobs
            schedule.clear()
            
            self.logger.info("API data fetcher stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error stopping API data fetcher: {str(e)}")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get status of the API data fetcher.
        
        Returns:
            Status information dictionary.
        """
        return {
            "is_running": self.is_running,
            "scheduled_jobs": len(schedule.jobs),
            "weather_cities": len(self.api_configs["weather"]["cities"]),
            "stock_symbols": len(self.api_configs["stock"]["symbols"]),
            "weather_api_configured": self.api_configs["weather"]["api_key"] is not None,
            "stock_api_configured": self.api_configs["stock"]["api_key"] is not None,
            "mongodb_connected": self.mongodb_client is not None
        }


if __name__ == "__main__":
    # Initialize API data fetcher
    fetcher = APIDataFetcher()
    
    try:
        # Start the fetcher
        fetcher.start()
        
        # Print status
        status = fetcher.get_status()
        print("API Data Fetcher Status:", json.dumps(status, indent=2))
        
        # Run for a short time for testing
        print("Running for 30 seconds...")
        time.sleep(30)
        
    except KeyboardInterrupt:
        print("Stopping API data fetcher...")
    finally:
        fetcher.stop()