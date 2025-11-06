"""
Weather API Data Fetcher

Fetches weather data from external APIs and processes it for storage.
"""

import os
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import aiohttp
import requests
import pandas as pd
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from ..mongodb_connector.connection import get_mongodb_connector

logger = structlog.get_logger(__name__)


class WeatherAPIFetcher:
    """
    Weather API data fetcher with support for multiple weather services.
    """
    
    def __init__(self, api_key: str = None, base_url: str = None):
        """
        Initialize weather API fetcher.
        
        Args:
            api_key: API key for weather service (defaults to env WEATHER_API_KEY)
            base_url: Base URL for weather API (defaults to OpenWeatherMap)
        """
        self.api_key = api_key or os.getenv('WEATHER_API_KEY')
        self.base_url = base_url or os.getenv('WEATHER_API_URL', 'https://api.openweathermap.org/data/2.5')
        self.mongodb_connector = get_mongodb_connector()
        
        # Default locations to fetch weather for
        self.default_locations = [
            {'name': 'New York', 'lat': 40.7128, 'lon': -74.0060},
            {'name': 'London', 'lat': 51.5074, 'lon': -0.1278},
            {'name': 'Tokyo', 'lat': 35.6762, 'lon': 139.6503},
            {'name': 'Sydney', 'lat': -33.8688, 'lon': 151.2093},
            {'name': 'Mumbai', 'lat': 19.0760, 'lon': 72.8777}
        ]
        
        logger.info("Weather API fetcher initialized", 
                   base_url=self.base_url,
                   has_api_key=bool(self.api_key))
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def fetch_current_weather_async(self, lat: float, lon: float, location_name: str = None) -> Optional[Dict[str, Any]]:
        """
        Fetch current weather data asynchronously.
        
        Args:
            lat: Latitude
            lon: Longitude
            location_name: Optional location name for metadata
            
        Returns:
            Weather data dictionary or None if error
        """
        if not self.api_key:
            logger.error("Weather API key not provided")
            return None
        
        url = f"{self.base_url}/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Transform and clean data
                        weather_data = self._transform_weather_data(data, location_name)
                        
                        logger.info("Weather data fetched successfully", 
                                   location=location_name or f"{lat},{lon}",
                                   temperature=weather_data.get('temperature'))
                        
                        return weather_data
                    else:
                        logger.error("Weather API request failed", 
                                   status=response.status,
                                   location=location_name or f"{lat},{lon}")
                        return None
                        
        except Exception as e:
            logger.error("Failed to fetch weather data", 
                        error=str(e),
                        location=location_name or f"{lat},{lon}")
            return None
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_current_weather_sync(self, lat: float, lon: float, location_name: str = None) -> Optional[Dict[str, Any]]:
        """
        Fetch current weather data synchronously.
        
        Args:
            lat: Latitude
            lon: Longitude
            location_name: Optional location name for metadata
            
        Returns:
            Weather data dictionary or None if error
        """
        if not self.api_key:
            logger.error("Weather API key not provided")
            return None
        
        url = f"{self.base_url}/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Transform and clean data
                weather_data = self._transform_weather_data(data, location_name)
                
                logger.info("Weather data fetched successfully", 
                           location=location_name or f"{lat},{lon}",
                           temperature=weather_data.get('temperature'))
                
                return weather_data
            else:
                logger.error("Weather API request failed", 
                           status=response.status_code,
                           location=location_name or f"{lat},{lon}")
                return None
                
        except Exception as e:
            logger.error("Failed to fetch weather data", 
                        error=str(e),
                        location=location_name or f"{lat},{lon}")
            return None
    
    def _transform_weather_data(self, raw_data: Dict[str, Any], location_name: str = None) -> Dict[str, Any]:
        """
        Transform raw weather API data into standardized format.
        
        Args:
            raw_data: Raw data from weather API
            location_name: Optional location name
            
        Returns:
            Transformed weather data
        """
        try:
            main = raw_data.get('main', {})
            weather = raw_data.get('weather', [{}])[0]
            wind = raw_data.get('wind', {})
            clouds = raw_data.get('clouds', {})
            sys = raw_data.get('sys', {})
            coord = raw_data.get('coord', {})
            
            transformed_data = {
                # Location information
                'location_name': location_name or raw_data.get('name', 'Unknown'),
                'country': sys.get('country'),
                'latitude': coord.get('lat'),
                'longitude': coord.get('lon'),
                
                # Weather conditions
                'temperature': main.get('temp'),
                'feels_like': main.get('feels_like'),
                'temperature_min': main.get('temp_min'),
                'temperature_max': main.get('temp_max'),
                'pressure': main.get('pressure'),
                'humidity': main.get('humidity'),
                'sea_level_pressure': main.get('sea_level'),
                'ground_level_pressure': main.get('grnd_level'),
                
                # Weather description
                'weather_main': weather.get('main'),
                'weather_description': weather.get('description'),
                'weather_icon': weather.get('icon'),
                
                # Wind information
                'wind_speed': wind.get('speed'),
                'wind_direction': wind.get('deg'),
                'wind_gust': wind.get('gust'),
                
                # Cloud information
                'cloudiness': clouds.get('all'),
                
                # Visibility
                'visibility': raw_data.get('visibility'),
                
                # Timestamps
                'data_timestamp': datetime.fromtimestamp(raw_data.get('dt', 0)),
                'sunrise': datetime.fromtimestamp(sys.get('sunrise', 0)) if sys.get('sunrise') else None,
                'sunset': datetime.fromtimestamp(sys.get('sunset', 0)) if sys.get('sunset') else None,
                'fetch_timestamp': datetime.utcnow(),
                
                # Metadata
                'data_source': 'openweathermap',
                'api_version': '2.5',
                'raw_data_id': raw_data.get('id')
            }
            
            # Remove None values
            transformed_data = {k: v for k, v in transformed_data.items() if v is not None}
            
            return transformed_data
            
        except Exception as e:
            logger.error("Failed to transform weather data", error=str(e))
            return {}
    
    async def fetch_multiple_locations_async(self, locations: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Fetch weather data for multiple locations asynchronously.
        
        Args:
            locations: List of location dictionaries with 'lat', 'lon', and optional 'name'
            
        Returns:
            List of weather data dictionaries
        """
        locations = locations or self.default_locations
        
        tasks = []
        for location in locations:
            task = self.fetch_current_weather_async(
                location['lat'], 
                location['lon'], 
                location.get('name')
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results and exceptions
        weather_data = []
        for result in results:
            if isinstance(result, dict) and result:
                weather_data.append(result)
            elif isinstance(result, Exception):
                logger.error("Exception in async weather fetch", error=str(result))
        
        logger.info("Multiple location weather fetch completed", 
                   requested=len(locations), 
                   successful=len(weather_data))
        
        return weather_data
    
    def fetch_multiple_locations_sync(self, locations: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Fetch weather data for multiple locations synchronously.
        
        Args:
            locations: List of location dictionaries with 'lat', 'lon', and optional 'name'
            
        Returns:
            List of weather data dictionaries
        """
        locations = locations or self.default_locations
        
        weather_data = []
        for location in locations:
            data = self.fetch_current_weather_sync(
                location['lat'], 
                location['lon'], 
                location.get('name')
            )
            if data:
                weather_data.append(data)
        
        logger.info("Multiple location weather fetch completed", 
                   requested=len(locations), 
                   successful=len(weather_data))
        
        return weather_data
    
    def save_weather_data(self, weather_data: List[Dict[str, Any]], collection_name: str = "weather_data") -> bool:
        """
        Save weather data to MongoDB.
        
        Args:
            weather_data: List of weather data dictionaries
            collection_name: MongoDB collection name
            
        Returns:
            bool: True if save successful, False otherwise
        """
        if not weather_data:
            logger.warning("No weather data to save")
            return False
        
        try:
            # Connect to MongoDB
            if not self.mongodb_connector.connect():
                return False
            
            # Create indexes for efficient querying
            self.mongodb_connector.create_index(collection_name, [
                ("location_name", 1),
                ("fetch_timestamp", -1)
            ])
            
            # Save data
            success = self.mongodb_connector.insert_documents(collection_name, weather_data)
            
            if success:
                logger.info("Weather data saved to MongoDB", 
                           collection=collection_name, 
                           count=len(weather_data))
            
            return success
            
        except Exception as e:
            logger.error("Failed to save weather data", 
                        collection=collection_name, 
                        error=str(e))
            return False
        finally:
            self.mongodb_connector.disconnect()
    
    async def fetch_and_save_async(self, locations: List[Dict[str, Any]] = None, 
                                  collection_name: str = "weather_data") -> Dict[str, Any]:
        """
        Fetch weather data and save to MongoDB asynchronously.
        
        Args:
            locations: List of location dictionaries
            collection_name: MongoDB collection name
            
        Returns:
            Dictionary with operation results
        """
        try:
            # Fetch weather data
            weather_data = await self.fetch_multiple_locations_async(locations)
            
            # Save to MongoDB
            success = self.save_weather_data(weather_data, collection_name)
            
            return {
                'success': success,
                'locations_requested': len(locations or self.default_locations),
                'data_points_fetched': len(weather_data),
                'collection': collection_name,
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error("Weather fetch and save operation failed", error=str(e))
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.utcnow()
            }
    
    def fetch_and_save_sync(self, locations: List[Dict[str, Any]] = None, 
                           collection_name: str = "weather_data") -> Dict[str, Any]:
        """
        Fetch weather data and save to MongoDB synchronously.
        
        Args:
            locations: List of location dictionaries
            collection_name: MongoDB collection name
            
        Returns:
            Dictionary with operation results
        """
        try:
            # Fetch weather data
            weather_data = self.fetch_multiple_locations_sync(locations)
            
            # Save to MongoDB
            success = self.save_weather_data(weather_data, collection_name)
            
            return {
                'success': success,
                'locations_requested': len(locations or self.default_locations),
                'data_points_fetched': len(weather_data),
                'collection': collection_name,
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error("Weather fetch and save operation failed", error=str(e))
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.utcnow()
            }


# Convenience functions
async def fetch_weather_data_async(api_key: str = None, locations: List[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Convenience function to fetch weather data asynchronously.
    
    Args:
        api_key: Weather API key
        locations: List of location dictionaries
        
    Returns:
        Dictionary with operation results
    """
    fetcher = WeatherAPIFetcher(api_key=api_key)
    return await fetcher.fetch_and_save_async(locations)


def fetch_weather_data_sync(api_key: str = None, locations: List[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Convenience function to fetch weather data synchronously.
    
    Args:
        api_key: Weather API key
        locations: List of location dictionaries
        
    Returns:
        Dictionary with operation results
    """
    fetcher = WeatherAPIFetcher(api_key=api_key)
    return fetcher.fetch_and_save_sync(locations)