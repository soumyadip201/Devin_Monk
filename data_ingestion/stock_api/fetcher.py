"""
Stock Price API Data Fetcher

Fetches stock price data from external APIs and processes it for storage.
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


class StockAPIFetcher:
    """
    Stock price API data fetcher with support for multiple stock data providers.
    """
    
    def __init__(self, api_key: str = None, base_url: str = None):
        """
        Initialize stock API fetcher.
        
        Args:
            api_key: API key for stock service (defaults to env STOCK_API_KEY)
            base_url: Base URL for stock API (defaults to Alpha Vantage)
        """
        self.api_key = api_key or os.getenv('STOCK_API_KEY')
        self.base_url = base_url or os.getenv('STOCK_API_URL', 'https://www.alphavantage.co/query')
        self.mongodb_connector = get_mongodb_connector()
        
        # Default stock symbols to fetch
        self.default_symbols = [
            'AAPL',  # Apple
            'GOOGL', # Google
            'MSFT',  # Microsoft
            'AMZN',  # Amazon
            'TSLA',  # Tesla
            'NVDA',  # NVIDIA
            'META',  # Meta (Facebook)
            'NFLX',  # Netflix
            'SPY',   # S&P 500 ETF
            'QQQ'    # NASDAQ ETF
        ]
        
        logger.info("Stock API fetcher initialized", 
                   base_url=self.base_url,
                   has_api_key=bool(self.api_key))
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def fetch_stock_quote_async(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch current stock quote asynchronously.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            
        Returns:
            Stock data dictionary or None if error
        """
        if not self.api_key:
            logger.error("Stock API key not provided")
            return None
        
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Check for API errors
                        if 'Error Message' in data:
                            logger.error("Stock API error", 
                                       symbol=symbol, 
                                       error=data['Error Message'])
                            return None
                        
                        if 'Note' in data:
                            logger.warning("Stock API rate limit", 
                                         symbol=symbol, 
                                         note=data['Note'])
                            return None
                        
                        # Transform and clean data
                        stock_data = self._transform_stock_quote_data(data, symbol)
                        
                        if stock_data:
                            logger.info("Stock quote fetched successfully", 
                                       symbol=symbol,
                                       price=stock_data.get('price'))
                        
                        return stock_data
                    else:
                        logger.error("Stock API request failed", 
                                   status=response.status,
                                   symbol=symbol)
                        return None
                        
        except Exception as e:
            logger.error("Failed to fetch stock quote", 
                        error=str(e),
                        symbol=symbol)
            return None
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_stock_quote_sync(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch current stock quote synchronously.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            
        Returns:
            Stock data dictionary or None if error
        """
        if not self.api_key:
            logger.error("Stock API key not provided")
            return None
        
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check for API errors
                if 'Error Message' in data:
                    logger.error("Stock API error", 
                               symbol=symbol, 
                               error=data['Error Message'])
                    return None
                
                if 'Note' in data:
                    logger.warning("Stock API rate limit", 
                                 symbol=symbol, 
                                 note=data['Note'])
                    return None
                
                # Transform and clean data
                stock_data = self._transform_stock_quote_data(data, symbol)
                
                if stock_data:
                    logger.info("Stock quote fetched successfully", 
                               symbol=symbol,
                               price=stock_data.get('price'))
                
                return stock_data
            else:
                logger.error("Stock API request failed", 
                           status=response.status_code,
                           symbol=symbol)
                return None
                
        except Exception as e:
            logger.error("Failed to fetch stock quote", 
                        error=str(e),
                        symbol=symbol)
            return None
    
    def _transform_stock_quote_data(self, raw_data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        """
        Transform raw stock API data into standardized format.
        
        Args:
            raw_data: Raw data from stock API
            symbol: Stock symbol
            
        Returns:
            Transformed stock data
        """
        try:
            # Alpha Vantage Global Quote format
            quote_data = raw_data.get('Global Quote', {})
            
            if not quote_data:
                logger.error("No quote data found in response", symbol=symbol)
                return {}
            
            # Extract and clean data
            price = self._safe_float(quote_data.get('05. price'))
            open_price = self._safe_float(quote_data.get('02. open'))
            high_price = self._safe_float(quote_data.get('03. high'))
            low_price = self._safe_float(quote_data.get('04. low'))
            previous_close = self._safe_float(quote_data.get('08. previous close'))
            change = self._safe_float(quote_data.get('09. change'))
            change_percent = quote_data.get('10. change percent', '').replace('%', '')
            change_percent = self._safe_float(change_percent)
            volume = self._safe_int(quote_data.get('06. volume'))
            
            transformed_data = {
                # Basic information
                'symbol': symbol.upper(),
                'price': price,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'previous_close': previous_close,
                'volume': volume,
                
                # Change information
                'change': change,
                'change_percent': change_percent,
                
                # Calculated fields
                'market_cap_estimate': None,  # Would need additional API call
                'pe_ratio': None,             # Would need additional API call
                
                # Timestamps
                'latest_trading_day': self._parse_date(quote_data.get('07. latest trading day')),
                'fetch_timestamp': datetime.utcnow(),
                
                # Metadata
                'data_source': 'alphavantage',
                'api_function': 'GLOBAL_QUOTE',
                'currency': 'USD'  # Assuming USD for most stocks
            }
            
            # Remove None values
            transformed_data = {k: v for k, v in transformed_data.items() if v is not None}
            
            return transformed_data
            
        except Exception as e:
            logger.error("Failed to transform stock data", 
                        symbol=symbol, 
                        error=str(e))
            return {}
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        try:
            if value is None or value == '':
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        try:
            if value is None or value == '':
                return None
            return int(float(value))  # Convert through float to handle decimal strings
        except (ValueError, TypeError):
            return None
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object."""
        try:
            if not date_str:
                return None
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return None
    
    async def fetch_multiple_stocks_async(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch stock data for multiple symbols asynchronously.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            List of stock data dictionaries
        """
        symbols = symbols or self.default_symbols
        
        # Add delay between requests to respect API rate limits
        stock_data = []
        for symbol in symbols:
            data = await self.fetch_stock_quote_async(symbol)
            if data:
                stock_data.append(data)
            
            # Rate limiting - wait between requests
            await asyncio.sleep(12)  # Alpha Vantage free tier: 5 requests per minute
        
        logger.info("Multiple stock fetch completed", 
                   requested=len(symbols), 
                   successful=len(stock_data))
        
        return stock_data
    
    def fetch_multiple_stocks_sync(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch stock data for multiple symbols synchronously.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            List of stock data dictionaries
        """
        symbols = symbols or self.default_symbols
        
        stock_data = []
        for i, symbol in enumerate(symbols):
            data = self.fetch_stock_quote_sync(symbol)
            if data:
                stock_data.append(data)
            
            # Rate limiting - wait between requests (except for last request)
            if i < len(symbols) - 1:
                import time
                time.sleep(12)  # Alpha Vantage free tier: 5 requests per minute
        
        logger.info("Multiple stock fetch completed", 
                   requested=len(symbols), 
                   successful=len(stock_data))
        
        return stock_data
    
    def save_stock_data(self, stock_data: List[Dict[str, Any]], collection_name: str = "stock_data") -> bool:
        """
        Save stock data to MongoDB.
        
        Args:
            stock_data: List of stock data dictionaries
            collection_name: MongoDB collection name
            
        Returns:
            bool: True if save successful, False otherwise
        """
        if not stock_data:
            logger.warning("No stock data to save")
            return False
        
        try:
            # Connect to MongoDB
            if not self.mongodb_connector.connect():
                return False
            
            # Create indexes for efficient querying
            self.mongodb_connector.create_index(collection_name, [
                ("symbol", 1),
                ("fetch_timestamp", -1)
            ])
            
            # Save data
            success = self.mongodb_connector.insert_documents(collection_name, stock_data)
            
            if success:
                logger.info("Stock data saved to MongoDB", 
                           collection=collection_name, 
                           count=len(stock_data))
            
            return success
            
        except Exception as e:
            logger.error("Failed to save stock data", 
                        collection=collection_name, 
                        error=str(e))
            return False
        finally:
            self.mongodb_connector.disconnect()
    
    async def fetch_and_save_async(self, symbols: List[str] = None, 
                                  collection_name: str = "stock_data") -> Dict[str, Any]:
        """
        Fetch stock data and save to MongoDB asynchronously.
        
        Args:
            symbols: List of stock symbols
            collection_name: MongoDB collection name
            
        Returns:
            Dictionary with operation results
        """
        try:
            # Fetch stock data
            stock_data = await self.fetch_multiple_stocks_async(symbols)
            
            # Save to MongoDB
            success = self.save_stock_data(stock_data, collection_name)
            
            return {
                'success': success,
                'symbols_requested': len(symbols or self.default_symbols),
                'data_points_fetched': len(stock_data),
                'collection': collection_name,
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error("Stock fetch and save operation failed", error=str(e))
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.utcnow()
            }
    
    def fetch_and_save_sync(self, symbols: List[str] = None, 
                           collection_name: str = "stock_data") -> Dict[str, Any]:
        """
        Fetch stock data and save to MongoDB synchronously.
        
        Args:
            symbols: List of stock symbols
            collection_name: MongoDB collection name
            
        Returns:
            Dictionary with operation results
        """
        try:
            # Fetch stock data
            stock_data = self.fetch_multiple_stocks_sync(symbols)
            
            # Save to MongoDB
            success = self.save_stock_data(stock_data, collection_name)
            
            return {
                'success': success,
                'symbols_requested': len(symbols or self.default_symbols),
                'data_points_fetched': len(stock_data),
                'collection': collection_name,
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error("Stock fetch and save operation failed", error=str(e))
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.utcnow()
            }


# Convenience functions
async def fetch_stock_data_async(api_key: str = None, symbols: List[str] = None) -> Dict[str, Any]:
    """
    Convenience function to fetch stock data asynchronously.
    
    Args:
        api_key: Stock API key
        symbols: List of stock symbols
        
    Returns:
        Dictionary with operation results
    """
    fetcher = StockAPIFetcher(api_key=api_key)
    return await fetcher.fetch_and_save_async(symbols)


def fetch_stock_data_sync(api_key: str = None, symbols: List[str] = None) -> Dict[str, Any]:
    """
    Convenience function to fetch stock data synchronously.
    
    Args:
        api_key: Stock API key
        symbols: List of stock symbols
        
    Returns:
        Dictionary with operation results
    """
    fetcher = StockAPIFetcher(api_key=api_key)
    return fetcher.fetch_and_save_sync(symbols)