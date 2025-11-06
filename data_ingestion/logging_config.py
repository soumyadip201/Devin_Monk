"""
Logging Configuration

Centralized logging configuration for the data ingestion pipeline.
"""

import os
import sys
import logging
from typing import Optional
import structlog
from datetime import datetime


def setup_logging(level: str = "INFO", 
                 log_file: Optional[str] = None,
                 enable_json_logs: bool = False) -> None:
    """
    Setup structured logging for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
        enable_json_logs: Whether to use JSON format for logs
    """
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Default log file if not specified
    if log_file is None:
        timestamp = datetime.now().strftime("%Y%m%d")
        log_file = os.path.join(log_dir, f"data_ingestion_{timestamp}.log")
    
    # Configure processors
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    if enable_json_logs:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Setup file handler for persistent logging
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        
        # Use JSON format for file logs
        file_formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
            '"logger": "%(name)s", "message": "%(message)s"}'
        )
        file_handler.setFormatter(file_formatter)
        
        # Add to root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
    
    # Log the configuration
    logger = structlog.get_logger(__name__)
    logger.info("Logging configured", 
               level=level,
               log_file=log_file,
               json_logs=enable_json_logs)


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a structured logger instance.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


class LoggingMixin:
    """
    Mixin class to add logging capabilities to other classes.
    """
    
    @property
    def logger(self) -> structlog.BoundLogger:
        """Get logger for this class."""
        return structlog.get_logger(self.__class__.__name__)


def log_function_call(func):
    """
    Decorator to log function calls with parameters and results.
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function
    """
    def wrapper(*args, **kwargs):
        logger = structlog.get_logger(func.__module__)
        
        # Log function entry
        logger.debug("Function called", 
                    function=func.__name__,
                    args=args,
                    kwargs=kwargs)
        
        try:
            result = func(*args, **kwargs)
            
            # Log successful completion
            logger.debug("Function completed", 
                        function=func.__name__,
                        result_type=type(result).__name__)
            
            return result
            
        except Exception as e:
            # Log exception
            logger.error("Function failed", 
                        function=func.__name__,
                        error=str(e),
                        exc_info=True)
            raise
    
    return wrapper


def log_execution_time(func):
    """
    Decorator to log function execution time.
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function
    """
    def wrapper(*args, **kwargs):
        import time
        
        logger = structlog.get_logger(func.__module__)
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            logger.info("Function execution completed", 
                       function=func.__name__,
                       execution_time_seconds=round(execution_time, 3))
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            logger.error("Function execution failed", 
                        function=func.__name__,
                        execution_time_seconds=round(execution_time, 3),
                        error=str(e))
            raise
    
    return wrapper


# Context manager for logging operations
class LoggedOperation:
    """
    Context manager for logging operations with start/end messages.
    """
    
    def __init__(self, operation_name: str, logger: structlog.BoundLogger = None, **context):
        """
        Initialize logged operation.
        
        Args:
            operation_name: Name of the operation
            logger: Logger instance (defaults to module logger)
            **context: Additional context to include in logs
        """
        self.operation_name = operation_name
        self.logger = logger or structlog.get_logger(__name__)
        self.context = context
        self.start_time = None
    
    def __enter__(self):
        """Enter the context."""
        import time
        self.start_time = time.time()
        
        self.logger.info("Operation started", 
                        operation=self.operation_name,
                        **self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context."""
        import time
        execution_time = time.time() - self.start_time if self.start_time else 0
        
        if exc_type is None:
            self.logger.info("Operation completed successfully", 
                           operation=self.operation_name,
                           execution_time_seconds=round(execution_time, 3),
                           **self.context)
        else:
            self.logger.error("Operation failed", 
                            operation=self.operation_name,
                            execution_time_seconds=round(execution_time, 3),
                            error=str(exc_val),
                            **self.context)
        
        return False  # Don't suppress exceptions


# Error tracking utilities
class ErrorTracker:
    """
    Utility class to track and log errors across the application.
    """
    
    def __init__(self):
        """Initialize error tracker."""
        self.errors = []
        self.logger = structlog.get_logger(self.__class__.__name__)
    
    def log_error(self, error: Exception, context: dict = None):
        """
        Log and track an error.
        
        Args:
            error: Exception that occurred
            context: Additional context information
        """
        error_info = {
            'timestamp': datetime.utcnow().isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context or {}
        }
        
        self.errors.append(error_info)
        
        self.logger.error("Error tracked", 
                         error_type=error_info['error_type'],
                         error_message=error_info['error_message'],
                         context=error_info['context'])
    
    def get_error_summary(self) -> dict:
        """
        Get summary of tracked errors.
        
        Returns:
            Dictionary with error summary
        """
        if not self.errors:
            return {'total_errors': 0, 'error_types': {}}
        
        error_types = {}
        for error in self.errors:
            error_type = error['error_type']
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        return {
            'total_errors': len(self.errors),
            'error_types': error_types,
            'recent_errors': self.errors[-5:] if len(self.errors) > 5 else self.errors
        }
    
    def clear_errors(self):
        """Clear tracked errors."""
        self.errors.clear()
        self.logger.info("Error tracker cleared")


# Global error tracker instance
_error_tracker = ErrorTracker()

def get_error_tracker() -> ErrorTracker:
    """
    Get global error tracker instance.
    
    Returns:
        ErrorTracker: Global error tracker
    """
    return _error_tracker