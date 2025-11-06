"""
Logging Utilities

Centralized logging configuration for the data ingestion pipeline.
"""

import os
import logging
import logging.handlers
from datetime import datetime
from typing import Optional


def get_logger(name: str, log_level: str = "INFO", 
               log_file: Optional[str] = None, 
               log_dir: str = "logs") -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically __name__).
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Optional log file name. If None, uses the logger name.
        log_dir: Directory to store log files.
        
    Returns:
        Configured logger instance.
    """
    
    # Create logger
    logger = logging.getLogger(name)
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    # Set log level
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(numeric_level)
    
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file is None:
        log_file = f"{name.replace('.', '_')}.log"
    
    log_file_path = os.path.join(log_dir, log_file)
    
    # Use rotating file handler to prevent log files from growing too large
    file_handler = logging.handlers.RotatingFileHandler(
        log_file_path,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    
    # Error file handler (separate file for errors)
    error_log_file = log_file.replace('.log', '_errors.log')
    error_file_path = os.path.join(log_dir, error_log_file)
    
    error_handler = logging.handlers.RotatingFileHandler(
        error_file_path,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    logger.addHandler(error_handler)
    
    return logger


def setup_pipeline_logging(log_level: str = "INFO", log_dir: str = "logs"):
    """
    Set up logging configuration for the entire pipeline.
    
    Args:
        log_level: Global logging level.
        log_dir: Directory to store log files.
    """
    
    # Create log directory
    os.makedirs(log_dir, exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Suppress verbose logging from third-party libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('pymongo').setLevel(logging.WARNING)
    logging.getLogger('kafka').setLevel(logging.WARNING)
    logging.getLogger('pyspark').setLevel(logging.WARNING)
    logging.getLogger('py4j').setLevel(logging.WARNING)
    
    # Create main pipeline log file
    main_log_file = os.path.join(log_dir, 'data_pipeline.log')
    main_handler = logging.handlers.RotatingFileHandler(
        main_log_file,
        maxBytes=50 * 1024 * 1024,  # 50 MB
        backupCount=10
    )
    
    main_formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    main_handler.setFormatter(main_formatter)
    
    # Add handler to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(main_handler)


class PipelineLogger:
    """
    Context manager for pipeline logging with automatic error handling.
    """
    
    def __init__(self, logger: logging.Logger, operation: str):
        """
        Initialize pipeline logger context.
        
        Args:
            logger: Logger instance to use.
            operation: Description of the operation being performed.
        """
        self.logger = logger
        self.operation = operation
        self.start_time = None
    
    def __enter__(self):
        """Enter the context and log operation start."""
        self.start_time = datetime.now()
        self.logger.info(f"Starting operation: {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context and log operation completion or error."""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        if exc_type is None:
            self.logger.info(f"Completed operation: {self.operation} (Duration: {duration:.2f}s)")
        else:
            self.logger.error(f"Failed operation: {self.operation} (Duration: {duration:.2f}s) - {exc_type.__name__}: {exc_val}")
        
        # Don't suppress exceptions
        return False


def log_function_call(logger: logging.Logger):
    """
    Decorator to log function calls with parameters and execution time.
    
    Args:
        logger: Logger instance to use.
        
    Returns:
        Decorator function.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            func_name = func.__name__
            
            # Log function call (be careful not to log sensitive data)
            logger.debug(f"Calling function: {func_name}")
            
            try:
                result = func(*args, **kwargs)
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                logger.debug(f"Function {func_name} completed successfully (Duration: {duration:.2f}s)")
                return result
                
            except Exception as e:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                logger.error(f"Function {func_name} failed (Duration: {duration:.2f}s) - {type(e).__name__}: {str(e)}")
                raise
        
        return wrapper
    return decorator


def create_log_summary(log_dir: str = "logs") -> dict:
    """
    Create a summary of log files and their sizes.
    
    Args:
        log_dir: Directory containing log files.
        
    Returns:
        Dictionary with log file information.
    """
    summary = {
        "log_directory": log_dir,
        "total_files": 0,
        "total_size_mb": 0,
        "files": []
    }
    
    if not os.path.exists(log_dir):
        return summary
    
    for filename in os.listdir(log_dir):
        if filename.endswith('.log'):
            file_path = os.path.join(log_dir, filename)
            file_size = os.path.getsize(file_path)
            file_size_mb = file_size / (1024 * 1024)
            
            summary["files"].append({
                "name": filename,
                "size_mb": round(file_size_mb, 2),
                "modified": datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
            })
            
            summary["total_size_mb"] += file_size_mb
    
    summary["total_files"] = len(summary["files"])
    summary["total_size_mb"] = round(summary["total_size_mb"], 2)
    
    return summary


if __name__ == "__main__":
    # Test logging setup
    setup_pipeline_logging()
    
    # Test logger creation
    logger = get_logger(__name__)
    
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    # Test context manager
    with PipelineLogger(logger, "test operation"):
        logger.info("Performing test operation")
        # Simulate some work
        import time
        time.sleep(1)
    
    # Test decorator
    @log_function_call(logger)
    def test_function(x, y):
        return x + y
    
    result = test_function(1, 2)
    logger.info(f"Test function result: {result}")
    
    # Create log summary
    summary = create_log_summary()
    logger.info(f"Log summary: {summary}")
    
    print("Logging test completed. Check the logs directory for output files.")