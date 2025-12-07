"""
Utility decorators for the Stock Pipeline system.

Follows reference.py principles:
- Reusable functionality
- Clean abstractions
- Type safety
"""

import time
from functools import wraps
from typing import Any, Callable, Optional, TypeVar
import requests
from requests.adapters import HTTPAdapter, Retry

from app.utils.logger import get_logger
from app.utils.exceptions import DataFetchError, ProcessingError

F = TypeVar('F', bound=Callable[..., Any])

logger = get_logger("decorators")


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
) -> Callable[[F], F]:
    """
    Retry decorator with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries (seconds)
        backoff: Multiplier for delay on each retry
        exceptions: Tuple of exceptions to catch
    
    Returns:
        Decorated function
    
    Example:
        >>> @retry(max_attempts=3, delay=2.0)
        >>> def fetch_data():
        >>>     return requests.get("https://api.example.com")
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    logger.debug(
                        f"Attempting {func.__name__} (attempt {attempt}/{max_attempts})"
                    )
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(
                            f"{func.__name__} succeeded on attempt {attempt}",
                            extra={"attempts": attempt}
                        )
                    return result
                
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"{func.__name__} failed, retrying in {current_delay}s",
                            extra={
                                "attempt": attempt,
                                "error": str(e),
                                "delay": current_delay
                            }
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts",
                            error=e,
                            extra={"attempts": max_attempts}
                        )
            
            # All retries exhausted
            raise last_exception
        
        return wrapper  # type: ignore
    
    return decorator


def timing(func: F) -> F:
    """
    Decorator to measure and log function execution time.
    
    Args:
        func: Function to time
    
    Returns:
        Decorated function
    
    Example:
        >>> @timing
        >>> def process_data(df):
        >>>     return df.transform()
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            
            logger.info(
                f"{func.__name__} completed",
                extra={
                    "execution_time_ms": round(elapsed * 1000, 2),
                    "function": func.__name__
                }
            )
            return result
        
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(
                f"{func.__name__} failed",
                error=e,
                extra={
                    "execution_time_ms": round(elapsed * 1000, 2),
                    "function": func.__name__
                }
            )
            raise
    
    return wrapper  # type: ignore


def validate_not_none(*param_names: str) -> Callable[[F], F]:
    """
    Decorator to validate that specified parameters are not None.
    
    Args:
        param_names: Names of parameters to validate
    
    Returns:
        Decorated function
    
    Raises:
        ValueError: If any specified parameter is None
    
    Example:
        >>> @validate_not_none('stock_code', 'price')
        >>> def save_price(stock_code, price):
        >>>     pass
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Get function signature
            import inspect
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Check specified parameters
            for param_name in param_names:
                if param_name in bound_args.arguments:
                    value = bound_args.arguments[param_name]
                    if value is None:
                        raise ValueError(
                            f"Parameter '{param_name}' cannot be None in {func.__name__}"
                        )
            
            return func(*args, **kwargs)
        
        return wrapper  # type: ignore
    
    return decorator


def create_http_session(
    max_retries: int = 5,
    backoff_factor: float = 1.0,
    status_forcelist: tuple = (500, 502, 503, 504)
) -> requests.Session:
    """
    Create HTTP session with retry logic.
    
    Args:
        max_retries: Maximum number of retries
        backoff_factor: Backoff multiplier
        status_forcelist: HTTP status codes to retry
    
    Returns:
        Configured requests.Session
    
    Example:
        >>> session = create_http_session()
        >>> response = session.get("https://api.example.com")
    """
    session = requests.Session()
    
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST", "PUT"]
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session
