"""
Structured logging utility for the Stock Pipeline system.

Follows reference.py principles:
- Clean interface
- Type safety
- Easy to use
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Dict
import json


class StructuredLogger:
    """
    Provides structured JSON logging with correlation IDs.
    
    Supports multiple log levels and automatic metadata enrichment.
    Useful for distributed tracing and log aggregation.
    
    Attributes:
        _logger: Internal Python logger instance
        _correlation_id: Optional ID for request tracing
    
    Example:
        >>> logger = StructuredLogger("pipeline", correlation_id="abc-123")
        >>> logger.info("Processing started", extra={"stock_count": 156})
        {"timestamp": "2025-12-06T15:00:00", "level": "INFO", ...}
    """
    
    def __init__(
        self,
        name: str,
        level: int = logging.INFO,
        log_file: Optional[Path] = None,
        correlation_id: Optional[str] = None
    ):
        """
        Initialize structured logger.
        
        Args:
            name: Logger name (e.g., 'pipeline', 'ingestion')
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Optional file path for log output
            correlation_id: Optional ID for tracing related log entries
        """
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)
        self._correlation_id = correlation_id
        
        # Avoid duplicate handlers
        if not self._logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_handler.setFormatter(self._get_formatter())
            self._logger.addHandler(console_handler)
            
            # File handler (optional)
            if log_file:
                log_file.parent.mkdir(parents=True, exist_ok=True)
                file_handler = logging.FileHandler(log_file)
                file_handler.setLevel(level)
                file_handler.setFormatter(self._get_formatter())
                self._logger.addHandler(file_handler)
    
    def _get_formatter(self) -> logging.Formatter:
        """Create JSON formatter for structured logs."""
        return logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
            '"logger": "%(name)s", "message": "%(message)s"}'
        )
    
    def _enrich_metadata(self, extra: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Add correlation ID and timestamp to log metadata."""
        metadata = extra or {}
        if self._correlation_id:
            metadata["correlation_id"] = self._correlation_id
        metadata["timestamp_ms"] = datetime.now().timestamp() * 1000
        return metadata
    
    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Log debug message with optional metadata."""
        enriched = self._enrich_metadata(extra)
        self._logger.debug(f"{message} | {json.dumps(enriched)}")
    
    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Log info message with optional metadata."""
        enriched = self._enrich_metadata(extra)
        self._logger.info(f"{message} | {json.dumps(enriched)}")
    
    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Log warning message with optional metadata."""
        enriched = self._enrich_metadata(extra)
        self._logger.warning(f"{message} | {json.dumps(enriched)}")
    
    def error(
        self,
        message: str,
        error: Optional[Exception] = None,
        extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log error message with optional exception and metadata.
        
        Args:
            message: Error description
            error: Exception object (if available)
            extra: Additional context
        """
        enriched = self._enrich_metadata(extra)
        if error:
            enriched["error_type"] = type(error).__name__
            enriched["error_message"] = str(error)
        self._logger.error(f"{message} | {json.dumps(enriched)}")
    
    def critical(
        self,
        message: str,
        error: Optional[Exception] = None,
        extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log critical message with optional exception and metadata."""
        enriched = self._enrich_metadata(extra)
        if error:
            enriched["error_type"] = type(error).__name__
            enriched["error_message"] = str(error)
        self._logger.critical(f"{message} | {json.dumps(enriched)}")


def get_logger(
    name: str,
    level: int = logging.INFO,
    correlation_id: Optional[str] = None
) -> StructuredLogger:
    """
    Factory function to create logger instances.
    
    Args:
        name: Logger name
        level: Log level
        correlation_id: Optional correlation ID
    
    Returns:
        Configured StructuredLogger instance
    
    Example:
        >>> logger = get_logger("ingestion")
        >>> logger.info("Started NGX ingestion")
    """
    # Use relative path from project root (works in both host and container)
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_file = log_dir / f"{name}.log"
    
    return StructuredLogger(
        name=name,
        level=level,
        log_file=log_file,
        correlation_id=correlation_id
    )
