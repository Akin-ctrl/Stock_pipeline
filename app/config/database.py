"""
Database connection and session management.

Follows reference.py principles:
- Clean interface
- Resource management
- Type safety
"""

from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from app.config.settings import get_settings
from app.utils.logger import get_logger
from app.utils.exceptions import ConnectionError as DBConnectionError

logger = get_logger("database")


class DatabaseManager:
    """
    Manages database connections and sessions.
    
    Provides engine creation, session management, and connection pooling.
    Follows singleton pattern for engine reuse.
    
    Attributes:
        _engine: SQLAlchemy engine instance
        _session_factory: Session factory
    
    Example:
        >>> db = DatabaseManager()
        >>> with db.get_session() as session:
        >>>     stocks = session.query(DimStock).all()
    """
    
    def __init__(self):
        """Initialize database manager with settings."""
        self._settings = get_settings()
        self._engine: Engine = None  # type: ignore
        self._session_factory: sessionmaker = None  # type: ignore
        self._initialize()
    
    def _initialize(self) -> None:
        """Create engine and session factory."""
        try:
            connection_string = self._settings.database.connection_string
            
            self._engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=self._settings.database.pool_size,
                max_overflow=self._settings.database.max_overflow,
                pool_pre_ping=True,  # Verify connections before using
                echo=self._settings.debug  # Log SQL in debug mode
            )
            
            # Configure connection events
            @event.listens_for(self._engine, "connect")
            def receive_connect(dbapi_conn, connection_record):
                logger.debug("Database connection established")
            
            @event.listens_for(self._engine, "close")
            def receive_close(dbapi_conn, connection_record):
                logger.debug("Database connection closed")
            
            self._session_factory = sessionmaker(
                bind=self._engine,
                autocommit=False,
                autoflush=False
            )
            
            logger.info(
                "Database manager initialized",
                extra={
                    "host": self._settings.database.host,
                    "database": self._settings.database.database,
                    "pool_size": self._settings.database.pool_size
                }
            )
        
        except Exception as e:
            logger.critical("Failed to initialize database", error=e)
            raise DBConnectionError(
                "Database initialization failed",
                details={"error": str(e)}
            ) from e
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope for database operations.
        
        Yields:
            SQLAlchemy session
        
        Raises:
            DatabaseError: If session operations fail
        
        Example:
            >>> db = DatabaseManager()
            >>> with db.get_session() as session:
            >>>     stock = session.query(DimStock).first()
            >>>     print(stock.company_name)
        """
        session = self._session_factory()
        try:
            yield session
            session.commit()
            logger.debug("Session committed successfully")
        except Exception as e:
            session.rollback()
            logger.error("Session rollback due to error", error=e)
            raise
        finally:
            session.close()
    
    @property
    def engine(self) -> Engine:
        """Get SQLAlchemy engine."""
        return self._engine
    
    def health_check(self) -> bool:
        """
        Check database connectivity.
        
        Returns:
            True if database is reachable, False otherwise
        """
        try:
            from sqlalchemy import text
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database health check passed")
            return True
        except Exception as e:
            logger.error("Database health check failed", error=e)
            return False
    
    def dispose(self) -> None:
        """Close all database connections."""
        if self._engine:
            self._engine.dispose()
            logger.info("Database connections disposed")


# Global database manager instance
_db_manager: DatabaseManager = None  # type: ignore


def get_db() -> DatabaseManager:
    """
    Get global database manager instance (singleton).
    
    Returns:
        DatabaseManager instance
    
    Example:
        >>> from app.config.database import get_db
        >>> db = get_db()
        >>> with db.get_session() as session:
        >>>     # perform database operations
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def get_session() -> Session:
    """
    Get a new database session (not a context manager).
    
    WARNING: Caller is responsible for closing the session.
    Prefer using get_db().get_session() context manager when possible.
    
    Returns:
        SQLAlchemy Session
        
    Example:
        >>> from app.config.database import get_session
        >>> session = get_session()
        >>> try:
        >>>     stock = session.query(Stock).first()
        >>> finally:
        >>>     session.close()
    """
    db = get_db()
    return db._session_factory()
