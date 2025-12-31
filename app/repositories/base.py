"""
Base repository class with common database operations.

Provides generic CRUD operations that can be inherited by specific repositories.
Uses SQLAlchemy sessions for database transactions.
"""

from typing import Generic, TypeVar, Type, Optional, List, Any, Dict
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.models import Base
from app.utils import get_logger
from app.utils.exceptions import (
    DatabaseError,
    RecordNotFoundError,
    DuplicateDataError,
)

# Type variable for generic repository
T = TypeVar('T', bound=Base)

logger = get_logger(__name__)


class BaseRepository(Generic[T]):
    """
    Generic repository for database operations.
    
    Provides common CRUD methods that can be used by all entity repositories.
    Uses generics to maintain type safety across different entity types.
    
    Example:
        class UserRepository(BaseRepository[User]):
            def __init__(self, session: Session):
                super().__init__(User, session)
    """
    
    def __init__(self, model: Type[T], session: Session):
        """
        Initialize repository with model class and database session.
        
        Args:
            model: SQLAlchemy model class
            session: Active database session
        """
        self.model = model
        self.session = session
        self.logger = logger
    
    def get_by_id(self, id_value: int) -> Optional[T]:
        """
        Retrieve a single record by primary key.
        
        Args:
            id_value: Primary key value
            
        Returns:
            Model instance or None if not found
            
        Raises:
            DatabaseError: If database operation fails
        """
        try:
            return self.session.get(self.model, id_value)
        except SQLAlchemyError as e:
            self.logger.error(
                f"Failed to get {self.model.__name__} by id",
                extra={"id": id_value, "error": str(e)}
            )
            raise DatabaseError(f"Failed to retrieve record: {str(e)}") from e
    
    def get_all(self, limit: Optional[int] = None, offset: int = 0) -> List[T]:
        """
        Retrieve all records with optional pagination.
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of model instances
            
        Raises:
            DatabaseError: If database operation fails
        """
        try:
            query = self.session.query(self.model)
            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)
            return query.all()
        except SQLAlchemyError as e:
            self.logger.error(
                f"Failed to get all {self.model.__name__}",
                extra={"error": str(e)}
            )
            raise DatabaseError(f"Failed to retrieve records: {str(e)}") from e
    
    def create(self, **kwargs) -> T:
        """
        Create a new record.
        
        Args:
            **kwargs: Field values for the new record
            
        Returns:
            Created model instance
            
        Raises:
            DuplicateDataError: If unique constraint is violated
            DatabaseError: If database operation fails
        """
        try:
            instance = self.model(**kwargs)
            self.session.add(instance)
            self.session.flush()  # Get ID without committing
            self.logger.info(
                f"Created {self.model.__name__}",
                extra={"kwargs": kwargs}
            )
            return instance
        except IntegrityError as e:
            self.session.rollback()
            self.logger.warning(
                f"Duplicate {self.model.__name__}",
                extra={"kwargs": kwargs, "error": str(e)}
            )
            raise DuplicateDataError(
                f"Record already exists: {str(e)}"
            ) from e
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(
                f"Failed to create {self.model.__name__}",
                extra={"kwargs": kwargs, "error": str(e)}
            )
            raise DatabaseError(f"Failed to create record: {str(e)}") from e
    
    def update(self, instance: T, **kwargs) -> T:
        """
        Update an existing record.
        
        Args:
            instance: Model instance to update
            **kwargs: Fields to update
            
        Returns:
            Updated model instance
            
        Raises:
            DatabaseError: If database operation fails
        """
        try:
            for key, value in kwargs.items():
                if hasattr(instance, key):
                    setattr(instance, key, value)
            self.session.flush()
            self.logger.info(
                f"Updated {self.model.__name__}",
                extra={"kwargs": kwargs}
            )
            return instance
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(
                f"Failed to update {self.model.__name__}",
                extra={"kwargs": kwargs, "error": str(e)}
            )
            raise DatabaseError(f"Failed to update record: {str(e)}") from e
    
    def delete(self, instance: T) -> None:
        """
        Delete a record.
        
        Args:
            instance: Model instance to delete
            
        Raises:
            DatabaseError: If database operation fails
        """
        try:
            self.session.delete(instance)
            self.session.flush()
            self.logger.info(
                f"Deleted {self.model.__name__}",
                extra={"instance": str(instance)}
            )
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(
                f"Failed to delete {self.model.__name__}",
                extra={"error": str(e)}
            )
            raise DatabaseError(f"Failed to delete record: {str(e)}") from e
    
    def count(self, **filters) -> int:
        """
        Count records matching filters.
        
        Args:
            **filters: Field filters (field_name=value)
            
        Returns:
            Number of matching records
            
        Raises:
            DatabaseError: If database operation fails
        """
        try:
            query = self.session.query(self.model)
            for key, value in filters.items():
                if hasattr(self.model, key):
                    query = query.filter(getattr(self.model, key) == value)
            return query.count()
        except SQLAlchemyError as e:
            self.logger.error(
                f"Failed to count {self.model.__name__}",
                extra={"filters": filters, "error": str(e)}
            )
            raise DatabaseError(f"Failed to count records: {str(e)}") from e
    
    def exists(self, **filters) -> bool:
        """
        Check if any records match filters.
        
        Args:
            **filters: Field filters (field_name=value)
            
        Returns:
            True if at least one record exists
        """
        return self.count(**filters) > 0
    
    def bulk_insert(self, instances: List[T]) -> None:
        """
        Insert multiple records in bulk.
        
        Args:
            instances: List of model instances to insert
            
        Raises:
            DatabaseError: If database operation fails
        """
        try:
            self.session.bulk_save_objects(instances)
            self.session.flush()
            self.logger.info(
                f"Bulk inserted {len(instances)} {self.model.__name__} records"
            )
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(
                f"Failed to bulk insert {self.model.__name__}",
                extra={"count": len(instances), "error": str(e)}
            )
            raise DatabaseError(f"Failed to bulk insert: {str(e)}") from e
    
    def commit(self) -> None:
        """
        Commit the current transaction.
        
        Raises:
            DatabaseError: If commit fails
        """
        try:
            self.session.commit()
            self.logger.debug("Transaction committed")
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error("Failed to commit transaction", extra={"error": str(e)})
            raise DatabaseError(f"Failed to commit: {str(e)}") from e
    
    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.session.rollback()
        self.logger.debug("Transaction rolled back")
