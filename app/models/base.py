"""
SQLAlchemy declarative base and common utilities.

Features:
- Type safety
- Clear abstractions
- Reusable components
"""

from datetime import datetime
from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base
from typing import Any, Dict

# Naming convention for constraints (improves Alembic migrations)
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

metadata = MetaData(naming_convention=convention)
Base = declarative_base(metadata=metadata)


class TimestampMixin:
    """
    Mixin for created_at and updated_at timestamps.
    
    Automatically tracks record creation and modification times.
    
    Example:
        >>> class MyModel(Base, TimestampMixin):
        >>>     __tablename__ = 'my_table'
        >>>     id = Column(Integer, primary_key=True)
    """
    
    @property
    def created_at_str(self) -> str:
        """Get created_at as ISO format string."""
        if hasattr(self, 'created_at') and self.created_at:
            return self.created_at.isoformat()
        return ""
    
    @property
    def updated_at_str(self) -> str:
        """Get updated_at as ISO format string."""
        if hasattr(self, 'updated_at') and self.updated_at:
            return self.updated_at.isoformat()
        return ""


class BaseModel(Base):
    """
    Abstract base model with common functionality.
    
    Provides to_dict() method and proper __repr__.
    All models should inherit from this.
    """
    __abstract__ = True
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert model instance to dictionary.
        
        Returns:
            Dictionary representation of the model
        
        Example:
            >>> stock = DimStock(stock_code='DANGCEM')
            >>> data = stock.to_dict()
            >>> print(data['stock_code'])
        """
        result = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            # Convert datetime to ISO string
            if isinstance(value, datetime):
                value = value.isoformat()
            result[column.name] = value
        return result
    
    def __repr__(self) -> str:
        """String representation showing all attributes."""
        attrs = []
        for column in self.__table__.columns:
            value = getattr(self, column.name, None)
            if value is not None:
                attrs.append(f"{column.name}={value!r}")
        return f"{self.__class__.__name__}({', '.join(attrs)})"
