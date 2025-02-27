from sqlalchemy import Column, String
from sqlalchemy.orm import relationship

from deltatask.models.base import Base, todo_tags

class Tag(Base):
    """Database model for tags."""
    __tablename__ = 'tags'
    
    id = Column(String(36), primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    
    # Relationships
    todos = relationship("Todo", secondary=todo_tags, back_populates="tags")