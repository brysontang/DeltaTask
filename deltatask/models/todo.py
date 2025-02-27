from sqlalchemy import Column, String, Text, DateTime, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from typing import Dict, Any

from deltatask.models.base import Base, todo_tags

class Todo(Base):
    """Database model for todo items."""
    __tablename__ = 'todos'
    
    id = Column(String(36), primary_key=True)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    created = Column(DateTime, default=func.now())
    updated = Column(DateTime, default=func.now(), onupdate=func.now())
    deadline = Column(String(50), nullable=True)
    urgency = Column(Integer, default=1)
    effort = Column(Integer, default=1)
    completed = Column(Boolean, default=False)
    parent_id = Column(String(36), ForeignKey('todos.id'), nullable=True)
    
    # Relationships
    subtasks = relationship("Todo", backref="parent", remote_side=[id])
    tags = relationship("Tag", secondary=todo_tags, back_populates="todos")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Todo object to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "created": self.created.isoformat() if self.created else None,
            "updated": self.updated.isoformat() if self.updated else None,
            "deadline": self.deadline,
            "urgency": self.urgency,
            "effort": self.effort,
            "completed": self.completed,
            "parent_id": self.parent_id,
            "tags": [tag.name for tag in self.tags]
        }