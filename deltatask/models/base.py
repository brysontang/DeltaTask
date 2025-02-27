from sqlalchemy import Column, ForeignKey, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base

# Database models
Base = declarative_base()

# Junction table for many-to-many relationship between todos and tags
todo_tags = Table(
    'todo_tags', 
    Base.metadata,
    Column('todo_id', String(36), ForeignKey('todos.id')),
    Column('tag_id', String(36), ForeignKey('tags.id'))
)