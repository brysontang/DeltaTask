import uuid
import logging
from typing import List, Dict, Any, Optional, Set, Tuple
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from deltatask.models import Base, Todo, Tag

# Get logger
logger = logging.getLogger("DeltaTask")

class DeltaTaskRepository:
    """Repository for database operations on tasks and tags."""
    
    def __init__(self, db_url: str = "sqlite:///deltatask.db"):
        """Initialize the repository with a database connection."""
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)
    
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
            logger.debug("Database transaction committed successfully")
        except Exception as e:
            logger.error(f"Database transaction error: {e}", exc_info=True)
            session.rollback()
            logger.info("Database transaction rolled back")
            raise
        finally:
            session.close()
    
    def add_todo(self, todo_data: Dict[str, Any]) -> str:
        """Add a new todo to the database."""
        with self.session_scope() as session:
            # Generate a UUID if not provided
            todo_id = todo_data.get('id', str(uuid.uuid4()))
            
            # Create the Todo object
            todo = Todo(
                id=todo_id,
                title=todo_data['title'],
                description=todo_data.get('description', ''),
                deadline=todo_data.get('deadline'),
                urgency=todo_data.get('urgency', 1),
                effort=todo_data.get('effort', 1),
                parent_id=todo_data.get('parent_id')
            )
            
            # Handle tags
            if 'tags' in todo_data and todo_data['tags']:
                for tag_name in todo_data['tags']:
                    # Check if tag exists
                    tag = session.query(Tag).filter(Tag.name == tag_name).first()
                    if not tag:
                        # Create new tag
                        tag = Tag(id=str(uuid.uuid4()), name=tag_name)
                        session.add(tag)
                    todo.tags.append(tag)
            
            session.add(todo)
            return todo_id
    
    def get_todos(self, include_completed: bool = False, 
                 parent_id: Optional[str] = None,
                 tags: List[str] = None) -> List[Dict[str, Any]]:
        """Get todos with optional filtering."""
        with self.session_scope() as session:
            query = session.query(Todo)
            
            # Apply filters
            if not include_completed:
                query = query.filter(Todo.completed == False)
                
            if parent_id is not None:
                query = query.filter(Todo.parent_id == parent_id)
                
            if tags:
                query = query.join(Todo.tags).filter(Tag.name.in_(tags)).distinct()
            
            todos = query.all()
            
            # Convert to dicts
            result = []
            for todo in todos:
                todo_dict = todo.to_dict()
                # We don't need to recursively get subtasks here as we'll do it in the service layer
                result.append(todo_dict)
            
            # Sort by deadline, urgency, and effort
            result.sort(key=lambda x: (
                x.get('deadline') is None,  # None deadlines come last
                x.get('deadline', '9999-12-31'),  # Then sort by deadline
                -x.get('urgency', 1),  # Then by urgency (descending)
                x.get('effort', 999)  # Then by effort (ascending)
            ))
            
            return result
    
    def get_todo_by_id(self, todo_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific todo by ID."""
        with self.session_scope() as session:
            todo = session.query(Todo).filter(Todo.id == todo_id).first()
            if not todo:
                return None
            return todo.to_dict()
    
    def update_todo(self, todo_id: str, updates: Dict[str, Any]) -> bool:
        """Update a todo with new values."""
        with self.session_scope() as session:
            todo = session.query(Todo).filter(Todo.id == todo_id).first()
            if not todo:
                return False
            
            # Update simple fields
            if 'title' in updates:
                todo.title = updates['title']
            if 'description' in updates:
                todo.description = updates['description']
            if 'deadline' in updates:
                todo.deadline = updates['deadline']
            if 'urgency' in updates:
                todo.urgency = updates['urgency']
            if 'effort' in updates:
                todo.effort = updates['effort']
            if 'completed' in updates:
                todo.completed = updates['completed']
            if 'parent_id' in updates:
                todo.parent_id = updates['parent_id']
            
            # Handle tags update
            if 'tags' in updates:
                # Clear existing tags
                todo.tags = []
                
                # Add new tags
                for tag_name in updates['tags']:
                    tag = session.query(Tag).filter(Tag.name == tag_name).first()
                    if not tag:
                        tag = Tag(id=str(uuid.uuid4()), name=tag_name)
                        session.add(tag)
                    todo.tags.append(tag)
            
            return True
    
    def delete_todo(self, todo_id: str, delete_subtasks: bool = True) -> bool:
        """Delete a todo and optionally its subtasks."""
        with self.session_scope() as session:
            todo = session.query(Todo).filter(Todo.id == todo_id).first()
            if not todo:
                return False
            
            if delete_subtasks:
                # Recursively delete all subtasks
                subtasks = session.query(Todo).filter(Todo.parent_id == todo_id).all()
                for subtask in subtasks:
                    self.delete_todo(subtask.id, True)
            else:
                # Update subtasks to remove parent reference
                session.query(Todo).filter(Todo.parent_id == todo_id).update({"parent_id": None})
            
            # Delete the todo
            session.delete(todo)
            return True
    
    def search_todos(self, query: str) -> List[Dict[str, Any]]:
        """Search todos by title, description, or tags."""
        with self.session_scope() as session:
            # Search todos with title or description containing the query
            todos = session.query(Todo).filter(
                (Todo.title.contains(query)) |
                (Todo.description.contains(query))
            ).all()
            
            # Also search in tags
            tag_todos = session.query(Todo).join(Todo.tags).filter(Tag.name.contains(query)).all()
            
            # Combine results and remove duplicates
            all_todos = set([todo.id for todo in todos] + [todo.id for todo in tag_todos])
            
            # Fetch full todos with their relationships
            results = []
            for todo_id in all_todos:
                todo = session.query(Todo).filter(Todo.id == todo_id).first()
                if todo:
                    results.append(todo.to_dict())
            
            # Sort by deadline, urgency, and effort
            results.sort(key=lambda x: (
                x.get('deadline') is None,
                x.get('deadline', '9999-12-31'),
                -x.get('urgency', 1),
                x.get('effort', 999)
            ))
            
            return results
    
    def get_all_tags(self) -> List[str]:
        """Get all unique tag names."""
        with self.session_scope() as session:
            tags = session.query(Tag.name).all()
            return [tag[0] for tag in tags]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get task statistics."""
        with self.session_scope() as session:
            total = session.query(Todo).count()
            completed = session.query(Todo).filter(Todo.completed == True).count()
            
            # Count by urgency
            by_urgency = {}
            for urgency in range(1, 6):
                count = session.query(Todo).filter(Todo.completed == False, Todo.urgency == urgency).count()
                by_urgency[urgency] = count
            
            # Count upcoming deadlines
            from datetime import datetime
            today = datetime.now().date().isoformat()
            week_later = today.replace(today[:8], str(int(today[8:]) + 7))
            upcoming_deadlines = session.query(Todo).filter(
                Todo.completed == False,
                Todo.deadline.between(today, week_later)
            ).count()
            
            return {
                "total": total,
                "completed": completed,
                "completion_rate": (completed / total * 100) if total > 0 else 0,
                "by_urgency": by_urgency,
                "upcoming_deadlines": upcoming_deadlines
            }