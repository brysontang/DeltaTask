import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import os

class TodoDatabase:
    """SQLite database manager for the todo application."""
    def __init__(self, db_path: str = "todo_app.db"):
        """Initialize the database connection and create tables if they don't exist."""
        self.db_path = db_path
        self.conn = self._create_connection()
        self._create_tables()
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a database connection to the SQLite database."""
        try:
            if not os.path.exists(os.path.dirname(self.db_path)) and os.path.dirname(self.db_path):
                os.makedirs(os.path.dirname(self.db_path))
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            return conn
        except sqlite3.Error as e:
            print(f"Database connection error: {e}")
            raise

    def _create_tables(self) -> None:
        """Create the necessary tables if they don't exist."""
        try:
            cursor = self.conn.cursor()
            
            # Create todos table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS todos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                deadline TEXT,
                urgency INTEGER NOT NULL,
                effort INTEGER NOT NULL,
                completed BOOLEAN NOT NULL DEFAULT 0,
                parent_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (parent_id) REFERENCES todos (id) ON DELETE CASCADE
            )
            ''')
            
            # Create tags table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            )
            ''')
            
            # Create todo_tags relationship table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS todo_tags (
                todo_id INTEGER,
                tag_id INTEGER,
                PRIMARY KEY (todo_id, tag_id),
                FOREIGN KEY (todo_id) REFERENCES todos (id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE CASCADE
            )
            ''')
            
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Table creation error: {e}")
            raise

    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()

class TodoManager:
    """Manager for handling todo operations using the database."""
    def __init__(self, db: TodoDatabase):
        """Initialize with a database instance."""
        self.db = db
    
    def create_todo(self, title: str, description: str = "", deadline: Optional[str] = None,
                   urgency: int = 1, effort: int = 1, parent_id: Optional[int] = None,
                   tags: List[str] = None) -> int:
        """
        Create a new todo item.
        
        Args:
            title: The title of the todo
            description: A detailed description
            deadline: ISO format date string (YYYY-MM-DD)
            urgency: Priority level (1-5, with 5 being highest)
            effort: Fibonacci number representing effort (1, 2, 3, 5, 8, 13, 21)
            parent_id: ID of parent todo if this is a subtask
            tags: List of tag names to associate with this todo
            
        Returns:
            The ID of the newly created todo
        """
        # Validate fibonacci sequence for effort
        valid_efforts = [1, 2, 3, 5, 8, 13, 21]
        if effort not in valid_efforts:
            raise ValueError(f"Effort must be a Fibonacci number from {valid_efforts}")
        
        # Validate urgency
        if not 1 <= urgency <= 5:
            raise ValueError("Urgency must be between 1 and 5")
            
        now = datetime.now().isoformat()
        
        try:
            cursor = self.db.conn.cursor()
            
            # Insert the todo
            cursor.execute('''
            INSERT INTO todos (title, description, deadline, urgency, effort, parent_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (title, description, deadline, urgency, effort, parent_id, now, now))
            
            todo_id = cursor.lastrowid
            
            # Handle tags if provided
            if tags:
                self._add_tags_to_todo(todo_id, tags)
                
            self.db.conn.commit()
            return todo_id
            
        except sqlite3.Error as e:
            self.db.conn.rollback()
            print(f"Error creating todo: {e}")
            raise
    
    def _add_tags_to_todo(self, todo_id: int, tags: List[str]) -> None:
        """Add tags to a todo, creating any tags that don't exist."""
        cursor = self.db.conn.cursor()
        
        for tag in tags:
            # Check if tag exists, create if it doesn't
            cursor.execute("SELECT id FROM tags WHERE name = ?", (tag,))
            result = cursor.fetchone()
            
            if result:
                tag_id = result['id']
            else:
                cursor.execute("INSERT INTO tags (name) VALUES (?)", (tag,))
                tag_id = cursor.lastrowid
            
            # Create relationship between todo and tag
            cursor.execute(
                "INSERT INTO todo_tags (todo_id, tag_id) VALUES (?, ?)",
                (todo_id, tag_id)
            )
    
    def get_todos(self, include_completed: bool = False, 
                  parent_id: Optional[int] = None,
                  tags: List[str] = None) -> List[Dict[str, Any]]:
        """
        Get todos with ordering by deadline, urgency, and effort.
        
        Args:
            include_completed: Whether to include completed todos
            parent_id: If provided, only return subtasks of this todo
            tags: Filter by these tags if provided
            
        Returns:
            List of todo dictionaries with their tags
        """
        try:
            # Build the base query
            query = """
            SELECT t.* FROM todos t
            """
            
            params = []
            where_clauses = []
            
            # Handle tag filtering
            if tags:
                tag_placeholders = ", ".join(["?"] * len(tags))
                query += f"""
                JOIN todo_tags tt ON t.id = tt.todo_id
                JOIN tags tg ON tt.tag_id = tg.id
                """
                where_clauses.append(f"tg.name IN ({tag_placeholders})")
                params.extend(tags)
            
            # Filter conditions
            if not include_completed:
                where_clauses.append("t.completed = 0")
            
            if parent_id is not None:
                where_clauses.append("t.parent_id = ?")
                params.append(parent_id)
            
            # Add WHERE clause if there are any conditions
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            
            # Add ordering
            query += """
            ORDER BY 
                CASE 
                    WHEN t.deadline IS NULL THEN 1 
                    ELSE 0 
                END,
                t.deadline ASC,
                t.urgency DESC,
                t.effort ASC
            """
            
            cursor = self.db.conn.cursor()
            cursor.execute(query, params)
            todos = [dict(row) for row in cursor.fetchall()]
            
            # Fetch tags for each todo
            for todo in todos:
                todo['tags'] = self._get_todo_tags(todo['id'])
                
                # Fetch subtasks if this is a parent task
                todo['subtasks'] = self.get_todos(include_completed, todo['id'])
                
            return todos
            
        except sqlite3.Error as e:
            print(f"Error retrieving todos: {e}")
            raise
    
    def _get_todo_tags(self, todo_id: int) -> List[str]:
        """Get all tags associated with a todo."""
        cursor = self.db.conn.cursor()
        cursor.execute("""
        SELECT t.name FROM tags t
        JOIN todo_tags tt ON t.id = tt.tag_id
        WHERE tt.todo_id = ?
        """, (todo_id,))
        return [row['name'] for row in cursor.fetchall()]
    
    def update_todo(self, todo_id: int, updates: Dict[str, Any]) -> bool:
        """
        Update a todo with new values.
        
        Args:
            todo_id: The ID of the todo to update
            updates: Dictionary of fields to update with their new values
            
        Returns:
            True if successful, False otherwise
        """
        valid_fields = {
            'title', 'description', 'deadline', 'urgency', 
            'effort', 'completed', 'parent_id'
        }
        
        # Filter out invalid fields
        filtered_updates = {k: v for k, v in updates.items() if k in valid_fields}
        
        if not filtered_updates:
            return False
            
        # Add updated_at timestamp
        filtered_updates['updated_at'] = datetime.now().isoformat()
        
        try:
            cursor = self.db.conn.cursor()
            
            # Build update query dynamically
            set_clause = ", ".join([f"{field} = ?" for field in filtered_updates])
            values = list(filtered_updates.values())
            values.append(todo_id)  # For the WHERE clause
            
            cursor.execute(
                f"UPDATE todos SET {set_clause} WHERE id = ?",
                values
            )
            
            # Handle tags if they were provided in updates
            if 'tags' in updates:
                # Remove existing tag relationships
                cursor.execute("DELETE FROM todo_tags WHERE todo_id = ?", (todo_id,))
                # Add new tags
                self._add_tags_to_todo(todo_id, updates['tags'])
            
            self.db.conn.commit()
            return cursor.rowcount > 0
            
        except sqlite3.Error as e:
            self.db.conn.rollback()
            print(f"Error updating todo: {e}")
            raise
    
    def delete_todo(self, todo_id: int, delete_subtasks: bool = True) -> bool:
        """
        Delete a todo and optionally its subtasks.
        
        Args:
            todo_id: The ID of the todo to delete
            delete_subtasks: Whether to delete all subtasks as well
            
        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.db.conn.cursor()
            
            if delete_subtasks:
                # SQLite will handle cascading deletes if configured in the schema
                cursor.execute("DELETE FROM todos WHERE id = ?", (todo_id,))
            else:
                # Set parent_id to NULL for all subtasks
                cursor.execute(
                    "UPDATE todos SET parent_id = NULL WHERE parent_id = ?", 
                    (todo_id,)
                )
                # Then delete the todo
                cursor.execute("DELETE FROM todos WHERE id = ?", (todo_id,))
            
            self.db.conn.commit()
            return cursor.rowcount > 0
            
        except sqlite3.Error as e:
            self.db.conn.rollback()
            print(f"Error deleting todo: {e}")
            raise
    
    def split_todo(self, todo_id: int, subtasks: List[Dict[str, Any]]) -> List[int]:
        """
        Split a larger task into smaller subtasks.
        
        Args:
            todo_id: The ID of the parent todo
            subtasks: List of dictionaries with subtask details
            
        Returns:
            List of newly created subtask IDs
        """
        subtask_ids = []
        
        try:
            # Check if parent todo exists
            cursor = self.db.conn.cursor()
            cursor.execute("SELECT id FROM todos WHERE id = ?", (todo_id,))
            if not cursor.fetchone():
                raise ValueError(f"Parent todo with ID {todo_id} does not exist")
            
            # Create each subtask
            for subtask in subtasks:
                subtask['parent_id'] = todo_id
                new_id = self.create_todo(**subtask)
                subtask_ids.append(new_id)
                
            return subtask_ids
            
        except sqlite3.Error as e:
            self.db.conn.rollback()
            print(f"Error splitting todo: {e}")
            raise

    def search_todos(self, query: str) -> List[Dict[str, Any]]:
        """
        Search todos by title, description, or tags.
        
        Args:
            query: Search term
            
        Returns:
            List of matching todos
        """
        try:
            search_term = f"%{query}%"
            
            cursor = self.db.conn.cursor()
            cursor.execute("""
            SELECT DISTINCT t.* FROM todos t
            LEFT JOIN todo_tags tt ON t.id = tt.todo_id
            LEFT JOIN tags tg ON tt.tag_id = tg.id
            WHERE 
                t.title LIKE ? OR 
                t.description LIKE ? OR
                tg.name LIKE ?
            ORDER BY t.deadline ASC, t.urgency DESC, t.effort ASC
            """, (search_term, search_term, search_term))
            
            todos = [dict(row) for row in cursor.fetchall()]
            
            # Fetch tags and subtasks for each todo
            for todo in todos:
                todo['tags'] = self._get_todo_tags(todo['id'])
                todo['subtasks'] = self.get_todos(include_completed=False, parent_id=todo['id'])
                
            return todos
            
        except sqlite3.Error as e:
            print(f"Error searching todos: {e}")
            raise

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about todos."""
        try:
            cursor = self.db.conn.cursor()
            
            # Total todos
            cursor.execute("SELECT COUNT(*) as total FROM todos")
            total = cursor.fetchone()['total']
            
            # Completed todos
            cursor.execute("SELECT COUNT(*) as completed FROM todos WHERE completed = 1")
            completed = cursor.fetchone()['completed']
            
            # Todos by urgency
            cursor.execute("""
            SELECT urgency, COUNT(*) as count 
            FROM todos 
            WHERE completed = 0
            GROUP BY urgency
            ORDER BY urgency DESC
            """)
            by_urgency = {row['urgency']: row['count'] for row in cursor.fetchall()}
            
            # Upcoming deadlines
            cursor.execute("""
            SELECT COUNT(*) as count
            FROM todos
            WHERE 
                completed = 0 AND 
                deadline IS NOT NULL AND 
                deadline <= date('now', '+7 day')
            """)
            upcoming_deadlines = cursor.fetchone()['count']
            
            return {
                "total": total,
                "completed": completed,
                "completion_rate": (completed / total * 100) if total > 0 else 0,
                "by_urgency": by_urgency,
                "upcoming_deadlines": upcoming_deadlines
            }
            
        except sqlite3.Error as e:
            print(f"Error getting stats: {e}")
            raise

class TodoService:
    """Service layer to handle business logic and expose API endpoints."""
    def __init__(self, db_path: str = "todo_app.db"):
        """Initialize with a database path."""
        self.db = TodoDatabase(db_path)
        self.manager = TodoManager(self.db)
    
    def __del__(self):
        """Clean up database connection when service is destroyed."""
        if hasattr(self, 'db'):
            self.db.close()
    
    # API Methods
    def add_todo(self, todo_data: Dict[str, Any]) -> Dict[str, Any]:
        """Add a new todo and return its details."""
        todo_id = self.manager.create_todo(**todo_data)
        return {"id": todo_id, "message": "Todo created successfully"}
    
    def get_all_todos(self, include_completed: bool = False, 
                      parent_id: Optional[int] = None,
                      tags: List[str] = None) -> List[Dict[str, Any]]:
        """Get all todos with optional filtering."""
        return self.manager.get_todos(include_completed, parent_id, tags)
    
    def get_todo_by_id(self, todo_id: int) -> Dict[str, Any]:
        """Get a specific todo by ID."""
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT * FROM todos WHERE id = ?", (todo_id,))
        todo = cursor.fetchone()
        
        if not todo:
            return {"error": "Todo not found"}
        
        result = dict(todo)
        result['tags'] = self.manager._get_todo_tags(todo_id)
        result['subtasks'] = self.manager.get_todos(include_completed=False, parent_id=todo_id)
        
        return result
    
    def update_todo_by_id(self, todo_id: int, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update a todo and return success status."""
        success = self.manager.update_todo(todo_id, updates)
        if success:
            return {"message": "Todo updated successfully"}
        return {"error": "Failed to update todo"}
    
    def delete_todo_by_id(self, todo_id: int, 
                         delete_subtasks: bool = True) -> Dict[str, Any]:
        """Delete a todo and return success status."""
        success = self.manager.delete_todo(todo_id, delete_subtasks)
        if success:
            return {"message": "Todo deleted successfully"}
        return {"error": "Failed to delete todo"}
    
    def create_subtasks(self, todo_id: int, 
                       subtasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create subtasks for a todo and return their IDs."""
        subtask_ids = self.manager.split_todo(todo_id, subtasks)
        return {
            "message": f"Created {len(subtask_ids)} subtasks",
            "subtask_ids": subtask_ids
        }
    
    def search(self, query: str) -> List[Dict[str, Any]]:
        """Search todos and return matches."""
        return self.manager.search_todos(query)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get todo statistics."""
        return self.manager.get_stats()

# Example usage
if __name__ == "__main__":
    service = TodoService()
    
    # Create a sample todo
    todo_data = {
        "title": "Complete project proposal",
        "description": "Write up the detailed proposal for the client meeting",
        "deadline": "2025-03-15",
        "urgency": 4,
        "effort": 8,
        "tags": ["work", "client", "writing"]
    }
    
    result = service.add_todo(todo_data)
    print(f"Created todo: {result}")
    
    # Split into subtasks
    subtasks = [
        {
            "title": "Gather requirements",
            "description": "Identify all client requirements",
            "urgency": 5,
            "effort": 3,
            "tags": ["work", "research"]
        },
        {
            "title": "Create outline",
            "description": "Develop proposal outline",
            "urgency": 3,
            "effort": 2,
            "tags": ["work", "writing"]
        },
        {
            "title": "Write draft",
            "description": "Write the first draft of the proposal",
            "urgency": 4,
            "effort": 5,
            "tags": ["work", "writing"]
        }
    ]
    
    if 'id' in result:
        subtask_result = service.create_subtasks(result['id'], subtasks)
        print(f"Created subtasks: {subtask_result}")
    
    # Get all todos
    todos = service.get_all_todos()
    print(f"Found {len(todos)} todos")
    
    # Clean up
    service.db.close()