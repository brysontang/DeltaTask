import os
import json
import shutil
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Set, Tuple, Union
import re
import frontmatter
import pathlib
import uuid
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Text, Float, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.sql import func
from contextlib import contextmanager
import sqlite3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("deltatask.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DeltaTask")


# Database models
Base = declarative_base()

# Junction table for many-to-many relationship between todos and tags
todo_tags = Table(
    'todo_tags', 
    Base.metadata,
    Column('todo_id', String(36), ForeignKey('todos.id')),
    Column('tag_id', String(36), ForeignKey('tags.id'))
)


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


class Tag(Base):
    """Database model for tags."""
    __tablename__ = 'tags'
    
    id = Column(String(36), primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    
    # Relationships
    todos = relationship("Todo", secondary=todo_tags, back_populates="tags")


class ObsidianMarkdownManager:
    """Manages the Obsidian markdown files for visualizing tasks."""
    
    def __init__(self, vault_path: str = "TaskVault"):
        """Initialize the markdown manager with a vault path."""
        self.vault_path = vault_path
        self._ensure_vault_exists()
        
    def sync_from_markdown(self) -> List[Dict[str, Any]]:
        """Scan markdown files for changes and return modified tasks to be synced with the database."""
        logger.info("Scanning markdown files for manual changes")
        
        modified_tasks = []
        tasks_path = os.path.join(self.vault_path, "tasks")
        
        try:
            if not os.path.exists(tasks_path):
                logger.warning(f"Tasks directory not found at {tasks_path}")
                return []
            
            # Scan all markdown files in the tasks directory
            for filename in os.listdir(tasks_path):
                if not filename.endswith(".md") or filename in ["all.md", "urgent.md", "today.md", "overdue.md"]:
                    continue
                
                try:
                    file_path = os.path.join(tasks_path, filename)
                    task_id = filename.replace(".md", "")
                    
                    # Parse the markdown file
                    post = frontmatter.load(file_path)
                    
                    # Check if the markdown file has valid frontmatter
                    if "id" not in post:
                        logger.warning(f"Task file {filename} missing ID in frontmatter")
                        continue
                        
                    if post["id"] != task_id:
                        logger.warning(f"Task file {filename} has mismatched ID: {post['id']} vs {task_id}")
                    
                    # Extract task data from frontmatter
                    task_data = {
                        "id": post["id"],
                        "title": post.get("title", f"Untitled Task {post['id']}"),
                        "updated": post.get("updated", datetime.now().isoformat()),
                        "urgency": post.get("urgency", 1),
                        "effort": post.get("effort", 1),
                        "completed": post.get("completed", False)
                    }
                    
                    if "deadline" in post:
                        task_data["deadline"] = post["deadline"]
                    
                    if "parent" in post:
                        task_data["parent_id"] = post["parent"]
                        
                    if "tags" in post:
                        task_data["tags"] = post["tags"]
                    
                    # Extract description from content
                    content = post.content.strip()
                    
                    # Extract description (everything before ## Subtasks section)
                    if "## Subtasks" in content:
                        description = content.split("## Subtasks")[0].strip()
                        task_data["description"] = description
                    else:
                        task_data["description"] = content
                    
                    modified_tasks.append(task_data)
                    logger.info(f"Parsed task file: {filename}")
                    
                except frontmatter.FrontmatterError as e:
                    logger.error(f"Error parsing frontmatter in {filename}: {e}", exc_info=True)
                except Exception as e:
                    logger.error(f"Error processing markdown file {filename}: {e}", exc_info=True)
            
            logger.info(f"Found {len(modified_tasks)} tasks from markdown files")
            return modified_tasks
            
        except Exception as e:
            logger.error(f"Error scanning markdown files: {e}", exc_info=True)
            return []
    
    def _ensure_vault_exists(self) -> None:
        """Create the vault directory structure if it doesn't exist."""
        # Create main vault directory
        os.makedirs(self.vault_path, exist_ok=True)
        
        # Create subdirectories for organization
        os.makedirs(os.path.join(self.vault_path, "tasks"), exist_ok=True)
        os.makedirs(os.path.join(self.vault_path, "tags"), exist_ok=True)
        
        # Create index files
        self._create_or_update_index()
    
    def _create_or_update_index(self, all_tags: Set[str] = None) -> None:
        """Create or update the main index file."""
        if all_tags is None:
            all_tags = set()
            
        index_path = os.path.join(self.vault_path, "index.md")
        
        content = """# Task Vault

## Overview
This vault contains your tasks organized as a graph of interconnected notes.

- [[tasks/all|All Tasks]]
- [[tags/index|Tags]]
- [[statistics|Statistics]]

## Quick Navigation
- [[tasks/urgent|Urgent Tasks]]
- [[tasks/today|Due Today]]
- [[tasks/overdue|Overdue Tasks]]

"""
        
        with open(index_path, "w") as f:
            f.write(content)
        
        # Create tag index
        tag_index_path = os.path.join(self.vault_path, "tags", "index.md")
        with open(tag_index_path, "w") as f:
            f.write("# Tags\n\n")
            for tag in all_tags:
                f.write(f"- [[{tag}]]\n")
    
    def _sanitize_filename(self, text: str) -> str:
        """Convert text into a valid filename."""
        # Replace invalid characters
        sanitized = re.sub(r'[\\/*?:"<>|]', "", text)
        # Replace spaces with dashes
        sanitized = sanitized.replace(" ", "-").lower()
        # Limit length
        if len(sanitized) > 100:
            sanitized = sanitized[:100]
        return sanitized
    
    def create_task_file(self, task: Dict[str, Any]) -> None:
        """Create a markdown file for a task."""
        logger.info(f"Creating markdown file for task {task.get('id', 'UNKNOWN')}")
        
        try:
            # Validate required fields
            if "id" not in task:
                logger.error("Task ID missing when creating task file")
                raise ValueError("Task ID is required")
                
            if "title" not in task:
                logger.error(f"Task title missing for task {task['id']}")
                task["title"] = f"Untitled Task {task['id']}"
                logger.warning(f"Using default title for task {task['id']}")
            
            # Prepare frontmatter
            metadata = {
                "id": task["id"],
                "title": task["title"],
                "created": task.get("created", datetime.now().isoformat()),
                "updated": task.get("updated", datetime.now().isoformat()),
                "urgency": task.get("urgency", 1),
                "effort": task.get("effort", 1),
                "completed": task.get("completed", False)
            }
            
            if "deadline" in task and task["deadline"]:
                metadata["deadline"] = task["deadline"]
                
            if "parent_id" in task and task["parent_id"]:
                metadata["parent"] = task["parent_id"]
                
            if "tags" in task and task["tags"]:
                metadata["tags"] = task["tags"]
            
            # Create the markdown content
            content = task.get("description", "") if task.get("description") else ""
            
            # Add links to subtasks section
            content += "\n\n## Subtasks\n\n"
            
            # Add links section for related tasks
            content += "\n\n## Related\n\n"
            
            # Create the file with frontmatter
            post = frontmatter.Post(content, **metadata)
            
            # Determine filename and path - now using just the ID for consistency
            filename = f"{task['id']}.md"
            file_path = os.path.join(self.vault_path, "tasks", filename)
            
            try:
                # Ensure the directory exists
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                # Write the file
                with open(file_path, "wb") as f:
                    frontmatter.dump(post, f)
                logger.info(f"Successfully created task file for {task['id']}")
            except IOError as e:
                logger.error(f"Failed to write task file {file_path}: {e}", exc_info=True)
                raise
            
            # Update parent file if this is a subtask
            if "parent_id" in task and task["parent_id"]:
                logger.info(f"Updating parent {task['parent_id']} with subtask {task['id']}")
                self._update_parent_subtasks(task["parent_id"], task["id"], task["title"])
            
            # Update tag files
            if "tags" in task and task["tags"]:
                logger.info(f"Updating {len(task['tags'])} tag files for task {task['id']}")
                self._update_tag_files(task["tags"], task["id"], task["title"])
                
        except Exception as e:
            logger.error(f"Error creating task file: {e}", exc_info=True)
            raise
    
    def _update_parent_subtasks(self, parent_id: str, subtask_id: str, subtask_title: str) -> None:
        """Update a parent task file to include a link to a new subtask."""
        parent_file = os.path.join(self.vault_path, "tasks", f"{parent_id}.md")
        if not os.path.exists(parent_file):
            logger.warning(f"Parent file not found: {parent_file}")
            return
            
        try:
            post = frontmatter.load(parent_file)
            content = post.content
            
            # Find the Subtasks section and add the link - now linking by ID only
            subtasks_section = "## Subtasks\n\n"
            if subtasks_section in content:
                # Create link using ID instead of title in filename
                link = f"- [[{subtask_id}|{subtask_title}]]\n"
                # Insert after the section header
                sections = content.split(subtasks_section)
                if len(sections) >= 2:
                    new_content = sections[0] + subtasks_section + link + sections[1]
                    post.content = new_content
                    
                    with open(parent_file, "wb") as f:
                        frontmatter.dump(post, f)
                    logger.info(f"Updated parent task {parent_id} with subtask {subtask_id}")
                else:
                    logger.warning(f"Could not find content after subtasks section in {parent_id}")
            else:
                logger.warning(f"Subtasks section not found in parent task {parent_id}")
        except Exception as e:
            logger.error(f"Error updating parent subtasks: {e}", exc_info=True)
    
    def _update_tag_files(self, tags: List[str], task_id: str, task_title: str) -> None:
        """Update or create tag files with links to the task."""
        for tag in tags:
            try:
                tag_filename = self._sanitize_filename(tag)
                tag_path = os.path.join(self.vault_path, "tags", f"{tag_filename}.md")
                
                # Link using ID instead of filename with title
                link = f"- [[tasks/{task_id}|{task_title}]]\n"
                
                if os.path.exists(tag_path):
                    try:
                        with open(tag_path, "r") as f:
                            content = f.read()
                    
                        if link not in content:
                            content += link
                            
                            with open(tag_path, "w") as f:
                                f.write(content)
                            logger.info(f"Updated tag file {tag} with task {task_id}")
                    except IOError as e:
                        logger.error(f"Error reading/writing tag file {tag_path}: {e}", exc_info=True)
                else:
                    try:
                        content = f"# {tag}\n\nTasks with this tag:\n\n{link}"
                        with open(tag_path, "w") as f:
                            f.write(content)
                        logger.info(f"Created new tag file for {tag}")
                    except IOError as e:
                        logger.error(f"Error creating tag file {tag_path}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Error processing tag {tag}: {e}", exc_info=True)
    
    def update_task_file(self, task: Dict[str, Any]) -> None:
        """Update a task markdown file."""
        task_file = os.path.join(self.vault_path, "tasks", f"{task['id']}.md")
        if not os.path.exists(task_file):
            # If file doesn't exist, create it
            logger.info(f"Task file {task['id']} not found, creating new file")
            self.create_task_file(task)
            return
            
        try:
            post = frontmatter.load(task_file)
            
            # Update frontmatter fields
            post["title"] = task["title"]
            post["updated"] = task.get("updated", datetime.now().isoformat())
            post["urgency"] = task.get("urgency", post.get("urgency", 1))
            post["effort"] = task.get("effort", post.get("effort", 1))
            post["completed"] = task.get("completed", post.get("completed", False))
            
            if "deadline" in task:
                post["deadline"] = task["deadline"]
            elif "deadline" in post and task.get("deadline") is None:
                del post["deadline"]
                
            # Handle description separately
            if "description" in task:
                # Preserve the subtasks and related sections
                sections = post.content.split("## Subtasks")
                if len(sections) >= 2:
                    post.content = task["description"] + "\n\n## Subtasks" + sections[1]
                else:
                    post.content = task["description"] + "\n\n## Subtasks\n\n\n\n## Related\n\n"
                    logger.warning(f"Couldn't find Subtasks section in {task['id']}, recreating structure")
            
            # Check for tags update to update tag files
            old_tags = post.get('tags', [])
            new_tags = task.get('tags', old_tags)
            
            # Update tags in frontmatter
            if "tags" in task:
                post["tags"] = task["tags"]
            
            try:
                # Write back to file
                with open(task_file, "wb") as f:
                    frontmatter.dump(post, f)
                logger.info(f"Updated task file {task['id']}")
            except IOError as e:
                logger.error(f"Error writing to task file {task_file}: {e}", exc_info=True)
                raise
            
            # Update tag files if tags changed
            if new_tags != old_tags:
                logger.info(f"Tags changed for task {task['id']}, updating tag files")
                # Remove from old tags
                for tag in old_tags:
                    if tag not in new_tags:
                        self._remove_task_from_tag(tag, task["id"])
                
                # Add to new tags
                for tag in new_tags:
                    if tag not in old_tags:
                        self._update_tag_files([tag], task["id"], task["title"])
                        
        except frontmatter.FrontmatterError as e:
            logger.error(f"Frontmatter error for task {task['id']}: {e}", exc_info=True)
            # Attempt recovery by recreating the file
            logger.info(f"Attempting to recreate task file {task['id']}")
            self.create_task_file(task)
        except Exception as e:
            logger.error(f"Error updating task file {task['id']}: {e}", exc_info=True)
            raise
    
    def _remove_task_from_tag(self, tag: str, task_id: str) -> None:
        """Remove a task link from a tag file."""
        tag_filename = self._sanitize_filename(tag)
        tag_path = os.path.join(self.vault_path, "tags", f"{tag_filename}.md")
        
        if not os.path.exists(tag_path):
            logger.warning(f"Tag file not found for tag '{tag}' when removing task {task_id}")
            return
            
        try:
            with open(tag_path, "r") as f:
                lines = f.readlines()
            
            # Filter out the line with this task ID
            new_lines = [line for line in lines if task_id not in line]
            
            # If we only have the header left, delete the file
            if len(new_lines) <= 3 and new_lines and new_lines[0].startswith("# "):
                try:
                    os.remove(tag_path)
                    logger.info(f"Removed empty tag file for '{tag}'")
                except OSError as e:
                    logger.error(f"Error removing empty tag file {tag_path}: {e}", exc_info=True)
            else:
                try:
                    with open(tag_path, "w") as f:
                        f.writelines(new_lines)
                    logger.info(f"Removed task {task_id} from tag '{tag}'")
                except IOError as e:
                    logger.error(f"Error updating tag file {tag_path}: {e}", exc_info=True)
                    
        except IOError as e:
            logger.error(f"Error reading tag file {tag_path}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error removing task {task_id} from tag '{tag}': {e}", exc_info=True)
    
    def delete_task_file(self, task_id: str) -> None:
        """Delete a task markdown file."""
        task_file = os.path.join(self.vault_path, "tasks", f"{task_id}.md")
        if not os.path.exists(task_file):
            logger.warning(f"Task file {task_id} not found when attempting to delete")
            return
            
        try:
            # Get task data before deletion
            post = frontmatter.load(task_file)
            parent_id = post.get("parent")
            tags = post.get("tags", [])
            
            # Remove from parent's subtasks list
            if parent_id:
                logger.info(f"Removing task {task_id} from parent {parent_id}")
                self._remove_from_parent_subtasks(parent_id, task_id)
            
            # Remove from tag files
            if tags:
                logger.info(f"Removing task {task_id} from {len(tags)} tags")
                for tag in tags:
                    self._remove_task_from_tag(tag, task_id)
            
            # Delete the task file
            try:
                os.remove(task_file)
                logger.info(f"Deleted task file {task_id}")
            except OSError as e:
                logger.error(f"Error deleting task file {task_file}: {e}", exc_info=True)
                raise
                
        except frontmatter.FrontmatterError as e:
            logger.error(f"Frontmatter error when deleting task {task_id}: {e}", exc_info=True)
            # Try to force delete the file if it exists
            if os.path.exists(task_file):
                try:
                    os.remove(task_file)
                    logger.info(f"Force deleted task file {task_id} after frontmatter error")
                except OSError as delete_error:
                    logger.error(f"Failed to force delete task file {task_file}: {delete_error}", exc_info=True)
        except Exception as e:
            logger.error(f"Error deleting task file {task_id}: {e}", exc_info=True)
            raise
    
    def _remove_from_parent_subtasks(self, parent_id: str, subtask_id: str) -> None:
        """Remove a subtask link from a parent task file."""
        parent_file = os.path.join(self.vault_path, "tasks", f"{parent_id}.md")
        if not os.path.exists(parent_file):
            logger.warning(f"Parent file {parent_id} not found when removing subtask {subtask_id}")
            return
            
        try:
            post = frontmatter.load(parent_file)
            content = post.content
            
            # Find and remove the link to the subtask - now checking for ID-based link
            lines = content.split('\n')
            # Look for lines containing the subtask ID in a link
            new_lines = [line for line in lines if f"[[{subtask_id}|" not in line]
            
            if len(new_lines) != len(lines):
                logger.info(f"Removed subtask {subtask_id} from parent {parent_id}")
            else:
                logger.warning(f"Subtask {subtask_id} link not found in parent {parent_id}")
                
            post.content = '\n'.join(new_lines)
            
            try:
                with open(parent_file, "wb") as f:
                    frontmatter.dump(post, f)
            except IOError as e:
                logger.error(f"Error writing to parent file {parent_file}: {e}", exc_info=True)
                raise
                
        except frontmatter.FrontmatterError as e:
            logger.error(f"Frontmatter error in parent file {parent_id}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error removing subtask {subtask_id} from parent {parent_id}: {e}", exc_info=True)
            raise
    
    def update_task_views(self, tasks: List[Dict[str, Any]]) -> None:
        """Update the task view files based on current tasks."""
        logger.info("Updating task view files")
        
        try:
            # All tasks view
            all_tasks_path = os.path.join(self.vault_path, "tasks", "all.md")
            with open(all_tasks_path, "w") as f:
                f.write("# All Tasks\n\n")
                
                if tasks:
                    for task in tasks:
                        completed = "✅ " if task.get('completed', False) else ""
                        deadline = f" (Due: {task.get('deadline', 'No deadline')})" if 'deadline' in task else ""
                        f.write(f"- {completed}[[{task['id']}|{task['title']}]]{deadline}\n")
                else:
                    f.write("No tasks found.\n")
            logger.info("Updated All Tasks view")
        except IOError as e:
            logger.error(f"Error updating All Tasks view: {e}", exc_info=True)
        
        try:
            # Urgent tasks view
            urgent_tasks_path = os.path.join(self.vault_path, "tasks", "urgent.md")
            with open(urgent_tasks_path, "w") as f:
                f.write("# Urgent Tasks\n\n")
                urgent_tasks = [t for t in tasks if not t.get('completed', False) and t.get('urgency', 1) >= 4]
                
                if urgent_tasks:
                    for task in urgent_tasks:
                        urgency = "🔥" * task.get('urgency', 1)
                        deadline = f" (Due: {task.get('deadline', 'No deadline')})" if 'deadline' in task else ""
                        f.write(f"- {urgency} [[{task['id']}|{task['title']}]]{deadline}\n")
                else:
                    f.write("No urgent tasks found.\n")
            logger.info(f"Updated Urgent Tasks view with {len(urgent_tasks) if 'urgent_tasks' in locals() else 0} tasks")
        except IOError as e:
            logger.error(f"Error updating Urgent Tasks view: {e}", exc_info=True)
        
        try:
            # Today's tasks
            today_tasks_path = os.path.join(self.vault_path, "tasks", "today.md")
            with open(today_tasks_path, "w") as f:
                f.write("# Due Today\n\n")
                today = datetime.now().date().isoformat()
                today_tasks = [t for t in tasks if not t.get('completed', False) and t.get('deadline') == today]
                
                if today_tasks:
                    for task in today_tasks:
                        urgency = "🔥" * task.get('urgency', 1)
                        f.write(f"- {urgency} [[{task['id']}|{task['title']}]]\n")
                else:
                    f.write("No tasks due today.\n")
            logger.info(f"Updated Today's Tasks view with {len(today_tasks) if 'today_tasks' in locals() else 0} tasks")
        except IOError as e:
            logger.error(f"Error updating Today's Tasks view: {e}", exc_info=True)
        
        try:
            # Overdue tasks
            overdue_tasks_path = os.path.join(self.vault_path, "tasks", "overdue.md")
            with open(overdue_tasks_path, "w") as f:
                f.write("# Overdue Tasks\n\n")
                today = datetime.now().date().isoformat()
                overdue_tasks = [t for t in tasks if not t.get('completed', False) 
                               and t.get('deadline') and t.get('deadline') < today]
                
                if overdue_tasks:
                    for task in overdue_tasks:
                        urgency = "🔥" * task.get('urgency', 1)
                        deadline = f" (Due: {task.get('deadline')})"
                        f.write(f"- {urgency} [[{task['id']}|{task['title']}]]{deadline}\n")
                else:
                    f.write("No overdue tasks.\n")
            logger.info(f"Updated Overdue Tasks view with {len(overdue_tasks) if 'overdue_tasks' in locals() else 0} tasks")
        except IOError as e:
            logger.error(f"Error updating Overdue Tasks view: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error updating task views: {e}", exc_info=True)
    
    def create_statistics_file(self, stats: Dict[str, Any]) -> None:
        """Create a statistics markdown file."""
        stats_path = os.path.join(self.vault_path, "statistics.md")
        
        try:
            content = f"""# Task Statistics

## Overview
- **Total Tasks**: {stats['total']}
- **Completed Tasks**: {stats['completed']}
- **Completion Rate**: {stats['completion_rate']:.1f}%
- **Upcoming Deadlines (Next 7 Days)**: {stats['upcoming_deadlines']}

## By Urgency
"""
            
            for urgency in range(5, 0, -1):
                count = stats['by_urgency'].get(urgency, 0)
                content += f"- **Level {urgency}**: {count} tasks\n"
            
            with open(stats_path, "w") as f:
                f.write(content)
            logger.info("Updated statistics file")
        except KeyError as e:
            logger.error(f"Missing key in statistics data: {e}", exc_info=True)
        except IOError as e:
            logger.error(f"Error writing statistics file: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error creating statistics file: {e}", exc_info=True)


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


class TaskService:
    """Service layer that abstracts and coordinates between database and markdown files."""
    
    def __init__(self, 
                db_url: str = "sqlite:///deltatask.db",
                vault_path: str = "TaskVault"):
        """Initialize with database and markdown managers."""
        self.repository = DeltaTaskRepository(db_url)
        self.markdown_manager = ObsidianMarkdownManager(vault_path)
    
    def _ensure_id(self, task_data: Dict[str, Any]) -> str:
        """Ensure the task has an ID, generating one if needed."""
        if 'id' not in task_data:
            task_data['id'] = str(uuid.uuid4())
        return task_data['id']
    
    def _recursively_add_subtasks(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively add subtasks to a task dictionary."""
        subtasks = self.repository.get_todos(include_completed=True, parent_id=task['id'])
        for subtask in subtasks:
            self._recursively_add_subtasks(subtask)
        task['subtasks'] = subtasks
        return task
    
    def _update_all_views(self) -> None:
        """Update all markdown views based on current database state."""
        all_tasks = self.get_all_tasks(include_completed=True)
        self.markdown_manager.update_task_views(all_tasks)
        
        # Update statistics
        stats = self.repository.get_statistics()
        self.markdown_manager.create_statistics_file(stats)
        
        # Update tag index
        all_tags = set(self.repository.get_all_tags())
        self.markdown_manager._create_or_update_index(all_tags)
    
    def add_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Add a new task and return its details."""
        logger.info(f"Adding new task: {task_data.get('title', 'Untitled')}")
        
        try:
            # Ensure task has an ID
            task_id = self._ensure_id(task_data)
            
            # Validate fibonacci sequence for effort
            valid_efforts = [1, 2, 3, 5, 8, 13, 21]
            if 'effort' in task_data and task_data['effort'] not in valid_efforts:
                error_msg = f"Effort must be a Fibonacci number from {valid_efforts}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Validate urgency
            if 'urgency' in task_data and not 1 <= task_data['urgency'] <= 5:
                error_msg = "Urgency must be between 1 and 5"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            try:
                # Add to database
                self.repository.add_todo(task_data)
                logger.info(f"Task {task_id} added to database")
            except Exception as e:
                logger.error(f"Failed to add task to database: {e}", exc_info=True)
                raise
            
            try:
                # Create markdown file
                self.markdown_manager.create_task_file(task_data)
                logger.info(f"Markdown file created for task {task_id}")
            except Exception as e:
                logger.error(f"Failed to create markdown file for task {task_id}: {e}", exc_info=True)
                # Continue even if markdown fails - we already have the data in the database
            
            try:
                # Update views
                self._update_all_views()
                logger.info("Task views updated")
            except Exception as e:
                logger.error(f"Failed to update task views: {e}", exc_info=True)
                # Continue even if views update fails
            
            return {"id": task_id, "message": "Task created successfully"}
            
        except Exception as e:
            logger.error(f"Error adding task: {e}", exc_info=True)
            return {"error": str(e)}
    
    def get_all_tasks(self, include_completed: bool = False, 
                    parent_id: Optional[str] = None,
                    tags: List[str] = None) -> List[Dict[str, Any]]:
        """Get all tasks with optional filtering and their subtasks."""
        # Get tasks from database
        tasks = self.repository.get_todos(include_completed, parent_id, tags)
        
        # Only add subtasks for top-level tasks
        if parent_id is None:
            for task in tasks:
                self._recursively_add_subtasks(task)
        
        return tasks
    
    def get_task_by_id(self, task_id: str) -> Dict[str, Any]:
        """Get a specific task by ID with its subtasks."""
        task = self.repository.get_todo_by_id(task_id)
        if not task:
            return {"error": "Task not found"}
        
        # Add subtasks
        self._recursively_add_subtasks(task)
        
        return task
    
    def update_task_by_id(self, task_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update a task and return success status."""
        logger.info(f"Updating task {task_id} with: {updates}")
        
        try:
            # Check if task exists
            existing_task = self.repository.get_todo_by_id(task_id)
            if not existing_task:
                logger.warning(f"Attempted to update non-existent task: {task_id}")
                return {"error": "Task not found"}
                
            # Validate fibonacci sequence for effort
            valid_efforts = [1, 2, 3, 5, 8, 13, 21]
            if 'effort' in updates and updates['effort'] not in valid_efforts:
                error_msg = f"Effort must be a Fibonacci number from {valid_efforts}"
                logger.error(f"Invalid effort value {updates['effort']} for task {task_id}")
                raise ValueError(error_msg)
            
            # Validate urgency
            if 'urgency' in updates and not 1 <= updates['urgency'] <= 5:
                error_msg = "Urgency must be between 1 and 5"
                logger.error(f"Invalid urgency value {updates['urgency']} for task {task_id}")
                raise ValueError(error_msg)
            
            try:
                # Update in database
                success = self.repository.update_todo(task_id, updates)
                
                if not success:
                    logger.error(f"Database update failed for task {task_id}")
                    return {"error": "Failed to update task in database"}
                
                logger.info(f"Task {task_id} updated in database")
            except Exception as e:
                logger.error(f"Error updating task {task_id} in database: {e}", exc_info=True)
                raise
            
            try:
                # Get updated task
                updated_task = self.repository.get_todo_by_id(task_id)
                if not updated_task:
                    logger.error(f"Could not retrieve updated task {task_id}")
                    return {"error": "Failed to retrieve updated task"}
                
                # Update markdown file
                self.markdown_manager.update_task_file(updated_task)
                logger.info(f"Markdown file updated for task {task_id}")
            except Exception as e:
                logger.error(f"Error updating markdown for task {task_id}: {e}", exc_info=True)
                # Continue even if markdown update fails
            
            try:
                # Update views
                self._update_all_views()
                logger.info("Task views updated after task update")
            except Exception as e:
                logger.error(f"Error updating views after task update: {e}", exc_info=True)
                # Continue even if views update fails
            
            return {"message": "Task updated successfully"}
            
        except ValueError as e:
            # Handle validation errors
            logger.error(f"Validation error updating task {task_id}: {e}", exc_info=True)
            return {"error": str(e)}
        except Exception as e:
            # Handle other errors
            logger.error(f"Unexpected error updating task {task_id}: {e}", exc_info=True)
            return {"error": f"Failed to update task: {str(e)}"}
    
    def delete_task_by_id(self, task_id: str, 
                        delete_subtasks: bool = True) -> Dict[str, Any]:
        """Delete a task and return success status."""
        logger.info(f"Deleting task {task_id} (with subtasks: {delete_subtasks})")
        
        try:
            # Check if task exists
            existing_task = self.repository.get_todo_by_id(task_id)
            if not existing_task:
                logger.warning(f"Attempted to delete non-existent task: {task_id}")
                return {"error": "Task not found"}
            
            try:
                # Delete from database
                success = self.repository.delete_todo(task_id, delete_subtasks)
                
                if not success:
                    logger.error(f"Database deletion failed for task {task_id}")
                    return {"error": "Failed to delete task from database"}
                
                logger.info(f"Task {task_id} deleted from database")
            except Exception as e:
                logger.error(f"Error deleting task {task_id} from database: {e}", exc_info=True)
                raise
            
            try:
                # Delete markdown file
                self.markdown_manager.delete_task_file(task_id)
                logger.info(f"Markdown file deleted for task {task_id}")
            except Exception as e:
                logger.error(f"Error deleting markdown for task {task_id}: {e}", exc_info=True)
                # Continue even if markdown deletion fails
            
            try:
                # Update views
                self._update_all_views()
                logger.info("Task views updated after task deletion")
            except Exception as e:
                logger.error(f"Error updating views after task deletion: {e}", exc_info=True)
                # Continue even if views update fails
            
            return {"message": "Task deleted successfully"}
            
        except Exception as e:
            # Handle other errors
            logger.error(f"Unexpected error deleting task {task_id}: {e}", exc_info=True)
            return {"error": f"Failed to delete task: {str(e)}"}
    
    def create_subtasks(self, task_id: str, 
                      subtasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create subtasks for a task and return their IDs."""
        # Check if parent task exists
        parent_task = self.repository.get_todo_by_id(task_id)
        if not parent_task:
            return {"error": "Parent task not found"}
        
        subtask_ids = []
        
        # Create each subtask
        for subtask in subtasks:
            subtask['parent_id'] = task_id
            subtask_id = self._ensure_id(subtask)
            self.add_task(subtask)
            subtask_ids.append(subtask_id)
        
        return {
            "message": f"Created {len(subtask_ids)} subtasks",
            "subtask_ids": subtask_ids
        }
    
    def search(self, query: str) -> List[Dict[str, Any]]:
        """Search tasks and return matches."""
        results = self.repository.search_todos(query)
        
        # Add subtasks
        for task in results:
            self._recursively_add_subtasks(task)
        
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get task statistics."""
        return self.repository.get_statistics()
    
    def sync_from_obsidian(self) -> Dict[str, Any]:
        """Sync changes from Obsidian markdown files back to the database."""
        logger.info("Starting sync from Obsidian to database")
        
        try:
            # Get all tasks from markdown files
            markdown_tasks = self.markdown_manager.sync_from_markdown()
            
            if not markdown_tasks:
                logger.info("No markdown tasks found for syncing")
                return {"message": "No tasks found for syncing", "count": 0}
            
            # Track statistics
            updated_count = 0
            error_count = 0
            
            # Process each task
            for task_data in markdown_tasks:
                try:
                    task_id = task_data["id"]
                    
                    # Check if task exists
                    existing_task = self.repository.get_todo_by_id(task_id)
                    
                    if existing_task:
                        # Update existing task
                        success = self.repository.update_todo(task_id, task_data)
                        if success:
                            logger.info(f"Updated task {task_id} from markdown")
                            updated_count += 1
                        else:
                            logger.error(f"Failed to update task {task_id} from markdown")
                            error_count += 1
                    else:
                        # Add new task
                        self.repository.add_todo(task_data)
                        logger.info(f"Added new task {task_id} from markdown")
                        updated_count += 1
                except Exception as e:
                    logger.error(f"Error syncing task {task_data.get('id', 'unknown')}: {e}", exc_info=True)
                    error_count += 1
            
            # Update all views to reflect changes
            try:
                self._update_all_views()
            except Exception as e:
                logger.error(f"Error updating views after sync: {e}", exc_info=True)
            
            result = {
                "message": "Sync completed",
                "updated": updated_count,
                "errors": error_count,
                "total": len(markdown_tasks)
            }
            logger.info(f"Sync completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error in Obsidian sync process: {e}", exc_info=True)
            return {"error": f"Sync failed: {str(e)}"}
    
    def reset(self) -> Dict[str, Any]:
        """Reset both the database and markdown files (for testing/development)."""
        logger.warning("Resetting entire task system")
        
        try:
            # Reset database by recreating tables
            Base.metadata.drop_all(self.repository.engine)
            Base.metadata.create_all(self.repository.engine)
            logger.info("Database reset complete")
            
            # Reset markdown files
            if os.path.exists(self.markdown_manager.vault_path):
                shutil.rmtree(self.markdown_manager.vault_path)
                logger.info(f"Removed vault directory: {self.markdown_manager.vault_path}")
            self.markdown_manager._ensure_vault_exists()
            logger.info("Created new vault directory")
            
            return {"message": "Task system reset successfully"}
        except Exception as e:
            logger.error(f"Error resetting task system: {e}", exc_info=True)
            return {"error": f"Reset failed: {str(e)}"}


# Example usage
if __name__ == "__main__":
    try:
        # Configure more verbose console logging for testing
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        logger.info("Starting DeltaTask demo")
        
        # Use SQLite by default
        service = TaskService()
        
        # Reset for demonstration
        reset_result = service.reset()
        logger.info(f"Reset result: {reset_result}")
        
        # Create a sample task
        task_data = {
            "title": "Complete project proposal",
            "description": "Write up the detailed proposal for the client meeting",
            "deadline": "2025-03-15",
            "urgency": 4,
            "effort": 8,
            "tags": ["work", "client", "writing"]
        }
        
        try:
            # Demonstrate proper error handling
            result = service.add_task(task_data)
            logger.info(f"Created task: {result}")
            
            # Demonstrate validation error handling
            invalid_task = {
                "title": "Invalid task",
                "urgency": 10  # Invalid urgency (should be 1-5)
            }
            invalid_result = service.add_task(invalid_task)
            logger.info(f"Attempted to create invalid task: {invalid_result}")
            
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
                logger.info(f"Created subtasks: {subtask_result}")
            
            # Get all tasks
            tasks = service.get_all_tasks()
            logger.info(f"Found {len(tasks)} tasks")
            
            # Demonstrate Obsidian sync feature
            logger.info("Demonstrating Obsidian sync...")
            sync_result = service.sync_from_obsidian()
            logger.info(f"Sync result: {sync_result}")
            
            # Get statistics
            stats = service.get_statistics()
            logger.info(f"Statistics: {stats}")
            
            logger.info("Demo completed successfully")
            
        except Exception as e:
            logger.error(f"Error in demo: {e}", exc_info=True)
            
    except Exception as e:
        print(f"Critical error: {e}")
        # Log to stdout in case logging setup failed
        import traceback
        traceback.print_exc()