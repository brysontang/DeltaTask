import os
import shutil
import uuid
import logging
from typing import List, Dict, Any, Optional, Set, Tuple, Union
from datetime import datetime

from deltatask.repositories import DeltaTaskRepository
from deltatask.services.obsidian_service import ObsidianMarkdownManager

# Get logger
logger = logging.getLogger("DeltaTask")

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
    
    def get_all_tags(self) -> List[str]:
        """Get all unique tag names."""
        return self.repository.get_all_tags()
    
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