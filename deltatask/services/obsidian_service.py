import os
import re
import logging
import frontmatter
from typing import List, Dict, Any, Set
from datetime import datetime

# Get logger
logger = logging.getLogger("DeltaTask")

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
        
    def _get_parent_title(self, parent_id: str) -> str:
        """Get the title of a parent task by its ID."""
        # Find parent file with any name pattern that starts with the parent ID
        tasks_dir = os.path.join(self.vault_path, "tasks")
        parent_file = None
        
        if os.path.exists(tasks_dir):
            for filename in os.listdir(tasks_dir):
                if filename.startswith(f"{parent_id}") and filename.endswith(".md"):
                    parent_file = os.path.join(tasks_dir, filename)
                    break
        
        if not parent_file or not os.path.exists(parent_file):
            logger.warning(f"Parent file not found for parent ID: {parent_id} when getting title")
            return None
            
        try:
            post = frontmatter.load(parent_file)
            return post.get("title", f"Parent Task {parent_id}")
        except Exception as e:
            logger.error(f"Error getting parent title for {parent_id}: {e}", exc_info=True)
            return None
    
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
            
            # Add parent link in related section if this is a subtask
            if "parent_id" in task and task["parent_id"]:
                parent_id = task["parent_id"]
                # Get parent title if available
                parent_title = self._get_parent_title(parent_id) or f"Parent Task {parent_id}"
                sanitized_parent_title = self._sanitize_filename(parent_title)
                content += f"- **Parent:** [[tasks/{parent_id} - {sanitized_parent_title}]]\n"
            
            # Create the file with frontmatter
            post = frontmatter.Post(content, **metadata)
            
            # Create a filename using ID and title for better readability in graph view
            sanitized_title = self._sanitize_filename(task['title'])
            filename = f"{task['id']} - {sanitized_title}.md"
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
        # Find parent file with any name pattern that starts with the parent ID
        tasks_dir = os.path.join(self.vault_path, "tasks")
        parent_file = None
        
        if os.path.exists(tasks_dir):
            for filename in os.listdir(tasks_dir):
                if filename.startswith(f"{parent_id}") and filename.endswith(".md"):
                    parent_file = os.path.join(tasks_dir, filename)
                    break
        
        if not parent_file or not os.path.exists(parent_file):
            logger.warning(f"Parent file not found for parent ID: {parent_id}")
            return
            
        try:
            post = frontmatter.load(parent_file)
            content = post.content
            
            # Find the Subtasks section and add the link - now linking with tasks/ prefix and sanitized title
            subtasks_section = "## Subtasks\n\n"
            if subtasks_section in content:
                # Create link using tasks/ prefix and sanitized title
                sanitized_title = self._sanitize_filename(subtask_title)
                link = f"- [[tasks/{subtask_id} - {sanitized_title}]]\n"
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
                
                # Link using tasks/ prefix and sanitized title
                sanitized_title = self._sanitize_filename(task_title)
                link = f"- [[tasks/{task_id} - {sanitized_title}]]\n"
                
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
        # First try with the new format (ID - title)
        sanitized_title = self._sanitize_filename(task["title"]) 
        task_file = os.path.join(self.vault_path, "tasks", f"{task['id']} - {sanitized_title}.md")
        
        # If not found, try the old format (just ID)
        if not os.path.exists(task_file):
            old_format_file = os.path.join(self.vault_path, "tasks", f"{task['id']}.md")
            if os.path.exists(old_format_file):
                logger.info(f"Found task file in old format, using: {old_format_file}")
                task_file = old_format_file
            else:
                # If file doesn't exist in either format, create it
                logger.info(f"Task file {task['id']} not found, creating new file")
                self.create_task_file(task)
                return
            
        try:
            post = frontmatter.load(task_file)
            
            # Update frontmatter fields
            old_title = post.get("title", "")
            new_title = task["title"]
            post["title"] = new_title
            post["updated"] = task.get("updated", datetime.now().isoformat())
            post["urgency"] = task.get("urgency", post.get("urgency", 1))
            post["effort"] = task.get("effort", post.get("effort", 1))
            post["completed"] = task.get("completed", post.get("completed", False))
            
            if "deadline" in task:
                post["deadline"] = task["deadline"]
            elif "deadline" in post and task.get("deadline") is None:
                del post["deadline"]
                
            # If title has changed, update links in all child tasks
            if old_title != new_title:
                self._update_child_parent_links(task["id"], new_title)
                
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
    
    def _remove_parent_links_from_children(self, parent_id: str) -> None:
        """Remove parent links from all child tasks when a parent task is deleted."""
        logger.info(f"Removing parent links in child tasks for deleted parent {parent_id}")
        tasks_dir = os.path.join(self.vault_path, "tasks")
        
        if not os.path.exists(tasks_dir):
            logger.warning(f"Tasks directory not found when removing child parent links")
            return
            
        try:
            # Find all child task files
            for filename in os.listdir(tasks_dir):
                if not filename.endswith(".md") or filename in ["all.md", "urgent.md", "today.md", "overdue.md"]:
                    continue
                    
                file_path = os.path.join(tasks_dir, filename)
                
                try:
                    post = frontmatter.load(file_path)
                    
                    # Check if this task has the target parent
                    if post.get("parent") == parent_id:
                        # Remove the parent reference from frontmatter
                        if "parent" in post:
                            del post["parent"]
                        
                        content = post.content
                        
                        # Remove the parent link from the content - updated for tasks/ prefix
                        parent_link_pattern = f"- \\*\\*Parent:\\*\\* \\[\\[tasks/{parent_id} - [^\\]]+\\]\\]\n"
                        updated_content = re.sub(parent_link_pattern, "", content)
                        
                        if updated_content != content:
                            post.content = updated_content
                            
                            with open(file_path, "wb") as f:
                                frontmatter.dump(post, f)
                            logger.info(f"Removed parent link from child task {post.get('id')}")
                            
                except Exception as e:
                    logger.error(f"Error removing parent link from child task file {filename}: {e}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Error removing child parent links: {e}", exc_info=True)
    
    def _update_child_parent_links(self, parent_id: str, new_parent_title: str) -> None:
        """Update parent links in all child tasks when a parent's title changes."""
        logger.info(f"Updating parent links in child tasks for parent {parent_id}")
        tasks_dir = os.path.join(self.vault_path, "tasks")
        
        if not os.path.exists(tasks_dir):
            logger.warning(f"Tasks directory not found when updating child parent links")
            return
            
        try:
            # Find all child task files
            for filename in os.listdir(tasks_dir):
                if not filename.endswith(".md") or filename in ["all.md", "urgent.md", "today.md", "overdue.md"]:
                    continue
                    
                file_path = os.path.join(tasks_dir, filename)
                
                try:
                    post = frontmatter.load(file_path)
                    
                    # Check if this task has the target parent
                    if post.get("parent") == parent_id:
                        content = post.content
                        
                        # Replace the parent link with updated title - updated for tasks/ prefix
                        sanitized_parent_title = self._sanitize_filename(new_parent_title)
                        parent_link_pattern = f"- \\*\\*Parent:\\*\\* \\[\\[tasks/{parent_id} - [^\\]]+\\]\\]"
                        new_parent_link = f"- **Parent:** [[tasks/{parent_id} - {sanitized_parent_title}]]"
                        
                        if re.search(parent_link_pattern, content):
                            # Replace the existing link
                            updated_content = re.sub(parent_link_pattern, new_parent_link, content)
                            post.content = updated_content
                            
                            with open(file_path, "wb") as f:
                                frontmatter.dump(post, f)
                            logger.info(f"Updated parent link in child task {post.get('id')}")
                        else:
                            # Add the parent link if it doesn't exist
                            related_section = "## Related\n\n"
                            if related_section in content:
                                sections = content.split(related_section)
                                new_content = sections[0] + related_section + new_parent_link + "\n" + sections[1]
                                post.content = new_content
                                
                                with open(file_path, "wb") as f:
                                    frontmatter.dump(post, f)
                                logger.info(f"Added parent link in child task {post.get('id')}")
                            
                except Exception as e:
                    logger.error(f"Error updating parent link in child task file {filename}: {e}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Error updating child parent links: {e}", exc_info=True)
    
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
            
            # Filter out the line with this task ID - updated for tasks/ prefix
            new_lines = [line for line in lines if f"tasks/{task_id} -" not in line]
            
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
        # Try to find the task file with any name pattern that starts with the task ID
        tasks_dir = os.path.join(self.vault_path, "tasks")
        matching_files = []
        
        if os.path.exists(tasks_dir):
            for filename in os.listdir(tasks_dir):
                if filename.startswith(f"{task_id}") and filename.endswith(".md"):
                    matching_files.append(os.path.join(tasks_dir, filename))
        
        if not matching_files:
            logger.warning(f"Task file {task_id} not found when attempting to delete")
            return
            
        # Use the first matching file
        task_file = matching_files[0]
            
        try:
            # Get task data before deletion
            post = frontmatter.load(task_file)
            parent_id = post.get("parent")
            tags = post.get("tags", [])
            task_title = post.get("title", f"Task {task_id}")
            
            # Remove this task from all child tasks' parent link
            # This ensures if a parent is deleted, child tasks no longer reference it
            self._remove_parent_links_from_children(task_id)
            
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
        # Find parent file with any name pattern that starts with the parent ID
        tasks_dir = os.path.join(self.vault_path, "tasks")
        parent_file = None
        
        if os.path.exists(tasks_dir):
            for filename in os.listdir(tasks_dir):
                if filename.startswith(f"{parent_id}") and filename.endswith(".md"):
                    parent_file = os.path.join(tasks_dir, filename)
                    break
        
        if not parent_file or not os.path.exists(parent_file):
            logger.warning(f"Parent file not found for parent ID: {parent_id} when removing subtask {subtask_id}")
            return
            
        try:
            post = frontmatter.load(parent_file)
            content = post.content
            
            # Find and remove the link to the subtask - updated for tasks/ prefix
            lines = content.split('\n')
            # Look for lines containing the subtask ID in a link with tasks/ prefix
            new_lines = [line for line in lines if f"[[tasks/{subtask_id} -" not in line]
            
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
                        sanitized_title = self._sanitize_filename(task['title'])
                        f.write(f"- {completed}[[tasks/{task['id']} - {sanitized_title}]]{deadline}\n")
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
                        sanitized_title = self._sanitize_filename(task['title'])
                        f.write(f"- {urgency} [[tasks/{task['id']} - {sanitized_title}]]{deadline}\n")
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
                        sanitized_title = self._sanitize_filename(task['title'])
                        f.write(f"- {urgency} [[tasks/{task['id']} - {sanitized_title}]]\n")
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
                        sanitized_title = self._sanitize_filename(task['title'])
                        f.write(f"- {urgency} [[tasks/{task['id']} - {sanitized_title}]]{deadline}\n")
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