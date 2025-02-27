from typing import Any
import uuid
import logging
from mcp.server.fastmcp import FastMCP
from task_service import TaskService  # Import your existing TaskService

# Initialize MCP Server
mcp = FastMCP("tasks")
service = TaskService()

# MCP Tool: Get task by ID
@mcp.tool()
async def get_task_by_id(task_id: str) -> dict[str, Any]:
    """Get details for a specific task by ID."""
    task = service.get_task_by_id(task_id)
    if not task:
        return {"error": "Task not found"}
    return task

# MCP Tool: Search tasks
@mcp.tool()
async def search_tasks(query: str) -> list[dict[str, Any]]:
    """Search tasks by title, description, or tags."""
    return service.search(query)

# MCP Tool: Create a new task
@mcp.tool()
async def create_task(title: str, description: str = "", urgency: int = 1, effort: int = 1, tags: list[str] = []) -> dict[str, Any]:
    """Create a new task."""
    task_data = {
        "id": str(uuid.uuid4()),  # Generate unique ID
        "title": title,
        "description": description,
        "urgency": urgency,
        "effort": effort,
        "tags": tags
    }
    result = service.add_task(task_data)
    return result

# MCP Tool: Update an existing task
@mcp.tool()
async def update_task(task_id: str, updates: dict[str, Any]) -> dict[str, Any]:
    """Update an existing task."""
    return service.update_task_by_id(task_id, updates)

# MCP Tool: Delete a task
@mcp.tool()
async def delete_task(task_id: str) -> dict[str, Any]:
    """Delete a task."""
    return service.delete_task_by_id(task_id)

# MCP Tool: Sync from Obsidian
@mcp.tool()
async def sync_tasks() -> dict[str, Any]:
    """Sync tasks from Obsidian markdown into SQLite."""
    return service.sync_from_obsidian()

# MCP Tool: List all tasks
@mcp.tool()
async def list_tasks() -> list[dict[str, Any]]:
    """List all tasks."""
    return service.get_all_tasks()

# MCP Tool: Get task statistics
@mcp.tool()
async def get_statistics() -> dict[str, Any]:
    """Get task statistics including completion rates and urgency distribution."""
    return service.get_statistics()

# MCP Tool: Create subtasks for a task
@mcp.tool()
async def create_subtasks(task_id: str, subtasks: list[dict[str, Any]]) -> dict[str, Any]:
    """Create multiple subtasks for a parent task."""
    return service.create_subtasks(task_id, subtasks)

# MCP Tool: Get all tags
@mcp.tool()
async def get_all_tags() -> list[str]:
    """Get all unique tag names used in tasks."""
    return service.get_all_tags()

# Run the MCP server
if __name__ == "__main__":
    mcp.run(transport='stdio')  # Required for Claude for Desktop
