"""
MCP (Model Context Protocol) server implementation for DeltaTask.
This server exposes the task service functionality over the MCP protocol.
"""

from typing import Any
import uuid
from mcp.server.fastmcp import FastMCP
from deltatask import logger
from deltatask.services import TaskService

# Initialize MCP Server
mcp = FastMCP("DeltaTask", port=8765)
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
async def list_tasks(tags: list[str] = None) -> list[dict[str, Any]]:
    """List all tasks with optional tags, if you user asks for a tag, please provide it in the request."""
    return service.get_all_tasks(tags=tags)

# MCP Tool: Get task statistics
@mcp.tool()
async def get_statistics() -> dict[str, Any]:
    """Get task statistics including completion rates and urgency distribution."""
    return service.get_statistics()

# MCP Tool: Create subtasks for a task
@mcp.tool()
async def create_subtasks(task_id: str, subtasks: list[dict[str, Any]]) -> dict[str, Any]:
    """Create multiple subtasks for a parent task with categories."""
    return service.create_subtasks(task_id, subtasks)

# MCP Tool: Get all tags
@mcp.tool()
async def get_all_tags() -> list[str]:
    """Get all unique tag names used in tasks."""
    return service.get_all_tags()

# MCP Tool: Get subtasks for a given parent task ID
@mcp.tool()
async def get_subtasks(parent_id: str) -> list[dict[str, Any]]:
    """Get subtasks for a given parent task ID."""
    return service.get_all_tasks(parent_id=parent_id)

# MCP Tool: Finish a task
@mcp.tool()
async def finish_task(task_id: str) -> dict[str, Any]:
    """Mark a task as completed."""
    return service.update_task_by_id(task_id, {"completed": True})

# Run the MCP server
if __name__ == "__main__":
    logger.info("Starting DeltaTask MCP server")
    mcp.run(transport='stdio')  # Required for Claude for Desktop
