# DeltaTask - Advanced Task Management System

A powerful, locally-hosted task management application with Obsidian integration and a Model Context Protocol (MCP) server.

## Features

- **Smart Task Management**: Create tasks with urgency levels and effort estimates
- **Prioritization Engine**: Automatically sorts tasks by urgency and effort
- **Task Decomposition**: Split larger tasks into manageable subtasks
- **Tagging System**: Organize tasks with custom tags
- **Local Storage**: All data stored locally in SQLite database
- **Obsidian Integration**: Bi-directional sync with Obsidian markdown files
- **MCP Server**: Full API access through Model Context Protocol

## Technical Details

### Data Model

- **Tasks**: Core task entity with properties:
  - Title and description
  - Urgency (1-5 scale, 5 being highest)
  - Effort (1-21 scale, following Fibonacci sequence)
  - Completion status
  - Parent-child relationships for subtasks
  - Tags for categorization

### Database Schema

The application uses SQLite with the following tables:

- `todos`: Stores all task items and their properties
- `tags`: Stores unique tag names
- `todo_tags`: Junction table for many-to-many relationship between tasks and tags

### Obsidian Integration

DeltaTask creates and maintains a structured Obsidian vault:

- Task files with frontmatter metadata
- Tag-based views for filtering tasks
- Statistics dashboard
- Bi-directional sync between Obsidian markdown and SQLite database

### MCP API Endpoints

The MCP server exposes the following operations:

- `get_task_by_id`: Get a specific task by ID
- `search_tasks`: Find tasks by title, description, or tags
- `create_task`: Create a new task
- `update_task`: Update a task's properties
- `delete_task`: Remove a task
- `sync_tasks`: Sync tasks from Obsidian markdown into SQLite
- `list_tasks`: List all tasks
- `get_statistics`: Retrieve metrics about tasks
- `create_subtasks`: Split a task into multiple subtasks
- `get_all_tags`: Get all unique tag names
- `get_subtasks`: Get subtasks for a given parent task
- `finish_task`: Mark a task as completed

## Getting Started

### Prerequisites

- Python 3.10+
- SQLite3
- Obsidian (optional, for markdown integration)

### Installation

1. Clone this repository
2. Set up the Python environment using `uv`:

   ```
   # Create and activate the virtual environment
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate

   # Install dependencies
   uv pip install -r requirements.txt
   ```

### Running the MCP Server

The DeltaTask MCP server can be used with Claude for Desktop:

1. Configure Claude for Desktop:

   - Open or create `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Add the DeltaTask server configuration:

   ```json
   {
     "mcpServers": {
       "deltatask": {
         "command": "uv",
         "args": [
           "--directory",
           "/ABSOLUTE/PATH/TO/DeltaTask",
           "run",
           "server.py"
         ]
       }
     }
   }
   ```

   - Restart Claude for Desktop

If you run into issues or want more details, check out the [Docs for the MCP](https://modelcontextprotocol.io/quickstart/server).

For instance from the docs:

You may need to put the full path to the `uv` executable in the `command` field. You can get this by running `which uv` on MacOS/Linux or `where uv` on Windows.

2. Use the DeltaTask tools in Claude for Desktop by clicking the hammer icon

## Model Context Protocol (MCP)

This application implements a Model Context Protocol approach for task management:

1. **Structured Data Model**: Clearly defined schema for tasks with relationships
2. **Priority Calculation**: Intelligent sorting based on multiple factors
3. **Hierarchical Organization**: Parent-child relationships for task decomposition
4. **Tagging System**: Flexible categorization for better context
5. **Statistics and Insights**: Data aggregation for understanding task patterns
6. **Obsidian Integration**: Markdown-based visualization and editing

## License

MIT License
