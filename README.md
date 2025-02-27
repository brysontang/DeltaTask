# DeltaTask - Todo Application Backend

A powerful, locally-hosted todo application backend with advanced task management capabilities.

## Features

- **Smart Task Management**: Create todos with deadlines, urgency levels, and effort estimates
- **Prioritization Engine**: Automatically sorts tasks by deadline, urgency, and effort
- **Task Decomposition**: Split larger tasks into manageable subtasks
- **Tagging System**: Organize todos with custom tags
- **Local Storage**: All data stored locally in SQLite database
- **Comprehensive API**: Full CRUD operations for todos and related entities

## Technical Details

### Data Model

- **Todos**: Core task entity with properties:
  - Title and description
  - Deadline (optional date)
  - Urgency (1-5 scale, 5 being highest)
  - Effort (Fibonacci sequence: 1, 2, 3, 5, 8, 13, 21)
  - Completion status
  - Parent-child relationships for subtasks
  - Tags for categorization

### Database Schema

The application uses SQLite with the following tables:

- `todos`: Stores all todo items and their properties
- `tags`: Stores unique tag names
- `todo_tags`: Junction table for many-to-many relationship between todos and tags

### API Endpoints

The `TodoService` class provides the following operations:

- `add_todo`: Create a new todo item
- `get_all_todos`: Retrieve todos with optional filtering
- `get_todo_by_id`: Get a specific todo by ID
- `update_todo_by_id`: Update a todo's properties
- `delete_todo_by_id`: Remove a todo (with option to handle subtasks)
- `create_subtasks`: Split a todo into multiple subtasks
- `search`: Find todos by title, description, or tags
- `get_statistics`: Retrieve metrics about todos

## Getting Started

### Prerequisites

- Python 3.6+
- SQLite3

### Installation

1. Clone this repository
2. Install dependencies (if any are added in the future)

## Model Context Protocol (MCP)

This backend implements a Model Context Protocol approach for task management:

1. **Structured Data Model**: Clearly defined schema for todos with relationships
2. **Priority Calculation**: Intelligent sorting based on multiple factors
3. **Hierarchical Organization**: Parent-child relationships for task decomposition
4. **Tagging System**: Flexible categorization for better context
5. **Statistics and Insights**: Data aggregation for understanding task patterns

## Future Development

- Frontend interface (in progress)
- Recurring tasks
- Time tracking
- Integration with calendar systems
- Mobile applications

## License

MIT License
