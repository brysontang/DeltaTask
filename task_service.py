"""
Exposes the TaskService class for the application.
This is a facade that provides access to the modular services in the deltatask package.
"""

from deltatask import logger
from deltatask.services import TaskService

# Re-export the TaskService class
__all__ = ['TaskService']