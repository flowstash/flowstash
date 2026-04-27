from typing import Protocol, runtime_checkable

@runtime_checkable
class TaskConsumer(Protocol):
    """
    Protocol for a task consumer that can be started and stopped.
    
    Implementations handle the backend-specific details of receiving
    and executing tasks (e.g. Dramatiq worker, FastAPI HTTP router).
    """
    async def start(self) -> None:
        """Start the consumer."""
        ...

    async def stop(self) -> None:
        """Stop the consumer."""
        ...
