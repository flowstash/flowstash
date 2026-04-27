import os
import uvicorn
from fastapi import FastAPI
from flowstash.config.runtime_config import RuntimeConfig
from flowstash.queue.consumer import TaskConsumer
from .main import create_app

class ManagedConsumer(TaskConsumer):
    """
    TaskConsumer implementation for managed mode.
    
    Starts a FastAPI server to receive task execution callbacks via HTTP.
    """
    def __init__(self, config: RuntimeConfig, host: str = "0.0.0.0", port: int = None):
        self.config = config
        self.host = host
        self.port = port or int(os.environ.get("PORT", 8080))
        self.app: FastAPI = create_app(self.config)
        self.server: uvicorn.Server = None

    async def start(self) -> None:
        config = uvicorn.Config(self.app, host=self.host, port=self.port, log_level="info")
        self.server = uvicorn.Server(config)
        await self.server.serve()

    async def stop(self) -> None:
        if self.server:
            self.server.should_exit = True
