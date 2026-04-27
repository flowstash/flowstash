from pathlib import Path
from fastapi import Request
from flowstash.config.env_loader import load_config_dir
from flowstash.runtime.ingress.app import create_fastapi_app
import os
env = os.getenv("ENVIRONMENT", "dev")
# Load configuration relative to this file
config = load_config_dir("config", environment=env)

# Create FastAPI app
app = create_fastapi_app(config, auto_import=[Path(__file__).parent / "src" / "api" / "routes" ])

@app.get("/health")
async def health():
    """Simple status webhook."""
    return {"status": "ok", "service": "{project_name}-api"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
