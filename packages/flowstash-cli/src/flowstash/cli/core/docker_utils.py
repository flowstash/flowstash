import shutil
import subprocess
from typing import Tuple, Optional

def check_docker_binary() -> bool:
    return shutil.which("docker") is not None

def check_docker_compose_binary() -> bool:
    # Check for 'docker compose' (V2) or 'docker-compose' (V1)
    if shutil.which("docker-compose"):
        return True
    
    # Check if 'docker compose' works
    try:
        subprocess.run(["docker", "compose", "version"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def check_docker_daemon() -> Tuple[bool, Optional[str]]:
    """Returns (is_running, error_message)"""
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True)
        return True, None
    except subprocess.CalledProcessError as e:
        return False, e.stderr.decode().strip()
    except FileNotFoundError:
        return False, "Docker binary not found"

def get_docker_compose_cmd() -> list:
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    return ["docker", "compose"]
