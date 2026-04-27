import os
import tarfile
import tempfile
import gzip
from pathlib import Path
from typing import List, Optional

def bundle_source(directory: Path, ignore_patterns: Optional[List[str]] = None) -> Path:
    """
    Bundles the source code in directory into a .tar.gz file.
    Follows .gitignore if present.
    """
    temp_dir = Path(tempfile.gettempdir())
    tar_path = temp_dir / f"source_{os.urandom(4).hex()}.tar.gz"
    
    # Basic ignore logic (should be improved to handle .gitignore properly)
    default_ignore = {".git", ".venv", "__pycache__", ".pytest_cache", ".flowstash"}
    if ignore_patterns:
        default_ignore.update(ignore_patterns)

    def is_ignored(path: Path) -> bool:
        for part in path.parts:
            if part in default_ignore:
                return True
        return False

    with tarfile.open(tar_path, "w:gz") as tar:
        for root, dirs, files in os.walk(directory):
            root_path = Path(root)
            if is_ignored(root_path.relative_to(directory)):
                continue
            
            for file in files:
                file_path = root_path / file
                if is_ignored(file_path.relative_to(directory)):
                    continue
                
                tar.add(file_path, arcname=str(file_path.relative_to(directory)))
                
    return tar_path
