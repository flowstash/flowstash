"""
JSON-safe serialization for RecordData.data payloads.

Pydantic models are wrapped in a typed envelope:
    {"$type": "pkg.mod.ClassName", "$data": {...}}

This allows the consumer side to reconstruct the original model class.  When
the class cannot be imported (e.g. old envelope from a past deploy), from_jsonable
falls back to returning the raw "$data" dict so delivery never crashes.

Envelope keys use "$" prefix to avoid collision with real payload keys.
"""

import base64
import importlib
import logging
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

_TYPE_KEY = "$type"
_DATA_KEY = "$data"


def to_jsonable(obj: Any) -> Any:
    """Convert obj to a JSON-safe value, wrapping pydantic models in a typed envelope."""
    if hasattr(obj, "model_dump") and hasattr(obj, "model_validate"):
        cls = type(obj)
        return {
            _TYPE_KEY: f"{cls.__module__}.{cls.__qualname__}",
            _DATA_KEY: to_jsonable(obj.model_dump(mode="json")),
        }
    if is_dataclass(obj) and not isinstance(obj, type):
        return {k: to_jsonable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    return obj


def from_jsonable(obj: Any) -> Any:
    """Inverse of to_jsonable. Reconstructs pydantic models from typed envelopes."""
    if isinstance(obj, dict):
        if _TYPE_KEY in obj and _DATA_KEY in obj:
            cls = _import_symbol(obj[_TYPE_KEY])
            inner = from_jsonable(obj[_DATA_KEY])
            if cls is not None:
                try:
                    return cls.model_validate(inner)
                except Exception as e:
                    logger.warning(
                        f"Could not reconstruct {obj[_TYPE_KEY]} from envelope: {e}. "
                        "Returning raw data dict."
                    )
            return inner
        return {k: from_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [from_jsonable(v) for v in obj]
    return obj


def _import_symbol(dotted: str) -> Any:
    """Import a class by its dotted module+qualname string. Returns None on failure."""
    # qualname may contain dots for nested classes (e.g. "Outer.Inner")
    # Try progressively shorter module splits until one imports cleanly.
    parts = dotted.rsplit(".", 1)
    if len(parts) < 2:
        return None
    module_path, attr = parts[0], parts[1]
    try:
        mod = importlib.import_module(module_path)
        return getattr(mod, attr)
    except (ImportError, AttributeError):
        pass

    # Handle nested classes: "pkg.mod.Outer.Inner" → module="pkg.mod", attr="Outer.Inner"
    parts2 = dotted.rsplit(".", 2)
    if len(parts2) == 3:
        try:
            mod = importlib.import_module(parts2[0])
            outer = getattr(mod, parts2[1])
            return getattr(outer, parts2[2])
        except (ImportError, AttributeError):
            pass

    logger.warning(f"Could not import symbol {dotted!r} for record deserialization.")
    return None
