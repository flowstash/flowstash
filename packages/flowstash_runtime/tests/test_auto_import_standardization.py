import sys
import os
import shutil
import pytest
from pathlib import Path
from flowstash.runtime.wiring.runtime import _import_module_from_path


@pytest.fixture
def temp_project(tmp_path):
    """
    Creates a temporary project structure:
    /tmp/app/
        src/
            api/
                __init__.py
                routes.py
        nopackage/
            script.py
    """
    app_dir = tmp_path / "app"
    app_dir.mkdir()

    src_dir = app_dir / "src"
    src_dir.mkdir()

    api_dir = src_dir / "api"
    api_dir.mkdir()
    (api_dir / "__init__.py").touch()

    with open(api_dir / "routes.py", "w") as f:
        f.write("def get_routes(): return []\n")

    nopackage_dir = app_dir / "nopackage"
    nopackage_dir.mkdir()
    with open(nopackage_dir / "script.py", "w") as f:
        f.write("val = 42\n")

    return app_dir


def test_import_with_src_root(temp_project):
    routes_file = temp_project / "src" / "api" / "routes.py"

    # Import should resolve relative to 'src'
    mod = _import_module_from_path(routes_file)

    assert mod.__name__ == "api.routes"
    assert hasattr(mod, "get_routes")
    assert str(temp_project / "src") in sys.path

    # Cleanup sys.modules to avoid side effects in other tests
    if "api.routes" in sys.modules:
        del sys.modules["api.routes"]


def test_import_without_src_fallback_to_package_root(temp_project):
    script_file = temp_project / "nopackage" / "script.py"

    # No 'src' in parents, should use 'nopackage' as base (it has no __init__.py)
    mod = _import_module_from_path(script_file)

    assert mod.__name__ == "script"
    assert mod.val == 42
    assert str(temp_project / "nopackage") in sys.path

    if "script" in sys.modules:
        del sys.modules["script"]


def test_import_failure_raises_exception(temp_project):
    broken_file = temp_project / "src" / "api" / "broken.py"
    with open(broken_file, "w") as f:
        f.write("import non_existent_module_xyz\n")

    with pytest.raises(ImportError):
        _import_module_from_path(broken_file)

    if "api.broken" in sys.modules:
        del sys.modules["api.broken"]
