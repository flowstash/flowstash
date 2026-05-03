# Implementation Plan: Standardize Import Root to `src/` and Fix Auto-Import Behavior

Goal
- Ensure module resolution is identical between local development and containerized deployments by treating `src/` (when present) as the canonical import root.
- Make auto-import deterministic and fail-fast so the application does not start with missing modules.

Summary of changes
1. In the runtime auto-importer (`packages/flowstash_runtime/src/flowstash/runtime/wiring/runtime.py`):
   - Prefer a deterministic `src/` root when a `src` directory exists in the file's ancestor chain.
   - Otherwise, use the package-boundary walk (stop at first ancestor without `__init__.py`) to compute the module name.
   - Add only the resolved `base_path` (the `src` dir or the package root) to `sys.path` before import.
   - Remove silent exception swallowing: raise import errors so the process fails fast.
2. Add unit tests that verify resolver behavior in several scenarios and assert that import failures raise.
3. Update docs and developer guidance explaining that code should import from the source root (e.g. `shared.utils`) or live under a real package namespace.

Rationale
- `sys.path` varies by invocation environment (cwd, packaging, docker WORKDIR, PYTHONPATH). Using on-disk markers (`src/` directory or `__init__.py`) makes resolution predictable and independent of process startup details.
- Fail-fast behavior prevents degraded health checks where the app starts but core functionality is missing.

Design details

A. Resolver algorithm (pseudocode)

1. Input: `file_path: Path` (file we want to import)
2. Resolve `file_path = file_path.resolve()`
3. If any ancestor directory is named `src` (closest to file wins):
   - `base_path = that_src_dir`
   - `module_name = dotted path from base_path to file` (drop `.py`; for `__init__.py` use package dir)
   - Insert `str(base_path)` into `sys.path` (at index 0 if not present)
   - `importlib.import_module(module_name)` (do not swallow exceptions)
   - Notes: This makes imports be relative to `src/` (i.e., `shared.utils`), matching the src-layout.
4. Else: run package-boundary walk (current fallback logic):
   - Walk up parents while `(parent / '__init__.py').exists()`; stop at the first directory without `__init__.py`; set that as `base_path`.
   - Compute `module_name` relative to `base_path` as before.
   - Insert `base_path` into `sys.path` (if missing) and import.
5. If import fails, raise the exception to the caller.

B. Why `src` first?
- Common Python project layout uses a top-level `src/` directory where packages live. Making `src` the canonical import root ensures developers can run code locally and in containers without changing PYTHONPATH.
- Choosing the nearest `src` ancestor handles nested projects or monorepos where not every repo root is the process working directory.

C. Edge cases and rules
- If multiple `src` directories exist in the ancestor chain, pick the closest to the file.
- If a project wants to use a top-level package name (e.g., `myapp.shared`), that package should be under `src/myapp/...` and imports should use `myapp.shared`.
- Files that rely on `from ...shared.utils` (relative up-three) must be migrated to absolute imports from the `src` root (e.g., `from shared.utils import ...`) or be placed under a named package to make relative imports valid.

Files and functions to change
- Update: `packages/flowstash_runtime/src/flowstash/runtime/wiring/runtime.py`
  - Replace `_import_module_from_path()` implementation with the new resolver.
  - Keep `_auto_import_path()` callers unchanged, but it will now propagate exceptions.
- Tests:
  - Add tests in `packages/flowstash_runtime/tests/` e.g. `test_auto_import_src_root.py`:
    - Scenario A: project with `src/` — verify `module_name` computed and import works when `src` on disk.
    - Scenario B: project with package `__init__.py` chain but no `src/` — verify package-boundary walk computes same module name as before.
    - Scenario C: import failure for a broken module path raises and `initialize_runtime` propagates the error.
  - Update existing test `test_initialize_runtime_accepts_extra_imports` to expect a `ValueError` only for non-existent path, not silent failure (keep current behavior for non-existent paths).

Implementation sketch (code-level)

- Function signature unchanged:
  def _import_module_from_path(file_path: Path) -> Any:

- Pseudocode mapping to code-level:

    file_path = file_path.resolve()

    # Prefer nearest ancestor named "src"
    current = file_path.parent if file_path.is_file() else file_path
    src_ancestor = None
    for p in list(current.parents) + [current]:
        if p.name == "src":
            src_ancestor = p
            break

    if src_ancestor:
        base_path = src_ancestor
        rel = file_path.parent.relative_to(base_path) if file_path.is_file() else file_path.relative_to(base_path)
        module_parts = list(rel.parts) + ([file_path.stem] if file_path.is_file() and file_path.name != "__init__.py" else [])
    else:
        # package-boundary walk (existing fallback)
        current_dir = file_path.parent if file_path.is_file() else file_path
        while (current_dir / "__init__.py").exists():
            current_dir = current_dir.parent
        base_path = current_dir
        rel = ...

    module_name = ".".join(module_parts)
    if str(base_path) not in sys.path:
        sys.path.insert(0, str(base_path))
    # Let exceptions bubble
    return importlib.import_module(module_name)

Testing and verification
- Unit tests (pytest) covering the above scenarios.
- Manual smoke test (local):

```bash
# from repository root
python -c "import sys, pprint; pprint.pprint(sys.path[:5])"
# Start the app the same way Docker will do (simulate WORKDIR=/app)
python -c "import sys; sys.path.insert(0, '/full/path/to/project/src'); import shared.utils; print('ok')"
```

- Docker smoke test:
  - Build image using existing `deployment/docker/base.Dockerfile`.
  - Run container and observe that startup fails if an auto-imported module has a real import error.

Rollout plan
1. Implement the resolver change in `runtime.py` and update tests.
2. Run test suite locally and fix any failures.
3. Update developer docs/README with the import rule and migration notes.
4. Deploy to staging and verify the server fails-fast on missing imports and that valid imports succeed.
5. Communicate to developers: "If you use the src layout, import from source root (e.g., `from shared.utils import ...`) or use a named package under `src` (e.g., `from myapp.shared...`)."

Migration guidance for existing code
- Replace relative imports that climb above package root with absolute imports from `src` root where appropriate.
- Prefer explicit named package namespaces under `src` for larger projects (e.g., `src/myapp/...` and `from myapp.shared import ...`).

Estimated work & timeline
- Implement changes + unit tests: 2–4 hours
- Run full test-suite and iterate: 1–2 hours
- Documentation + rollout: 1 hour

Next steps I can take
- Implement the `_import_module_from_path` changes and run the runtime unit tests locally.
- Or if you prefer, I can open a small PR with the change and tests for review.


