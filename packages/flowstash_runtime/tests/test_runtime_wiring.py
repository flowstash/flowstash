import sys

import pytest
from flowstash.config.runtime_config import RuntimeConfig, BackendConfig, BackendType
from flowstash.runtime.wiring import runtime as runtime_module
from flowstash.runtime.wiring.runtime import initialize_runtime, build_worker_runtime
import flowstash.runtime.worker.backends.managed.managed_consumer as managed_consumer_module
from flowstash.runtime.worker.runner import build_worker_consumer, run_worker
from flowstash.runtime.worker.backends.dramatiq.dramatiq_consumer import (
    DramatiqConsumer,
)
from flowstash.runtime.worker.backends.managed.managed_consumer import ManagedConsumer


def test_initialize_runtime_sets_backend():
    config = RuntimeConfig(backend=BackendConfig(type=BackendType.ASYNC))
    rt = initialize_runtime(config)
    assert rt.backend is not None
    from flowstash.queue.backend import get_backend

    assert get_backend() == rt.backend


def test_build_worker_consumer_dramatiq():
    config = RuntimeConfig(backend=BackendConfig(type=BackendType.DRAMATIQ))
    consumer = build_worker_consumer(config)
    assert isinstance(consumer, DramatiqConsumer)


def test_build_worker_consumer_managed():
    config = RuntimeConfig(backend=BackendConfig(type=BackendType.MANAGED))
    consumer = build_worker_consumer(config)
    assert isinstance(consumer, ManagedConsumer)


@pytest.mark.asyncio
async def test_run_worker_managed_defaults_to_http_mode(monkeypatch):
    config = RuntimeConfig(backend=BackendConfig(type=BackendType.MANAGED))

    called = []

    async def fake_run_managed_http_server(passed_config):
        called.append(passed_config)

    async def unexpected_start(self):
        raise AssertionError("ManagedConsumer.start should not run in HTTP mode")

    monkeypatch.setattr(sys, "argv", ["worker_main.py"])
    monkeypatch.setattr(
        "flowstash.runtime.worker.backends.managed.main.run_managed_http_server",
        fake_run_managed_http_server,
    )
    monkeypatch.setattr(
        managed_consumer_module.ManagedConsumer, "start", unexpected_start
    )

    await run_worker(config)

    assert called == [config]


@pytest.mark.asyncio
async def test_run_worker_managed_job_mode_uses_managed_consumer(monkeypatch):
    config = RuntimeConfig(backend=BackendConfig(type=BackendType.MANAGED))

    started = []

    async def fake_start(self):
        started.append(self.config)

    async def unexpected_run_managed_http_server(passed_config):
        raise AssertionError("HTTP server should not start in managed job mode")

    monkeypatch.setattr(sys, "argv", ["worker_main.py", "run-task", "pkg.task"])
    monkeypatch.setattr(
        managed_consumer_module.ManagedConsumer,
        "start",
        fake_start,
    )
    monkeypatch.setattr(
        "flowstash.runtime.worker.backends.managed.main.run_managed_http_server",
        unexpected_run_managed_http_server,
    )

    await run_worker(config)

    assert started == [config]


def test_build_worker_consumer_async_raises():
    config = RuntimeConfig(backend=BackendConfig(type=BackendType.ASYNC))
    with pytest.raises(ValueError, match="BackendType.ASYNC.*does not support"):
        build_worker_consumer(config)


def test_build_worker_runtime_is_deprecated_but_works():
    config = RuntimeConfig(backend=BackendConfig(type=BackendType.ASYNC))
    # This should still work but internaly use initialize_runtime
    rt = build_worker_runtime(config)
    assert rt.backend is not None


def test_runtime_broker_property():
    config = RuntimeConfig(backend=BackendConfig(type=BackendType.DRAMATIQ))
    rt = initialize_runtime(config)
    # This should return the global broker
    assert rt.broker is not None


def test_initialize_runtime_accepts_extra_imports():
    config = RuntimeConfig(backend=BackendConfig(type=BackendType.ASYNC))
    # We just verify it doesn't crash
    rt = initialize_runtime(config, auto_import=["/tmp/non_existent_path_test"])
    assert rt is not None


def test_initialize_runtime_prints_flowstash_version(monkeypatch, capsys):
    monkeypatch.setattr(runtime_module, "_get_flowstash_version", lambda: "9.9.9")

    config = RuntimeConfig(backend=BackendConfig(type=BackendType.ASYNC))

    initialize_runtime(config)

    captured = capsys.readouterr()
    assert "Flowstash version: 9.9.9" in captured.out


if __name__ == "__main__":
    pytest.main([__file__])
