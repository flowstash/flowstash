"""Job-mode observability drain: waits in slices, logs progress, and only drops
queued writes once the deadline is exceeded."""

import json

from flowstash.runtime.worker.backends.managed import managed_consumer as mc


class _FakeManager:
    def __init__(self, pending_sequence):
        self._seq = list(pending_sequence)
        self.flush_calls = []
        self.shutdown_calls = []

    def flush(self, timeout):
        self.flush_calls.append(timeout)

    def pending(self):
        return self._seq.pop(0) if len(self._seq) > 1 else self._seq[0]

    def shutdown(self, cancel_pending=False):
        self.shutdown_calls.append(cancel_pending)
        return 42


def test_drain_returns_once_nothing_is_pending(monkeypatch):
    fake = _FakeManager([
        {"store_writes": 5, "buffered": 3, "inflight_batches": 1, "inflight_terminal": 0},
        {"store_writes": 0, "buffered": 0, "inflight_batches": 0, "inflight_terminal": 0},
    ])
    monkeypatch.setattr(mc.AsyncManager, "get_instance", classmethod(lambda cls: fake))
    mc._drain_observability(timeout_s=600)
    assert len(fake.flush_calls) == 2
    assert fake.shutdown_calls == []  # nothing dropped


def test_drain_drops_only_after_deadline(monkeypatch):
    fake = _FakeManager([{"store_writes": 7, "buffered": 0, "inflight_batches": 0, "inflight_terminal": 0}])
    monkeypatch.setattr(mc.AsyncManager, "get_instance", classmethod(lambda cls: fake))
    clock = iter([0.0, 0.0, 0.0, 31.0, 31.0, 62.0, 62.0, 62.0])
    monkeypatch.setattr(mc.time, "monotonic", lambda: next(clock))
    mc._drain_observability(timeout_s=60)
    assert fake.shutdown_calls == [True]
    # flush slices never exceed the configured slice length
    assert all(t <= 30.0 for t in fake.flush_calls)


def test_run_binding_line_is_json_with_run_id(monkeypatch, capsys):
    monkeypatch.setenv("FLOWSTASH_OBS_SCOPE", "abc123")
    monkeypatch.setenv("CLOUD_RUN_EXECUTION", "job-exec-1")
    mc._log_run_binding("run-9", "worker.tasks.x")
    line = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert line["flowstash_run_bind"] is True
    assert line["run_id"] == "run-9"
    assert line["obs_scope"] == "abc123"
    assert line["execution"] == "job-exec-1"
