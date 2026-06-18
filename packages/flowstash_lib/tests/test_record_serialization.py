import json
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel

from flowstash.pipelines.record_serialization import from_jsonable, to_jsonable


class Inner(BaseModel):
    value: int
    label: str


class Outer(BaseModel):
    name: str
    inner: Inner
    tags: list[str]
    ts: Optional[datetime] = None


# ── to_jsonable ──────────────────────────────────────────────────────────────


def test_primitive_passthrough():
    assert to_jsonable(42) == 42
    assert to_jsonable("hello") == "hello"
    assert to_jsonable(None) is None
    assert to_jsonable(3.14) == 3.14


def test_bytes_becomes_base64():
    result = to_jsonable(b"\x00\x01\x02")
    assert result == "AAEC"


def test_datetime_becomes_isoformat():
    dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    assert to_jsonable(dt) == dt.isoformat()


def test_dict_recursed():
    result = to_jsonable({"a": 1, "b": [2, 3]})
    assert result == {"a": 1, "b": [2, 3]}


def test_list_recursed():
    assert to_jsonable([1, "x", None]) == [1, "x", None]


def test_pydantic_model_wrapped():
    inner = Inner(value=7, label="hi")
    result = to_jsonable(inner)
    assert result["$type"].endswith("Inner")
    assert result["$data"] == {"value": 7, "label": "hi"}


def test_pydantic_model_json_serializable():
    outer = Outer(name="test", inner=Inner(value=1, label="a"), tags=["x"])
    serialized = to_jsonable(outer)
    # Must not raise
    json.dumps(serialized)


def test_pydantic_nested_inside_dict():
    data = {"msg": Inner(value=3, label="z")}
    result = to_jsonable(data)
    assert result["msg"]["$type"].endswith("Inner")


def test_pydantic_nested_inside_list():
    data = [Inner(value=1, label="a"), Inner(value=2, label="b")]
    result = to_jsonable(data)
    assert all("$type" in item for item in result)


# ── from_jsonable ────────────────────────────────────────────────────────────


def test_round_trip_simple_model():
    original = Inner(value=42, label="world")
    assert from_jsonable(to_jsonable(original)) == original


def test_round_trip_nested_model():
    original = Outer(name="outer", inner=Inner(value=5, label="nested"), tags=["a", "b"])
    assert from_jsonable(to_jsonable(original)) == original


def test_round_trip_model_in_dict():
    original = {"key": Inner(value=99, label="dict")}
    result = from_jsonable(to_jsonable(original))
    assert result["key"] == Inner(value=99, label="dict")


def test_round_trip_model_in_list():
    original = [Inner(value=1, label="a"), Inner(value=2, label="b")]
    result = from_jsonable(to_jsonable(original))
    assert result == original


def test_plain_dict_passthrough():
    data = {"x": 1, "y": [2, 3]}
    assert from_jsonable(data) == data


def test_unknown_type_returns_data_dict():
    envelope = {"$type": "nonexistent.module.FakeClass", "$data": {"foo": "bar"}}
    result = from_jsonable(envelope)
    # Should return the $data dict, not crash
    assert result == {"foo": "bar"}


def test_json_round_trip_via_string():
    original = Inner(value=10, label="json")
    serialized = json.dumps(to_jsonable(original))
    restored = from_jsonable(json.loads(serialized))
    assert restored == original


# ── Original bug: pydantic object in record.data must not raise ──────────────


class ChannelMessage(BaseModel):
    channel: str
    text: str


def test_channel_message_serializes_and_round_trips():
    msg = ChannelMessage(channel="general", text="hello")
    # Must not raise TypeError (the original bug)
    raw = json.dumps(to_jsonable(msg))
    # Must reconstruct original model (importable module-level class)
    restored = from_jsonable(json.loads(raw))
    assert isinstance(restored, ChannelMessage)
    assert restored.channel == "general"
