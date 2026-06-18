from src.flowstash.queue.backend import JobHandle
from src.flowstash.decorators import integration_task


@integration_task(
    integration="test",
    integration_pipeline="test",
)
def test_task(*args, **kwargs):
    """
    test task
    """
    print(f"Processing test task with args: {args} and kwargs: {kwargs}")
    return "test result"


@integration_task(
    integration="test",
    integration_pipeline="test",
)
async def test_async_task(*args, **kwargs):
    """
    test task
    """
    print(f"Processing test task with args: {args} and kwargs: {kwargs}")
    return "test result"


async def test1():
    """make sure i can either await results from the task, or not"""

    # 1. Call without awaiting
    result = test_task(1, 2, key="value")
    assert result is JobHandle

    # 2. Call with awaiting
    result = await test_task(3, 4, key="another")
    assert result == "test result"

    # 1. Call without awaiting
    result = test_async_task(1, 2, key="value")
    assert result is JobHandle

    # 2. Call with awaiting
    result = await test_async_task(3, 4, key="another")
    assert result == "test result"
