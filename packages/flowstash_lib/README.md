# flowstash-lib

The core library of the [FlowStash](https://flowstash.github.io/flowstash/) integration framework — the programming model you write integrations against:

- **`@integration_task` / `@integration_step`** — queueable tasks and traced inline steps
- **`@ingress.webhook` / `@ingress.poll`** — HTTP ingress and scheduled polling with durable cursor state
- **Record feeds** — deduplicated, latest-wins pub/sub for business records (`RecordsFeed`, `@feed_consumer`)
- **`State`** — scoped key-value persistence for cursors and checkpoints
- **Observability** — runs, spans, logs, data exchanges, and record links, correlated automatically

```python
from flowstash.pipelines import RecordData, feed_consumer

@feed_consumer(feed_id="orders", subscription="erp-sync")
async def sync_order(record: RecordData):
    ...
```

## Install

```bash
pip install flowstash-lib
```

Usually installed as part of the full framework: `pip install flowstash`. Executing tasks and serving webhooks additionally requires `flowstash-runtime`.

📚 **Documentation:** https://flowstash.github.io/flowstash/ — see [Tasks & Steps](https://flowstash.github.io/flowstash/concepts/tasks-and-steps/), [Feeds & Pipelines](https://flowstash.github.io/flowstash/concepts/feeds-and-pipelines/), and the [API reference](https://flowstash.github.io/flowstash/reference/api/flowstash-lib/).
