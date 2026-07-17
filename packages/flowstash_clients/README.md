# flowstash-clients

Declarative API clients for the [FlowStash](https://flowstash.github.io/flowstash/) integration framework — and a useful standalone HTTP client layer.

Define a client in YAML (base URL, OAuth2/API-key/basic auth, retries, TLS, request mocking), optionally add typed methods in Python, and call it:

```python
from flowstash.clients import HttpClient, client

@client("crm")
class CrmClient(HttpClient):
    async def contacts(self) -> list[dict]:
        resp = await self.request("GET", "/contacts")
        return resp.json()

contacts = await CrmClient.get_client().contacts()
```

Includes GraphQL (`flowstash.clients.graphql`) and OData (`flowstash.clients.odata`, with `@odata.nextLink` pagination) helpers, automatic OAuth2 token refresh, retry with backoff, and secret-masked observability of every request.

## Install

```bash
pip install flowstash-clients
```

This is the lowest-layer FlowStash package — use it alone, or as part of the full framework via `pip install flowstash`.

📚 **Documentation:** [Clients concept](https://flowstash.github.io/flowstash/concepts/clients/) · [Calling External APIs](https://flowstash.github.io/flowstash/guides/calling-external-apis/) · [API reference](https://flowstash.github.io/flowstash/reference/api/flowstash-clients/)
