from flowstash.decorators import integration_task
from flowstash.clients import get_client

@integration_task(integration="demo", integration_pipeline="adhoc_task")
async def adhoc_task(data: dict):
    """
    Ad-hoc task triggered via webhook.
    """

    print(f"Running ad-hoc task with data: {data}")
