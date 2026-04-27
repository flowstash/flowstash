from flowstash.decorators import integration_task
from flowstash.queue.backend import Schedule




@integration_task(integration="demo", integration_pipeline="demo", default_schedule=Schedule(cron="0 0 * * 0"))
def demo_task():
    """
    Demo task to be executed weekly.
    """
    print(f"Processing user...")
    
    
    