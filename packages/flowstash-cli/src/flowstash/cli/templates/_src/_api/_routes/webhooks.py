from flowstash import ingress
from flowstash.pipelines import RecordData
from fastapi import Request
from flowstash.pipelines.records_feed import RecordsFeed

@ingress.webhook(integration="demo", pipeline="process_user", path="/demo/process_user", method="POST")
async def process_user_webhook(request: Request):
    """Webhook that triggers a background worker task."""
    try:
        data = await request.json()
    except Exception:
        data = {}
        
    user_id = data.get("user_id")
    
    if not user_id:
        return {"error": "user_id required"}, 400

    feed = RecordsFeed.get("test_feed")
    await feed.publish(RecordData(record_id=user_id, record_type="user", data={"user_id": user_id}))
    
    return {
        "status": "submitted",
        "user_id": user_id
    }
