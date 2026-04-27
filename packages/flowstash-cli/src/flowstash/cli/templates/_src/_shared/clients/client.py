from typing import List, Optional
from flowstash.clients import HttpClient, client

@client("DemoClient")
class DemoClient(HttpClient):
    """
    Client for the Demo API.
    """
    
    async def get_users(self) -> List[dict]:
        response = await self.request("GET", "/users")
        data = response.json()
        return data
    
    async def get_user(self, user_id: int) -> dict:
        response = await self.request("GET", f"/users/{{user_id}}")
        data = response.json()
        return data
