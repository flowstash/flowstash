"""
Webhook router generation from flowstash registry.

Builds FastAPI routes from webhooks registered with @ingress.webhook decorator.
"""
import inspect
from typing import Any, Optional
from fastapi import APIRouter, Request, Response
from flowstash.ingress import ingress
from flowstash.context import integration_context


async def _invoke_handler(handler, request: Request, body: bytes) -> Any:
    """
    Invoke a webhook handler within the integration context.
    
    Handles both sync and async handlers.
    """
    metadata = handler._ingress_metadata
    
    with integration_context(
        integration=metadata["integration"],
        integration_pipeline=metadata["pipeline"]
    ):
        # Pass the request to the handler - let the handler decide what to do with it
        if inspect.iscoroutinefunction(handler):
            result = await handler(request)
        else:
            result = handler(request)
        
        return result


def build_webhook_router() -> APIRouter:
    """
    Build FastAPI router from registered webhooks.
    
    Reads all webhooks registered via @ingress.webhook and creates
    FastAPI endpoints for each one.
    
    Returns:
        APIRouter with webhook routes tagged under "webhooks"
    """
    router = APIRouter(tags=["webhooks"])
    
    for handler in ingress.get_webhooks():
        metadata = handler._ingress_metadata
        # Build path from metadata
        path = metadata["path"]
        methods = [metadata.get("method", "POST")]

        # Create endpoint closure that captures the handler
        # Define the endpoint function factory
        def create_endpoint(h=handler):
            async def endpoint(request: Request) -> Response:
                result = await _invoke_handler(h, request, await request.body())
                # Default to 202 Accepted for webhook processing
                if result is None:
                    return Response(status_code=202)
                return result
            # Attach metadata for CLI reflection traversing FastAPI routes
            endpoint._ingress_metadata = h._ingress_metadata
            return endpoint

        # Create the endpoint function
        endpoint = create_endpoint(handler)
        
        router.add_api_route(path, endpoint, methods=methods)
    
    return router
