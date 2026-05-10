"""
Webhook router generation from flowstash registry.

Builds FastAPI routes from webhooks registered with @ingress.webhook decorator.
"""

import inspect
from typing import Any, Optional
import datetime

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from flowstash.ingress import ingress
from flowstash.context import integration_context
from flowstash.observability.ingestion import (
    record_data_exchange,
    record_run_started,
    record_run_ended,
)
from flowstash.observability.model import DataExchangeEvent


async def _invoke_handler(handler, request: Request, body: bytes) -> Any:
    """
    Invoke a webhook handler.
    Handles both sync and async handlers.
    """
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
        def create_endpoint(h=handler):
            async def endpoint(request: Request) -> Response:
                body_bytes = await request.body()
                meta = h._ingress_metadata

                with integration_context(
                    integration=meta["integration"],
                    integration_pipeline=meta["pipeline"],
                    span_name=h.__name__,
                    record_lifecycle=False,
                ):
                    from flowstash.context import current_context
                    ctx = current_context()
                    await record_run_started(
                        correlation=ctx.corelation, entry_point=h.__name__
                    )
                    start_time = datetime.datetime.now(datetime.UTC)
                    try:
                        result = await _invoke_handler(h, request, body_bytes)

                        # Handle response mapping
                        if result is None:
                            response = Response(status_code=202)
                        elif isinstance(result, Response):
                            response = result
                        else:
                            response = JSONResponse(content=jsonable_encoder(result))

                        end_time = datetime.datetime.now(datetime.UTC)

                        # Attempt to capture body for logging
                        response_body = getattr(response, "body", b"")

                        await record_data_exchange(
                            DataExchangeEvent(
                                integration=meta["integration"],
                                channel="WEBHOOK",
                                operation=f"{request.method} {meta['path']}",
                                remote_system=(
                                    request.client.host if request.client else "unknown"
                                ),
                                address=str(request.url),
                                occurred_at=start_time,
                                completed_at=end_time,
                                state="SUCCEEDED",
                                attempt=1,
                                http_method=request.method,
                                status_code=response.status_code,
                                request_payload=body_bytes,
                                request_content_type=request.headers.get(
                                    "content-type"
                                ),
                                response_payload=response_body,
                                response_content_type=response.headers.get(
                                    "content-type"
                                ),
                            )
                        )
                        await record_run_ended(
                            correlation=ctx.corelation, status="SUCCEEDED"
                        )
                        return response

                    except Exception as e:
                        await record_run_ended(
                            correlation=ctx.corelation, status="FAILED"
                        )
                        end_time = datetime.datetime.now(datetime.UTC)
                        await record_data_exchange(
                            DataExchangeEvent(
                                integration=meta["integration"],
                                channel="WEBHOOK",
                                operation=f"{request.method} {meta['path']}",
                                remote_system=(
                                    request.client.host if request.client else "unknown"
                                ),
                                address=str(request.url),
                                occurred_at=start_time,
                                completed_at=end_time,
                                state="FAILED",
                                attempt=1,
                                http_method=request.method,
                                status_code=500,
                                request_payload=body_bytes,
                                request_content_type=request.headers.get(
                                    "content-type"
                                ),
                                attrs={"error": str(e)},
                            )
                        )
                        raise

            # Attach metadata for CLI reflection traversing FastAPI routes
            endpoint._ingress_metadata = h._ingress_metadata
            return endpoint

        # Create the endpoint function
        endpoint = create_endpoint(handler)

        router.add_api_route(path, endpoint, methods=methods)

    return router
