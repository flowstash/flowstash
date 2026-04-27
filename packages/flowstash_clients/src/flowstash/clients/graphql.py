from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TypeVar

from .http import HttpClient

T = TypeVar("T")

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class GraphQLErrorInfo:
    """
    Structured GraphQL error information.
    """
    message: str
    path: Optional[List[Any]] = None
    locations: Optional[List[Dict[str, int]]] = None
    extensions: Optional[Dict[str, Any]] = None

@dataclass(frozen=True)
class GraphQLResponse:
    """
    Holds a GraphQL response data and errors.
    """
    data: Optional[Dict[str, Any]]
    errors: List[GraphQLErrorInfo]
    extensions: Optional[Dict[str, Any]] = None
    raw: Optional[Dict[str, Any]] = None

class GraphQLHttpError(Exception):
    """
    Exception raised when a GraphQL request fails at the HTTP level.
    """
    def __init__(self, status_code: int, body_text: str):
        super().__init__(f"GraphQL HTTP error {status_code}: {body_text}")
        self.status_code = status_code
        self.body_text = body_text

class GraphQLExecutionError(Exception):
    """
    Exception raised when a GraphQL response contains errors.
    """
    def __init__(self, errors: List[GraphQLErrorInfo], data: Optional[Dict[str, Any]] = None):
        message = errors[0].message if errors else "GraphQL execution error"
        super().__init__(message)
        self.errors = errors
        self.data = data

class GraphQLClient:
    """
    GraphQL client wrapping HttpClient.
    Provides ergonomic access to GraphQL services.
    """
    def __init__(
        self, 
        http: HttpClient, 
        endpoint_path: str = "/",
        *, 
        default_headers: Optional[Dict[str, str]] = None,
        strict: bool = True
    ):
        self._http = http
        self._endpoint_path = endpoint_path
        self._default_headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if default_headers:
            self._default_headers.update(default_headers)
        self._strict = strict

    async def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: Optional[str] = None,
        *,
        headers: Optional[Dict[str, str]] = None,
        strict: Optional[bool] = None
    ) -> GraphQLResponse:
        """
        Execute a GraphQL operation (Query or Mutation).
        """
        body: Dict[str, Any] = {"query": query}
        if variables is not None:
            body["variables"] = variables
        if operation_name is not None:
            body["operationName"] = operation_name

        merged_headers = dict(self._default_headers)
        if headers:
            merged_headers.update(headers)

        # We use try/except to catch HTTP status errors from HttpClient if any
        try:
            resp = await self._http.request(
                "POST", 
                self._endpoint_path, 
                headers=merged_headers, 
                json=body
            )
        except Exception as e:
            import httpx
            if isinstance(e, httpx.HTTPStatusError):
                raise GraphQLHttpError(e.response.status_code, e.response.text)
            raise e

        # GraphQL can return 200 with errors in the body
        payload = resp.json()
        if not isinstance(payload, dict):
             raise GraphQLHttpError(resp.status_code, f"Expected JSON dictionary, got {type(payload).__name__}")

        errors = [
            GraphQLErrorInfo(
                message=e.get("message", "GraphQL error"),
                path=e.get("path"),
                locations=e.get("locations"),
                extensions=e.get("extensions"),
            )
            for e in (payload.get("errors") or [])
            if isinstance(e, dict)
        ]

        result = GraphQLResponse(
            data=payload.get("data"),
            errors=errors,
            extensions=payload.get("extensions"),
            raw=payload,
        )

        use_strict = strict if strict is not None else self._strict
        if use_strict and errors:
            raise GraphQLExecutionError(errors, data=result.data)

        return result

    async def query(
        self, 
        query: str, 
        variables: Optional[Dict[str, Any]] = None, 
        operation_name: Optional[str] = None, 
        *, 
        headers: Optional[Dict[str, str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Execute a GraphQL query and return the data.
        """
        response = await self.execute(query, variables, operation_name, headers=headers)
        return response.data

    async def mutate(
        self, 
        mutation: str, 
        variables: Optional[Dict[str, Any]] = None, 
        operation_name: Optional[str] = None, 
        *, 
        headers: Optional[Dict[str, str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Execute a GraphQL mutation and return the data.
        """
        response = await self.execute(mutation, variables, operation_name, headers=headers)
        return response.data
