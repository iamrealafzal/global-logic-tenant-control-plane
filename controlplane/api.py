"""HTTP routes for tenants and tasks."""

from __future__ import annotations

import logging

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict
from starlette.exceptions import HTTPException as StarletteHTTPException

from controlplane.domain import AppError, ValidationError
from controlplane.service import Service, mutation_json, parse_limit, task_json, tenant_json

log = logging.getLogger(__name__)
router = APIRouter()


class CreateTenantBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    slug: str
    name: str


class UpdateTenantBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    version: int


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def app_error(_request: Request, exc: AppError) -> JSONResponse:
        if exc.status >= 500:
            log.exception("request failed")
        return _error(exc.status, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def invalid_body(_request: Request, _exc: RequestValidationError) -> JSONResponse:
        return _error(400, "validation_error", "request body must be valid json")

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        if exc.status_code == 404:
            return _error(404, "not_found", "route not found")
        if exc.status_code == 405:
            return _error(405, "method_not_allowed", "method not allowed")
        message = exc.detail if isinstance(exc.detail, str) else "request failed"
        return _error(exc.status_code, "http_error", message)

    @app.exception_handler(Exception)
    async def unexpected(_request: Request, exc: Exception) -> JSONResponse:
        log.exception("request failed", exc_info=exc)
        return _error(500, "internal_error", "internal error")


@router.get("/healthz")
async def health(request: Request) -> JSONResponse:
    try:
        await _service(request).healthy()
    except Exception:
        log.exception("health")
        return _error(503, "unavailable", "not ready")
    return JSONResponse({"status": "ok"})


@router.post("/v1/tenants", status_code=201)
async def create_tenant(request: Request, body: CreateTenantBody) -> JSONResponse:
    _require_json(request)
    tenant, task = await _service(request).create_tenant(body.slug, body.name)
    return JSONResponse(
        mutation_json(tenant, task),
        status_code=201,
        headers={"Location": f"/v1/tenants/{tenant.id}"},
    )


@router.get("/v1/tenants")
async def list_tenants(
    request: Request,
    status: str = "",
    cursor: str = "",
    limit: str | None = None,
) -> JSONResponse:
    page = await _service(request).list_tenants(status, cursor, parse_limit(limit))
    body: dict = {"tenants": [tenant_json(item) for item in page.tenants]}
    if page.next_cursor:
        body["next_cursor"] = page.next_cursor
    return JSONResponse(body)


@router.get("/v1/tenants/{tenant_id}")
async def get_tenant(request: Request, tenant_id: str) -> JSONResponse:
    tenant = await _service(request).get_tenant(tenant_id)
    return JSONResponse(tenant_json(tenant))


@router.patch("/v1/tenants/{tenant_id}", status_code=202)
async def update_tenant(request: Request, tenant_id: str, body: UpdateTenantBody) -> JSONResponse:
    _require_json(request)
    tenant, task = await _service(request).update_tenant(tenant_id, body.name, body.version)
    return JSONResponse(mutation_json(tenant, task), status_code=202)


@router.delete("/v1/tenants/{tenant_id}", status_code=202)
async def delete_tenant(request: Request, tenant_id: str) -> JSONResponse:
    tenant, task = await _service(request).delete_tenant(tenant_id)
    return JSONResponse(mutation_json(tenant, task), status_code=202)


@router.get("/v1/tasks")
async def list_tasks(
    request: Request,
    tenant_id: str = "",
    status: str = "",
    cursor: str = "",
    limit: str | None = None,
) -> JSONResponse:
    page = await _service(request).list_tasks(tenant_id, status, cursor, parse_limit(limit))
    body: dict = {"tasks": [task_json(item) for item in page.tasks]}
    if page.next_cursor:
        body["next_cursor"] = page.next_cursor
    return JSONResponse(body)


@router.get("/v1/tasks/{task_id}")
async def get_task(request: Request, task_id: str) -> JSONResponse:
    task = await _service(request).get_task(task_id)
    return JSONResponse(task_json(task))


def _service(request: Request) -> Service:
    return request.app.state.service


def _require_json(request: Request) -> None:
    content_type = request.headers.get("content-type", "")
    if not content_type.startswith("application/json"):
        raise ValidationError("content-type must be application/json")


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse({"code": code, "message": message}, status_code=status)
