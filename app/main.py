import os
import hmac
import hashlib
import logging
from ipaddress import ip_address, ip_network
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.requests import Request

from app.breaker import (
    WorkerManager,
    create_rule,
    delete_rule,
    get_admin_summary,
    list_auto_disabled_channels,
    list_events,
    list_recovery_states,
    list_unstable_disabled_channels,
    manual_restore_channel,
    list_rules,
    RuleValidationError,
    update_rule,
    RulePayload,
)
from app.db import get_conn
from app.queries import (
    ip_usage_detail_sql,
    ip_usage_summary_sql,
    leaderboard_token_model_sql,
    leaderboard_token_summary_sql,
    overview_sql,
    rankings_sql,
    recent_logs_sql,
    token_model_usage_sql,
)
from app.schema import bootstrap_database


LOGGER = logging.getLogger("channel_breaker.web")


@asynccontextmanager
async def lifespan(app: FastAPI):
    bootstrap_database(import_missing_business_tables=os.getenv("INIT_IMPORT_FROM_DUMP", "0") == "1")
    workers_enabled = os.getenv("WORKERS_ENABLED", "1") != "0"
    workers = WorkerManager() if workers_enabled else None
    if workers:
        workers.start()
    app.state.workers = workers
    try:
        yield
    finally:
        if workers:
            workers.stop()


app = FastAPI(title="OneAPI Channel Breaker", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

ADMIN_PATH = os.getenv("ADMIN_PATH", "/console-9f3a2d7e").strip() or "/console-9f3a2d7e"
if not ADMIN_PATH.startswith("/"):
    ADMIN_PATH = f"/{ADMIN_PATH}"
TRUST_PROXY_HEADERS = os.getenv("TRUST_PROXY_HEADERS", "1") != "0"
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
ADMIN_AUTH_COOKIE = "admin_auth"
ADMIN_API_PREFIXES = (
    "/api/admin-summary",
    "/api/rules",
    "/api/events",
    "/api/channels/",
)


class AdminLoginPayload(BaseModel):
    password: str


def parse_allowed_ip_networks() -> list:
    raw = os.getenv("ALLOWED_IPS", "").strip()
    if not raw:
        return []
    networks = []
    for item in raw.split(","):
        candidate = item.strip()
        if not candidate:
            continue
        if "/" in candidate:
            networks.append(ip_network(candidate, strict=False))
        else:
            address = ip_address(candidate)
            suffix = "/32" if address.version == 4 else "/128"
            networks.append(ip_network(f"{candidate}{suffix}", strict=False))
    return networks


ALLOWED_IP_NETWORKS = parse_allowed_ip_networks()


def get_client_ip(request: Request) -> str:
    if TRUST_PROXY_HEADERS:
        forwarded_for = request.headers.get("x-forwarded-for", "").strip()
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        real_ip = request.headers.get("x-real-ip", "").strip()
        if real_ip:
            return real_ip
    if request.client and request.client.host:
        return request.client.host
    return ""


def ip_is_allowed(client_ip: str) -> bool:
    if not ALLOWED_IP_NETWORKS:
        return True
    try:
        address = ip_address(client_ip)
    except ValueError:
        return False
    return any(address in network for network in ALLOWED_IP_NETWORKS)


def get_admin_password_hash() -> str:
    if not ADMIN_PASSWORD:
        return ""
    return hashlib.sha256(ADMIN_PASSWORD.encode("utf-8")).hexdigest()


def is_admin_authenticated(request: Request) -> bool:
    expected = get_admin_password_hash()
    if not expected:
        return True
    actual = request.cookies.get(ADMIN_AUTH_COOKIE, "")
    if not actual:
        return False
    return hmac.compare_digest(actual, expected)


def requires_admin_auth(path: str) -> bool:
    if not ADMIN_PASSWORD:
        return False
    if path == ADMIN_PATH:
        return True
    return any(path.startswith(prefix) for prefix in ADMIN_API_PREFIXES)


@app.middleware("http")
async def restrict_interface_by_ip(request: Request, call_next):
    if not ALLOWED_IP_NETWORKS:
        return await call_next(request)

    path = request.url.path
    if path == "/api/health":
        return await call_next(request)

    client_ip = get_client_ip(request)
    if ip_is_allowed(client_ip):
        return await call_next(request)

    LOGGER.warning(
        "ip access denied path=%s client_ip=%s forwarded_for=%s real_ip=%s remote_addr=%s allowed_ips=%s",
        path,
        client_ip or "unknown",
        request.headers.get("x-forwarded-for", ""),
        request.headers.get("x-real-ip", ""),
        request.client.host if request.client else "",
        ",".join(str(network) for network in ALLOWED_IP_NETWORKS) or "none",
    )

    if path.startswith("/api/"):
        return JSONResponse(
            status_code=403,
            content={"detail": f"access denied for ip {client_ip or 'unknown'}"},
        )
    return HTMLResponse("Forbidden", status_code=403)


@app.middleware("http")
async def protect_admin_with_password(request: Request, call_next):
    path = request.url.path
    if path in ("/api/admin-auth/login", "/api/admin-auth/logout", "/api/health"):
        return await call_next(request)
    if not requires_admin_auth(path):
        return await call_next(request)
    if is_admin_authenticated(request):
        return await call_next(request)

    if path.startswith("/api/"):
        return JSONResponse(status_code=401, content={"detail": "admin authentication required"})
    return await call_next(request)


def parse_filters(
    start_ts: Optional[int],
    end_ts: Optional[int],
    token_id: Optional[int],
    user_id: Optional[int],
    model_name: Optional[str],
    group: Optional[str],
) -> Dict[str, Any]:
    now_ts = int(datetime.now().timestamp())
    if end_ts is None:
        end_ts = now_ts
    if start_ts is None:
        start_ts = int((datetime.now() - timedelta(days=7)).timestamp())

    return {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "token_id": token_id,
        "user_id": user_id,
        "model_name": model_name,
        "group": group,
    }


@app.get("/", response_class=HTMLResponse)
def home_page(request: Request):
    return templates.TemplateResponse(request, "leaderboard.html", {"admin_path": ADMIN_PATH})


@app.get(ADMIN_PATH, response_class=HTMLResponse)
def admin_dashboard(request: Request):
    if ADMIN_PASSWORD and not is_admin_authenticated(request):
        return templates.TemplateResponse(request, "admin_login.html", {"admin_path": ADMIN_PATH})
    return templates.TemplateResponse(request, "admin.html", {"admin_path": ADMIN_PATH})


@app.get("/analytics", response_class=HTMLResponse)
def analytics_page(request: Request):
    return templates.TemplateResponse(request, "index.html", {"admin_path": ADMIN_PATH})


@app.get("/leaderboard", response_class=HTMLResponse)
def leaderboard_page(request: Request):
    return templates.TemplateResponse(request, "leaderboard.html", {"admin_path": ADMIN_PATH})


@app.get("/api/health")
def health():
    workers = getattr(app.state, "workers", None)
    return {
        "ok": True,
        "workers": workers.snapshot() if workers else {},
    }


@app.post("/api/admin-auth/login")
def admin_login(payload: AdminLoginPayload):
    if not ADMIN_PASSWORD:
        return {"ok": True, "admin_path": ADMIN_PATH}
    if not hmac.compare_digest(payload.password, ADMIN_PASSWORD):
        raise HTTPException(status_code=401, detail="invalid admin password")
    response = JSONResponse({"ok": True, "admin_path": ADMIN_PATH})
    response.set_cookie(
        ADMIN_AUTH_COOKIE,
        value=get_admin_password_hash(),
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=86400 * 7,
    )
    return response


@app.post("/api/admin-auth/logout")
def admin_logout():
    response = JSONResponse({"ok": True})
    response.delete_cookie(ADMIN_AUTH_COOKIE)
    return response


@app.get("/api/admin-summary")
def admin_summary():
    return get_admin_summary()


@app.get("/api/rules")
def rules():
    return {"items": list_rules()}


@app.post("/api/rules")
def create_rule_api(payload: RulePayload):
    try:
        return create_rule(payload)
    except RuleValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.put("/api/rules/{rule_id}")
def update_rule_api(rule_id: int, payload: RulePayload):
    try:
        return update_rule(rule_id, payload)
    except RuleValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.delete("/api/rules/{rule_id}")
def delete_rule_api(rule_id: int):
    try:
        delete_rule(rule_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True}


@app.get("/api/events")
def events(limit: int = Query(default=50, ge=1, le=200)):
    return list_events(limit=limit)


@app.get("/api/channels/auto-disabled")
def auto_disabled_channels():
    return {"items": list_auto_disabled_channels()}


@app.get("/api/channels/unstable-disabled")
def unstable_disabled_channels():
    return {"items": list_unstable_disabled_channels()}


@app.get("/api/channels/recovery-state")
def recovery_state():
    return list_recovery_states()


@app.post("/api/channels/{channel_id}/manual-restore")
def manual_restore_channel_api(channel_id: int):
    try:
        return manual_restore_channel(channel_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/filters")
def filters():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT token_id, token_name FROM logs WHERE token_id IS NOT NULL ORDER BY token_id")
            tokens = cur.fetchall()

            cur.execute("SELECT DISTINCT user_id, username FROM logs WHERE user_id IS NOT NULL ORDER BY user_id")
            users = cur.fetchall()

            cur.execute("SELECT DISTINCT model_name FROM logs WHERE model_name <> '' ORDER BY model_name")
            models = cur.fetchall()

            cur.execute("SELECT DISTINCT `group` FROM logs WHERE `group` IS NOT NULL AND `group` <> '' ORDER BY `group`")
            groups = cur.fetchall()

    return {
        "tokens": tokens,
        "users": users,
        "models": [m["model_name"] for m in models],
        "groups": [g["group"] for g in groups],
    }


@app.get("/api/overview")
def overview(
    start_ts: Optional[int] = Query(default=None),
    end_ts: Optional[int] = Query(default=None),
    token_id: Optional[int] = Query(default=None),
    user_id: Optional[int] = Query(default=None),
    model_name: Optional[str] = Query(default=None),
    group: Optional[str] = Query(default=None),
):
    params = parse_filters(start_ts, end_ts, token_id, user_id, model_name, group)
    sql, values = overview_sql(params)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)
            row = cur.fetchone()

    requests = int(row["requests"] or 0)
    success_requests = int(row["success_requests"] or 0)
    error_requests = int(row["error_requests"] or 0)

    row["success_rate"] = round((success_requests / requests) * 100, 2) if requests else 0
    row["error_rate"] = round((error_requests / requests) * 100, 2) if requests else 0
    row["filters"] = params
    return row


@app.get("/api/token-model-usage")
def token_model_usage(
    start_ts: Optional[int] = Query(default=None),
    end_ts: Optional[int] = Query(default=None),
    token_id: Optional[int] = Query(default=None),
    user_id: Optional[int] = Query(default=None),
    model_name: Optional[str] = Query(default=None),
    group: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
):
    params = parse_filters(start_ts, end_ts, token_id, user_id, model_name, group)
    sql, values = token_model_usage_sql(params, limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)
            rows = cur.fetchall()

    return {"items": rows, "filters": params}


@app.get("/api/ip-usage-details")
def ip_usage_details(
    dimension: str = Query(default="user", pattern="^(user|token|model)$"),
    token_id: Optional[int] = Query(default=None),
    user_id: Optional[int] = Query(default=None),
    model_name: Optional[str] = Query(default=None),
    start_ts: Optional[int] = Query(default=None),
    end_ts: Optional[int] = Query(default=None),
    filter_token_id: Optional[int] = Query(default=None),
    filter_user_id: Optional[int] = Query(default=None),
    filter_model_name: Optional[str] = Query(default=None),
    group: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
):
    params = parse_filters(
        start_ts=start_ts,
        end_ts=end_ts,
        token_id=filter_token_id,
        user_id=filter_user_id,
        model_name=filter_model_name,
        group=group,
    )

    if dimension == "user":
        if user_id is None:
            raise HTTPException(status_code=400, detail="user_id is required for user dimension")
        detail_value = user_id
    elif dimension == "model":
        if not model_name:
            raise HTTPException(status_code=400, detail="model_name is required for model dimension")
        detail_value = model_name
    else:
        if token_id is None:
            raise HTTPException(status_code=400, detail="token_id is required for token dimension")
        detail_value = token_id

    summary_sql, summary_values = ip_usage_summary_sql(params, dimension, detail_value)
    detail_sql, detail_values = ip_usage_detail_sql(params, dimension, detail_value, limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(summary_sql, summary_values)
            summary = cur.fetchone()

            cur.execute(detail_sql, detail_values)
            rows = cur.fetchall()

    return {
        "dimension": dimension,
        "summary": summary,
        "items": rows,
        "filters": params,
    }


@app.get("/api/rankings")
def rankings(
    dimension: str = Query(default="token", pattern="^(user|token|model)$"),
    metric: str = Query(default="quota", pattern="^(quota|requests|tokens)$"),
    start_ts: Optional[int] = Query(default=None),
    end_ts: Optional[int] = Query(default=None),
    token_id: Optional[int] = Query(default=None),
    user_id: Optional[int] = Query(default=None),
    model_name: Optional[str] = Query(default=None),
    group: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
):
    params = parse_filters(start_ts, end_ts, token_id, user_id, model_name, group)
    sql, values = rankings_sql(params, dimension, metric, limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)
            rows = cur.fetchall()

    return {
        "dimension": dimension,
        "metric": metric,
        "items": rows,
        "filters": params,
    }


@app.get("/api/recent-logs")
def recent_logs(
    start_ts: Optional[int] = Query(default=None),
    end_ts: Optional[int] = Query(default=None),
    token_id: Optional[int] = Query(default=None),
    user_id: Optional[int] = Query(default=None),
    model_name: Optional[str] = Query(default=None),
    group: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    params = parse_filters(start_ts, end_ts, token_id, user_id, model_name, group)
    sql, values = recent_logs_sql(params, limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)
            rows = cur.fetchall()

    return {"items": rows, "filters": params}


@app.get("/api/leaderboard")
def leaderboard(
    start_ts: Optional[int] = Query(default=None),
    end_ts: Optional[int] = Query(default=None),
    group: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
):
    params = parse_filters(
        start_ts=start_ts,
        end_ts=end_ts,
        token_id=None,
        user_id=None,
        model_name=None,
        group=group,
    )

    summary_sql, summary_values = leaderboard_token_summary_sql(params, limit)
    model_sql, model_values = leaderboard_token_model_sql(params)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(summary_sql, summary_values)
            summary_rows = cur.fetchall()

            cur.execute(model_sql, model_values)
            token_model_rows = cur.fetchall()

    def fnum(v: Any) -> float:
        if v is None:
            return 0.0
        return float(v)

    def inum(v: Any) -> int:
        if v is None:
            return 0
        return int(v)

    model_map: Dict[int, list] = {}
    for row in token_model_rows:
        token_id = row["token_id"]
        if token_id not in model_map:
            model_map[token_id] = []
        model_map[token_id].append(row)

    max_tokens = max((fnum(r["total_tokens"]) for r in summary_rows), default=1.0)
    max_requests = max((fnum(r["requests"]) for r in summary_rows), default=1.0)
    max_model_count = max((fnum(r["model_count"]) for r in summary_rows), default=1.0)
    max_active_days = max((fnum(r["active_days"]) for r in summary_rows), default=1.0)

    items = []
    for idx, row in enumerate(summary_rows, start=1):
        token_id = row["token_id"]
        model_rows = model_map.get(token_id, [])
        top_model = model_rows[0]["model_name"] if model_rows else "-"
        top_models = [m["model_name"] for m in model_rows[:5]]

        total_tokens = fnum(row["total_tokens"])
        requests = inum(row["requests"])
        model_count = inum(row["model_count"])
        active_days = inum(row["active_days"])

        workload_index = (
            (total_tokens / max_tokens) * 60
            + (requests / max_requests) * 25
            + (model_count / max_model_count) * 10
            + (active_days / max_active_days) * 5
        )

        item = {
            **row,
            "requests": requests,
            "total_prompt_tokens": inum(row["total_prompt_tokens"]),
            "total_completion_tokens": inum(row["total_completion_tokens"]),
            "total_tokens": inum(row["total_tokens"]),
            "total_quota": inum(row["total_quota"]),
            "model_count": model_count,
            "active_days": active_days,
            "rank": idx,
            "top_model": top_model,
            "models_used_top5": top_models,
            "workload_index": round(workload_index, 2),
        }
        items.append(item)

    return {"items": items, "filters": params}


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))
    uvicorn.run("app.main:app", host=host, port=port, reload=False)
