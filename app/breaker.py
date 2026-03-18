import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from pydantic import BaseModel, Field

from app.db import get_conn
from app.breaker_logic import (
    event_matches_rule,
    interpret_probe_response,
    json_text,
    normalize_int_list,
    normalize_str_list,
    parse_json_text,
    parse_csv_items,
    join_csv_items,
    remove_model_from_list,
    restore_model_to_list,
)


LOGGER = logging.getLogger("channel_breaker")
SERVICE_NAME = "channel_breaker"
AUTO_DISABLED_STATUS = 3
ENABLED_STATUS = 1
CHECKPOINT_NAME = "log_scanner"
RECOVERY_SUCCESS_TARGET = 3
SCAN_BATCH_SIZE = int(os.getenv("SCAN_BATCH_SIZE", "200"))
SCAN_INTERVAL_SECONDS = float(os.getenv("SCAN_INTERVAL_SECONDS", "2"))
RECOVERY_INTERVAL_SECONDS = float(os.getenv("RECOVERY_INTERVAL_SECONDS", "10"))
PROBE_TIMEOUT_SECONDS = float(os.getenv("PROBE_TIMEOUT_SECONDS", "8"))
PROBE_URL_TEMPLATE = os.getenv(
    "CHANNEL_TEST_URL_TEMPLATE",
    "https://api.zhitong.work/api/channel/test/{channel_id}?model={model}",
)
PROBE_STREAM_ENABLED = os.getenv("CHANNEL_TEST_STREAM", "1") != "0"
PROBE_HEADERS = {
    "new-api-user": os.getenv("CHANNEL_TEST_USER", "1"),
    "Authorization": os.getenv("CHANNEL_TEST_AUTHORIZATION", ""),
}
NEW_API_BASE_URL = os.getenv("NEW_API_BASE_URL", "").strip().rstrip("/")
NEW_API_ACCESS_TOKEN = os.getenv("NEW_API_ACCESS_TOKEN", "").strip()
NEW_API_USER_ID = os.getenv("NEW_API_USER_ID", "").strip()
NEW_API_REFRESH_TIMEOUT_SECONDS = float(os.getenv("NEW_API_REFRESH_TIMEOUT_SECONDS", "10"))
WECOM_ROBOT_WEBHOOK = os.getenv("WECOM_ROBOT_WEBHOOK", "").strip()
WECOM_NOTIFY_TIMEOUT_SECONDS = float(os.getenv("WECOM_NOTIFY_TIMEOUT_SECONDS", "5"))
NEW_API_ADMIN_ROLE_MIN = 10
NEW_API_USER_STATUS_ENABLED = 1

RULE_JSON_FIELDS = (
    "match_channel_ids",
    "match_groups",
    "match_models",
    "match_error_text",
    "match_error_codes",
    "match_status_codes",
    "match_request_paths",
)
ACTION_DISABLE_CHANNEL = "disable_channel"
ACTION_RESTORE_CHANNEL = "restore_channel"
ACTION_DISABLE_MODEL = "disable_model"
ACTION_RESTORE_MODEL = "restore_model"
ACTION_DISABLE_UNSTABLE_CHANNEL = "disable_unstable_channel"


class RulePayload(BaseModel):
    name: str
    enabled: bool = True
    priority: int = 0
    match_channel_ids: List[int] = Field(default_factory=list)
    match_groups: List[str] = Field(default_factory=list)
    match_models: List[str] = Field(default_factory=list)
    match_error_text: List[str] = Field(default_factory=list)
    match_error_codes: List[str] = Field(default_factory=list)
    match_status_codes: List[int] = Field(default_factory=list)
    match_request_paths: List[str] = Field(default_factory=list)
    window_seconds: int = 300
    threshold_count: int = 3
    action_type: str = "disable_channel"


class RuleValidationError(ValueError):
    pass


@dataclass
class WorkerStatus:
    running: bool = False
    last_run_at: Optional[int] = None
    last_success_at: Optional[int] = None
    last_error: str = ""
    processed_count: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "running": self.running,
            "last_run_at": self.last_run_at,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "processed_count": self.processed_count,
            "extra": self.extra,
        }


class WorkerManager:
    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.status = {
            "log_scanner": WorkerStatus(),
            "recovery_probe": WorkerStatus(),
        }
        self._threads: List[threading.Thread] = []

    def start(self) -> None:
        if self._threads:
            return
        self._threads = [
            threading.Thread(target=self._scan_loop, name="log-scanner", daemon=True),
            threading.Thread(target=self._recovery_loop, name="recovery-probe", daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        for thread in self._threads:
            thread.join(timeout=5)
        self._threads = []

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        return {name: status.to_dict() for name, status in self.status.items()}

    def _scan_loop(self) -> None:
        status = self.status["log_scanner"]
        while not self.stop_event.is_set():
            status.running = True
            status.last_run_at = now_ts()
            try:
                processed = scan_error_logs_once(batch_size=SCAN_BATCH_SIZE)
                status.processed_count += processed
                status.last_success_at = now_ts()
                status.last_error = ""
                status.extra = {"last_batch_size": processed}
                wait_time = 0 if processed >= SCAN_BATCH_SIZE else SCAN_INTERVAL_SECONDS
            except Exception as exc:
                LOGGER.exception("log scanner loop failed")
                status.last_error = str(exc)
                wait_time = SCAN_INTERVAL_SECONDS
            self.stop_event.wait(wait_time)
        status.running = False

    def _recovery_loop(self) -> None:
        status = self.status["recovery_probe"]
        while not self.stop_event.is_set():
            status.running = True
            status.last_run_at = now_ts()
            try:
                processed = probe_recovery_candidates()
                status.processed_count += processed["total"]
                status.last_success_at = now_ts()
                status.last_error = ""
                status.extra = processed
            except Exception as exc:
                LOGGER.exception("recovery probe loop failed")
                status.last_error = str(exc)
            self.stop_event.wait(RECOVERY_INTERVAL_SECONDS)
        status.running = False


def now_ts() -> int:
    return int(time.time())


def serialize_rule_row(row: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(row)
    for field_name in RULE_JSON_FIELDS:
        item[field_name] = parse_json_text(item.get(field_name), [])
    item["enabled"] = bool(item.get("enabled"))
    item["priority"] = int(item.get("priority") or 0)
    item["window_seconds"] = int(item.get("window_seconds") or 0)
    item["threshold_count"] = int(item.get("threshold_count") or 0)
    return item


def payload_to_record(payload: RulePayload) -> Dict[str, Any]:
    action_type = (payload.action_type or "").strip() or ACTION_DISABLE_CHANNEL
    if action_type not in {ACTION_DISABLE_CHANNEL, ACTION_DISABLE_MODEL, ACTION_DISABLE_UNSTABLE_CHANNEL}:
        action_type = ACTION_DISABLE_CHANNEL
    match_channel_ids = normalize_int_list(payload.match_channel_ids)
    match_groups = normalize_str_list(payload.match_groups)
    match_models = normalize_str_list(payload.match_models)
    match_error_text = normalize_str_list(payload.match_error_text)
    match_error_codes = normalize_str_list(payload.match_error_codes)
    match_status_codes = normalize_int_list(payload.match_status_codes)
    match_request_paths = normalize_str_list(payload.match_request_paths)
    if not any(
        [
            match_channel_ids,
            match_groups,
            match_models,
            match_error_text,
            match_error_codes,
            match_status_codes,
            match_request_paths,
        ]
    ):
        raise RuleValidationError("at least one match condition is required; all-empty global rules are not allowed")
    return {
        "name": payload.name.strip(),
        "enabled": 1 if payload.enabled else 0,
        "priority": int(payload.priority),
        "match_channel_ids": json_text(match_channel_ids),
        "match_groups": json_text(match_groups),
        "match_models": json_text(match_models),
        "match_error_text": json_text(match_error_text),
        "match_error_codes": json_text(match_error_codes),
        "match_status_codes": json_text(match_status_codes),
        "match_request_paths": json_text(match_request_paths),
        "window_seconds": max(1, int(payload.window_seconds)),
        "threshold_count": max(1, int(payload.threshold_count)),
        "action_type": action_type,
    }


def compute_hit_key(rule: Dict[str, Any], event: Dict[str, Any]) -> str:
    if rule["action_type"] == ACTION_DISABLE_MODEL:
        return f"{rule['id']}:{event['channel_id']}:{event['model_name']}"
    return f"{rule['id']}:{event['channel_id']}"


def normalize_ability_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "group": row.get("group") or "",
        "model": row.get("model") or "",
        "channel_id": int(row.get("channel_id") or 0),
        "enabled": int(row.get("enabled") or 0),
        "priority": int(row.get("priority") or 0),
        "weight": int(row.get("weight") or 0),
        "tag": row.get("tag"),
    }


def is_service_managed_disabled(other_info: Dict[str, Any]) -> bool:
    breaker = other_info.get("breaker", {}) if isinstance(other_info, dict) else {}
    return breaker.get("managed_by") == SERVICE_NAME and breaker.get("state") == "auto_disabled"


def is_service_managed_unstable_disabled(other_info: Dict[str, Any]) -> bool:
    breaker = other_info.get("breaker", {}) if isinstance(other_info, dict) else {}
    return breaker.get("managed_by") == SERVICE_NAME and breaker.get("state") == "unstable_disabled"


def channel_disabled_by_model_depletion(other_info: Dict[str, Any]) -> bool:
    breaker = other_info.get("breaker", {}) if isinstance(other_info, dict) else {}
    return bool(breaker.get("auto_disabled_by_model_depletion"))


def should_execute_disable_action(channel_id: int, probe_model: str) -> tuple[bool, str, Dict[str, Any]]:
    probe_target = str(probe_model or "").strip()
    if not probe_target:
        return False, "probe skipped: no model available", {"success": False, "detail": "no model available"}
    success, detail, payload = perform_channel_probe(channel_id, probe_target)
    if success:
        return False, "probe succeeded; skip disable", payload
    return True, detail, payload


def refresh_new_api_channel_cache(channel_id: int) -> bool:
    if not NEW_API_BASE_URL:
        return False

    access_token, user_id = resolve_new_api_auth()
    if not access_token or not user_id:
        return False

    url = f"{NEW_API_BASE_URL}/api/channel/"
    payload = json_text({"id": int(channel_id)}).encode("utf-8")
    req = urllib_request.Request(
        url=url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": access_token,
            "New-Api-User": user_id,
        },
        method="PUT",
    )
    try:
        with urllib_request.urlopen(req, timeout=NEW_API_REFRESH_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            if resp.getcode() >= 400:
                LOGGER.warning("new-api channel refresh failed channel_id=%s status=%s body=%s", channel_id, resp.getcode(), body[:500])
                return False
            LOGGER.info("new-api channel refresh succeeded channel_id=%s", channel_id)
            return True
    except urllib_error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        LOGGER.warning("new-api channel refresh failed channel_id=%s status=%s body=%s", channel_id, exc.code, body[:500])
        return False
    except urllib_error.URLError as exc:
        LOGGER.warning("new-api channel refresh network error channel_id=%s detail=%s", channel_id, exc.reason)
        return False


def resolve_new_api_auth(log_missing: bool = True) -> tuple[str, str]:
    if NEW_API_ACCESS_TOKEN and NEW_API_USER_ID:
        return NEW_API_ACCESS_TOKEN, NEW_API_USER_ID

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, access_token
                FROM users
                WHERE status = %s
                  AND role >= %s
                  AND deleted_at IS NULL
                  AND access_token IS NOT NULL
                  AND access_token <> ''
                ORDER BY role DESC, id ASC
                LIMIT 1
                """,
                (NEW_API_USER_STATUS_ENABLED, NEW_API_ADMIN_ROLE_MIN),
            )
            row = cur.fetchone()

    if not row:
        if log_missing:
            LOGGER.warning("new-api channel refresh auth unavailable: no enabled admin user with access_token found")
        return "", ""

    access_token = str(row.get("access_token") or "").strip()
    user_id = str(int(row.get("id") or 0))
    if not access_token or user_id == "0":
        if log_missing:
            LOGGER.warning("new-api channel refresh auth unavailable: invalid admin user row")
        return "", ""

    LOGGER.info("new-api channel refresh auth loaded from database user_id=%s", user_id)
    return access_token, user_id


def get_new_api_refresh_status() -> Dict[str, Any]:
    if not NEW_API_BASE_URL:
        return {
            "configured": False,
            "ready": False,
            "source": "disabled",
            "warning": "",
        }

    if NEW_API_ACCESS_TOKEN and NEW_API_USER_ID:
        return {
            "configured": True,
            "ready": True,
            "source": "env",
            "warning": "",
        }

    access_token, user_id = resolve_new_api_auth(log_missing=False)
    if access_token and user_id:
        return {
            "configured": True,
            "ready": True,
            "source": "database",
            "warning": "",
        }

    return {
        "configured": True,
        "ready": False,
        "source": "database",
        "warning": "未检测到已启用管理员的 access_token，new-api 缓存刷新不会生效。请前往 new-api 后台：个人设置 -> 安全设置 -> 系统访问令牌，点击生成令牌；或显式配置 NEW_API_ACCESS_TOKEN 和 NEW_API_USER_ID。",
    }


def build_model_snapshot(
    channel: Dict[str, Any],
    ability_rows: List[Dict[str, Any]],
    model_name: str,
    source_log_ids: List[int],
    rule: Dict[str, Any],
    channel_disabled_due_to_model_depletion: bool,
) -> Dict[str, Any]:
    return {
        "model_name": model_name,
        "original_models": parse_csv_items(channel.get("models")),
        "original_test_model": str(channel.get("test_model") or "").strip(),
        "original_abilities": [normalize_ability_row(row) for row in ability_rows],
        "source_log_ids": source_log_ids,
        "source_rule_id": rule["id"],
        "source_rule_name": rule["name"],
        "channel_disabled_due_to_model_depletion": channel_disabled_due_to_model_depletion,
    }


def build_model_depleted_other_info(
    other_info: Dict[str, Any],
    reason: str,
    ts: int,
    rule: Dict[str, Any],
    source_log_ids: List[int],
    model_name: str,
) -> Dict[str, Any]:
    info = dict(other_info or {})
    info["status_reason"] = reason
    info["status_time"] = ts
    info["breaker"] = {
        "managed_by": SERVICE_NAME,
        "state": "auto_disabled",
        "disabled_at": ts,
        "recovered_at": None,
        "source_rule_id": rule["id"],
        "source_rule_name": rule["name"],
        "source_log_ids": source_log_ids,
        "probe_model": "",
        "auto_disabled_by_model_depletion": True,
        "source_model_name": model_name,
        "recovery_policy": {
            "interval_seconds": RECOVERY_INTERVAL_SECONDS,
            "required_success_count": RECOVERY_SUCCESS_TARGET,
        },
    }
    return info


def build_unstable_disabled_other_info(
    other_info: Dict[str, Any],
    reason: str,
    ts: int,
    rule: Dict[str, Any],
    source_log_ids: List[int],
    hit_count: int,
) -> Dict[str, Any]:
    info = dict(other_info or {})
    info["status_reason"] = reason
    info["status_time"] = ts
    info["breaker"] = {
        "managed_by": SERVICE_NAME,
        "state": "unstable_disabled",
        "disabled_at": ts,
        "recovered_at": None,
        "requires_manual_recovery": True,
        "source_rule_id": rule["id"],
        "source_rule_name": rule["name"],
        "source_log_ids": source_log_ids,
        "disable_count_within_window": hit_count,
        "stability_window_seconds": int(rule["window_seconds"]),
        "stability_threshold_count": int(rule["threshold_count"]),
    }
    return info


def list_rules() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM rule_definitions ORDER BY priority DESC, id ASC")
            return [serialize_rule_row(row) for row in cur.fetchall()]


def create_rule(payload: RulePayload) -> Dict[str, Any]:
    record = payload_to_record(payload)
    ts = now_ts()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rule_definitions (
                  name, enabled, priority,
                  match_channel_ids, match_groups, match_models,
                  match_error_text, match_error_codes, match_status_codes, match_request_paths,
                  window_seconds, threshold_count, action_type,
                  created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    record["name"],
                    record["enabled"],
                    record["priority"],
                    record["match_channel_ids"],
                    record["match_groups"],
                    record["match_models"],
                    record["match_error_text"],
                    record["match_error_codes"],
                    record["match_status_codes"],
                    record["match_request_paths"],
                    record["window_seconds"],
                    record["threshold_count"],
                    record["action_type"],
                    ts,
                    ts,
                ),
            )
            rule_id = cur.lastrowid
            cur.execute("SELECT * FROM rule_definitions WHERE id = %s", (rule_id,))
            return serialize_rule_row(cur.fetchone())


def update_rule(rule_id: int, payload: RulePayload) -> Dict[str, Any]:
    record = payload_to_record(payload)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE rule_definitions
                SET name = %s,
                    enabled = %s,
                    priority = %s,
                    match_channel_ids = %s,
                    match_groups = %s,
                    match_models = %s,
                    match_error_text = %s,
                    match_error_codes = %s,
                    match_status_codes = %s,
                    match_request_paths = %s,
                    window_seconds = %s,
                    threshold_count = %s,
                    action_type = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    record["name"],
                    record["enabled"],
                    record["priority"],
                    record["match_channel_ids"],
                    record["match_groups"],
                    record["match_models"],
                    record["match_error_text"],
                    record["match_error_codes"],
                    record["match_status_codes"],
                    record["match_request_paths"],
                    record["window_seconds"],
                    record["threshold_count"],
                    record["action_type"],
                    now_ts(),
                    rule_id,
                ),
            )
            cur.execute("SELECT * FROM rule_definitions WHERE id = %s", (rule_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"rule {rule_id} not found")
            return serialize_rule_row(row)


def delete_rule(rule_id: int) -> None:
    with get_conn(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM rule_definitions WHERE id = %s", (rule_id,))
                if not cur.fetchone():
                    raise ValueError(f"rule {rule_id} not found")
                cur.execute("DELETE FROM rule_hits WHERE rule_id = %s", (rule_id,))
                cur.execute("DELETE FROM rule_definitions WHERE id = %s", (rule_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def list_events(limit: int = 50) -> Dict[str, List[Dict[str, Any]]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  a.id,
                  a.channel_id,
                  c.name AS channel_name,
                  a.action_type,
                  a.reason,
                  a.source_rule_id,
                  a.source_log_ids,
                  a.before_status,
                  a.after_status,
                  a.created_at,
                  a.metadata_json
                FROM channel_action_audit a
                LEFT JOIN channels c ON c.id = a.channel_id
                ORDER BY a.id DESC
                LIMIT %s
                """,
                (limit,),
            )
            actions = []
            for row in cur.fetchall():
                item = dict(row)
                item["metadata_json"] = parse_json_text(item.get("metadata_json"), {})
                item["source_log_ids"] = parse_json_text(item.get("source_log_ids"), [])
                actions.append(item)

            cur.execute(
                """
                SELECT
                  rh.id,
                  rh.rule_id,
                  rd.name AS rule_name,
                  rh.log_id,
                  rh.channel_id,
                  c.name AS channel_name,
                  rh.matched_at,
                  rh.hit_key,
                  rh.snapshot_json
                FROM rule_hits rh
                LEFT JOIN rule_definitions rd ON rd.id = rh.rule_id
                LEFT JOIN channels c ON c.id = rh.channel_id
                ORDER BY rh.id DESC
                LIMIT %s
                """,
                (limit,),
            )
            hits = []
            for row in cur.fetchall():
                item = dict(row)
                item["snapshot_json"] = parse_json_text(item.get("snapshot_json"), {})
                hits.append(item)

    return {"actions": actions, "hits": hits}


def list_auto_disabled_channels() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  c.id,
                  c.name,
                  c.status,
                  c.test_model,
                  c.models,
                  c.base_url,
                  c.other_info,
                  c.`group`
                FROM channels c
                WHERE c.status = %s
                ORDER BY c.id DESC
                """
                ,
                (AUTO_DISABLED_STATUS,),
            )
            channel_rows = cur.fetchall()
            cur.execute("SELECT * FROM channel_recovery_state")
            channel_states = {int(row["channel_id"]): row for row in cur.fetchall()}
            cur.execute("SELECT * FROM channel_model_recovery_state ORDER BY channel_id ASC, model_name ASC")
            model_state_rows = cur.fetchall()

    model_state_map: Dict[int, List[Dict[str, Any]]] = {}
    for row in model_state_rows:
        model_state_map.setdefault(int(row["channel_id"]), []).append(dict(row))

    items: List[Dict[str, Any]] = []
    for row in channel_rows:
        other_info = parse_json_text(row.get("other_info"), {})
        if not is_service_managed_disabled(other_info):
            continue
        breaker_state = other_info.get("breaker", {}) if isinstance(other_info, dict) else {}
        channel_state = channel_states.get(int(row["id"]))
        model_states = model_state_map.get(int(row["id"]), [])
        probe_model = ""
        last_probe_at = None
        last_probe_result = None
        last_error = ""
        disabled_at = breaker_state.get("disabled_at")
        if channel_state:
            probe_model = channel_state.get("probe_model") or ""
            last_probe_at = channel_state.get("last_probe_at")
            last_probe_result = channel_state.get("last_probe_result")
            last_error = channel_state.get("last_error") or ""
            disabled_at = channel_state.get("disabled_at") or disabled_at
        elif model_states:
            probe_model = model_states[0]["model_name"] if len(model_states) == 1 else f"{len(model_states)} models"
            last_probe_at = max((row.get("last_probe_at") or 0) for row in model_states) or None
            latest_row = max(model_states, key=lambda item: (item.get("last_probe_at") or 0, item["model_name"]))
            last_probe_result = latest_row.get("last_probe_result")
            last_error = latest_row.get("last_error") or ""
            disabled_at = min((row.get("disabled_at") or disabled_at or 0) for row in model_states) or disabled_at
        items.append(
            {
                **row,
                "other_info": other_info,
                "disabled_reason": other_info.get("status_reason", ""),
                "breaker": breaker_state,
                "probe_model": probe_model,
                "last_probe_at": last_probe_at,
                "last_probe_result": last_probe_result,
                "last_error": last_error,
                "disabled_at": disabled_at,
                "model_recovery_count": len(model_states),
            }
        )
    return items


def list_unstable_disabled_channels() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  c.id,
                  c.name,
                  c.status,
                  c.test_model,
                  c.models,
                  c.base_url,
                  c.other_info,
                  c.`group`
                FROM channels c
                WHERE c.status = %s
                ORDER BY c.id DESC
                """,
                (AUTO_DISABLED_STATUS,),
            )
            rows = cur.fetchall()

    items: List[Dict[str, Any]] = []
    for row in rows:
        other_info = parse_json_text(row.get("other_info"), {})
        if not is_service_managed_unstable_disabled(other_info):
            continue
        breaker_state = other_info.get("breaker", {}) if isinstance(other_info, dict) else {}
        items.append(
            {
                **row,
                "other_info": other_info,
                "breaker": breaker_state,
                "disabled_reason": other_info.get("status_reason", ""),
                "disabled_at": breaker_state.get("disabled_at"),
                "requires_manual_recovery": bool(breaker_state.get("requires_manual_recovery")),
                "disable_count_within_window": int(breaker_state.get("disable_count_within_window") or 0),
            }
        )
    return items


def list_model_recovery_states() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  ms.channel_id,
                  ms.model_name,
                  ms.source_rule_id,
                  ms.consecutive_success_count,
                  ms.last_probe_at,
                  ms.last_probe_result,
                  ms.last_error,
                  ms.disabled_at,
                  ms.updated_at,
                  ms.snapshot_json,
                  c.name AS channel_name,
                  c.status AS channel_status,
                  c.test_model,
                  c.models,
                  c.other_info
                FROM channel_model_recovery_state ms
                JOIN channels c ON c.id = ms.channel_id
                ORDER BY ms.disabled_at DESC, ms.channel_id ASC, ms.model_name ASC
                """
            )
            rows = cur.fetchall()

    items: List[Dict[str, Any]] = []
    for row in rows:
        snapshot = parse_json_text(row.get("snapshot_json"), {})
        other_info = parse_json_text(row.get("other_info"), {})
        items.append(
            {
                **row,
                "snapshot_json": snapshot,
                "other_info": other_info,
                "channel_disabled_by_model_depletion": channel_disabled_by_model_depletion(other_info),
            }
        )
    return items


def list_recovery_states() -> Dict[str, List[Dict[str, Any]]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  c.id,
                  c.name,
                  c.status,
                  c.test_model,
                  c.models,
                  c.other_info,
                  rs.probe_model,
                  rs.source_rule_id,
                  rs.consecutive_success_count,
                  rs.last_probe_at,
                  rs.last_probe_result,
                  rs.last_error,
                  rs.disabled_at,
                  rs.updated_at
                FROM channels c
                JOIN channel_recovery_state rs ON rs.channel_id = c.id
                ORDER BY rs.disabled_at DESC
                """
            )
            channel_rows = cur.fetchall()

    channel_items: List[Dict[str, Any]] = []
    for row in channel_rows:
        other_info = parse_json_text(row.get("other_info"), {})
        breaker = other_info.get("breaker", {}) if isinstance(other_info, dict) else {}
        channel_items.append({**row, "other_info": other_info, "breaker": breaker})
    return {"channel_items": channel_items, "model_items": list_model_recovery_states()}


def get_admin_summary() -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM rule_definitions WHERE enabled = 1")
            enabled_rules = int(cur.fetchone()["total"] or 0)
            cur.execute("SELECT COUNT(*) AS total FROM channel_recovery_state")
            channel_recovery_pending = int(cur.fetchone()["total"] or 0)
            cur.execute("SELECT COUNT(*) AS total FROM channel_model_recovery_state")
            model_recovery_pending = int(cur.fetchone()["total"] or 0)
            cur.execute("SELECT COUNT(*) AS total FROM channel_action_audit WHERE action_type = %s", (ACTION_DISABLE_CHANNEL,))
            disabled_total = int(cur.fetchone()["total"] or 0)
            cur.execute("SELECT COUNT(*) AS total FROM channel_action_audit WHERE action_type = %s", (ACTION_RESTORE_CHANNEL,))
            restored_total = int(cur.fetchone()["total"] or 0)
            cur.execute("SELECT COUNT(*) AS total FROM channel_action_audit WHERE action_type = %s", (ACTION_DISABLE_MODEL,))
            disabled_model_total = int(cur.fetchone()["total"] or 0)
            cur.execute("SELECT COUNT(*) AS total FROM channel_action_audit WHERE action_type = %s", (ACTION_RESTORE_MODEL,))
            restored_model_total = int(cur.fetchone()["total"] or 0)
            cur.execute("SELECT COUNT(*) AS total FROM channel_action_audit WHERE action_type = %s", (ACTION_DISABLE_UNSTABLE_CHANNEL,))
            unstable_disabled_total = int(cur.fetchone()["total"] or 0)
            cur.execute("SELECT COUNT(*) AS total FROM rule_hits")
            hit_total = int(cur.fetchone()["total"] or 0)
    return {
        "enabled_rules": enabled_rules,
        "recovery_pending": channel_recovery_pending + model_recovery_pending,
        "channel_recovery_pending": channel_recovery_pending,
        "model_recovery_pending": model_recovery_pending,
        "disabled_total": disabled_total,
        "restored_total": restored_total,
        "disabled_model_total": disabled_model_total,
        "restored_model_total": restored_model_total,
        "unstable_disabled_total": unstable_disabled_total,
        "hit_total": hit_total,
        "new_api_refresh": get_new_api_refresh_status(),
    }


def scan_error_logs_once(batch_size: int = SCAN_BATCH_SIZE) -> int:
    last_log_id = get_checkpoint(CHECKPOINT_NAME)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  id,
                  created_at,
                  type,
                  content,
                  username,
                  model_name,
                  channel_id,
                  token_id,
                  `group`,
                  channel_name,
                  other,
                  request_id
                FROM logs
                WHERE id > %s AND type = 5
                ORDER BY id ASC
                LIMIT %s
                """,
                (last_log_id, batch_size),
            )
            rows = cur.fetchall()

    for row in rows:
        try:
            process_error_log(row)
        except Exception:
            LOGGER.exception("failed to process log id=%s", row["id"])
        finally:
            set_checkpoint(CHECKPOINT_NAME, int(row["id"]))
    return len(rows)


def process_error_log(row: Dict[str, Any]) -> None:
    event = build_log_event(row)
    if event["channel_id"] is None:
        return

    rules = get_enabled_rules()
    for rule in rules:
        if event_matches_rule(event, rule):
            apply_rule_hit_and_action(event, rule)


def get_enabled_rules() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM rule_definitions WHERE enabled = 1 ORDER BY priority DESC, id ASC")
            return [serialize_rule_row(row) for row in cur.fetchall()]


def build_log_event(row: Dict[str, Any]) -> Dict[str, Any]:
    other = parse_json_text(row.get("other"), {})
    channel_id = row.get("channel_id")
    if channel_id is None and isinstance(other, dict):
        channel_id = other.get("channel_id")
    status_code = other.get("status_code") if isinstance(other, dict) else None
    try:
        status_code = int(status_code) if status_code is not None else None
    except (TypeError, ValueError):
        status_code = None

    event = {
        "log_id": int(row["id"]),
        "created_at": int(row.get("created_at") or now_ts()),
        "channel_id": int(channel_id) if channel_id is not None else None,
        "group": row.get("group") or "",
        "model_name": row.get("model_name") or "",
        "content": row.get("content") or "",
        "request_id": row.get("request_id") or "",
        "channel_name": row.get("channel_name") or other.get("channel_name", ""),
        "error_code": other.get("error_code", "") if isinstance(other, dict) else "",
        "status_code": status_code,
        "request_path": other.get("request_path", "") if isinstance(other, dict) else "",
        "other": other if isinstance(other, dict) else {},
    }
    return event


def apply_rule_hit_and_action(event: Dict[str, Any], rule: Dict[str, Any]) -> None:
    hit_key = compute_hit_key(rule, event)
    should_refresh_cache = False
    unstable_notify_payload: Optional[Dict[str, Any]] = None
    with get_conn(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT IGNORE INTO rule_hits (
                      rule_id, log_id, channel_id, matched_at, hit_key, snapshot_json
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        rule["id"],
                        event["log_id"],
                        event["channel_id"],
                        event["created_at"],
                        hit_key,
                        json_text(event),
                    ),
                )
                inserted = cur.rowcount == 1
                if not inserted:
                    conn.commit()
                    return

                threshold_window_start = event["created_at"] - int(rule["window_seconds"]) + 1
                cur.execute(
                    """
                    SELECT COUNT(*) AS total
                    FROM rule_hits
                    WHERE rule_id = %s
                      AND hit_key = %s
                      AND matched_at >= %s
                    """,
                    (rule["id"], hit_key, threshold_window_start),
                )
                hit_count = int(cur.fetchone()["total"] or 0)
                if hit_count < int(rule["threshold_count"]):
                    conn.commit()
                    return

                cur.execute(
                    """
                    SELECT log_id
                    FROM rule_hits
                    WHERE rule_id = %s
                      AND hit_key = %s
                      AND matched_at >= %s
                    ORDER BY matched_at DESC, log_id DESC
                    LIMIT %s
                    """,
                    (rule["id"], hit_key, threshold_window_start, max(int(rule["threshold_count"]), 5)),
                )
                source_log_ids = [int(item["log_id"]) for item in cur.fetchall()]
                if rule["action_type"] == ACTION_DISABLE_MODEL:
                    should_refresh_cache = apply_disable_model_action(cur, event, rule, hit_count, source_log_ids)
                elif rule["action_type"] == ACTION_DISABLE_UNSTABLE_CHANNEL:
                    should_refresh_cache = apply_disable_unstable_channel_action(
                        cur, event, rule, hit_count, source_log_ids
                    )
                    if should_refresh_cache:
                        unstable_notify_payload = build_unstable_channel_notification_payload(
                            event=event,
                            rule=rule,
                            source_log_ids=source_log_ids,
                        )
                else:
                    should_refresh_cache = apply_disable_channel_action(cur, event, rule, hit_count, source_log_ids)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if should_refresh_cache:
        refresh_new_api_channel_cache(event["channel_id"])
    if unstable_notify_payload:
        send_wecom_markdown_async(unstable_notify_payload["title"], unstable_notify_payload["body"])


def apply_disable_channel_action(
    cur: Any,
    event: Dict[str, Any],
    rule: Dict[str, Any],
    hit_count: int,
    source_log_ids: List[int],
) -> bool:
    cur.execute(
        """
        SELECT id, name, status, test_model, models, other_info
        FROM channels
        WHERE id = %s
        FOR UPDATE
        """,
        (event["channel_id"],),
    )
    channel = cur.fetchone()
    if not channel:
        return False

    other_info = parse_json_text(channel.get("other_info"), {})
    breaker = other_info.get("breaker", {}) if isinstance(other_info, dict) else {}
    if int(channel.get("status") or 0) == AUTO_DISABLED_STATUS and breaker.get("managed_by") == SERVICE_NAME:
        return False
    if int(channel.get("status") or 0) == AUTO_DISABLED_STATUS and breaker.get("managed_by") != SERVICE_NAME:
        return False

    ts = now_ts()
    probe_model = choose_probe_model(channel, event["model_name"])
    should_disable, probe_detail, probe_payload = should_execute_disable_action(event["channel_id"], probe_model)
    if not should_disable:
        LOGGER.info(
            "skip channel disable after active probe succeeded channel_id=%s probe_model=%s detail=%s",
            event["channel_id"],
            probe_model,
            probe_detail,
        )
        return False
    reason = (
        f"Rule {rule['name']} triggered by {hit_count} error logs "
        f"within {int(rule['window_seconds'])} seconds; active probe failed: {probe_detail}"
    )
    new_other_info = build_disabled_other_info(
        other_info=other_info,
        reason=reason,
        ts=ts,
        rule=rule,
        source_log_ids=source_log_ids,
        probe_model=probe_model,
    )
    before_status = int(channel.get("status") or 0)
    cur.execute(
        "UPDATE channels SET status = %s, other_info = %s WHERE id = %s",
        (AUTO_DISABLED_STATUS, json_text(new_other_info), event["channel_id"]),
    )
    cur.execute(
        """
        INSERT INTO channel_action_audit (
          channel_id, action_type, reason, source_rule_id, source_log_ids,
          before_status, after_status, created_at, metadata_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            event["channel_id"],
            ACTION_DISABLE_CHANNEL,
            reason,
            rule["id"],
            json_text(source_log_ids),
            before_status,
            AUTO_DISABLED_STATUS,
            ts,
            json_text({"rule": rule["name"], "event": event, "probe_model": probe_model, "probe_payload": probe_payload}),
        ),
    )
    cur.execute(
        """
        INSERT INTO channel_recovery_state (
          channel_id, probe_model, source_rule_id, consecutive_success_count,
          last_probe_at, last_probe_result, last_error, disabled_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          probe_model = VALUES(probe_model),
          source_rule_id = VALUES(source_rule_id),
          consecutive_success_count = 0,
          last_probe_at = NULL,
          last_probe_result = NULL,
          last_error = NULL,
          disabled_at = VALUES(disabled_at),
          updated_at = VALUES(updated_at)
        """,
        (
            event["channel_id"],
            probe_model,
            rule["id"],
            0,
            None,
            None,
            None,
            ts,
            ts,
        ),
    )
    return True


def count_recent_channel_disables(cur: Any, channel_id: int, window_seconds: int) -> int:
    threshold_window_start = now_ts() - int(window_seconds)
    cur.execute(
        """
        SELECT COUNT(*) AS total
        FROM channel_action_audit
        WHERE channel_id = %s
          AND action_type = %s
          AND created_at >= %s
        """,
        (channel_id, ACTION_DISABLE_CHANNEL, threshold_window_start),
    )
    return int(cur.fetchone()["total"] or 0)


def apply_disable_unstable_channel_action(
    cur: Any,
    event: Dict[str, Any],
    rule: Dict[str, Any],
    hit_count: int,
    source_log_ids: List[int],
) -> bool:
    cur.execute(
        """
        SELECT id, name, status, test_model, models, other_info
        FROM channels
        WHERE id = %s
        FOR UPDATE
        """,
        (event["channel_id"],),
    )
    channel = cur.fetchone()
    if not channel:
        return False

    other_info = parse_json_text(channel.get("other_info"), {})
    breaker = other_info.get("breaker", {}) if isinstance(other_info, dict) else {}
    if int(channel.get("status") or 0) == AUTO_DISABLED_STATUS:
        return False

    recent_disable_count = count_recent_channel_disables(cur, event["channel_id"], int(rule["window_seconds"]))
    if recent_disable_count < int(rule["threshold_count"]):
        return False

    ts = now_ts()
    probe_model = choose_probe_model(channel, event["model_name"])
    should_disable, probe_detail, probe_payload = should_execute_disable_action(event["channel_id"], probe_model)
    if not should_disable:
        LOGGER.info(
            "skip unstable channel disable after active probe succeeded channel_id=%s probe_model=%s detail=%s",
            event["channel_id"],
            probe_model,
            probe_detail,
        )
        return False

    reason = (
        f"Rule {rule['name']} marked channel as unstable after {recent_disable_count} automatic disables "
        f"within {int(rule['window_seconds'])} seconds; active probe failed: {probe_detail}"
    )
    new_other_info = build_unstable_disabled_other_info(
        other_info=other_info,
        reason=reason,
        ts=ts,
        rule=rule,
        source_log_ids=source_log_ids,
        hit_count=recent_disable_count,
    )
    before_status = int(channel.get("status") or 0)
    cur.execute(
        "UPDATE channels SET status = %s, other_info = %s WHERE id = %s",
        (AUTO_DISABLED_STATUS, json_text(new_other_info), event["channel_id"]),
    )
    cur.execute("DELETE FROM channel_recovery_state WHERE channel_id = %s", (event["channel_id"],))
    cur.execute(
        """
        INSERT INTO channel_action_audit (
          channel_id, action_type, reason, source_rule_id, source_log_ids,
          before_status, after_status, created_at, metadata_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            event["channel_id"],
            ACTION_DISABLE_UNSTABLE_CHANNEL,
            reason,
            rule["id"],
            json_text(source_log_ids),
            before_status,
            AUTO_DISABLED_STATUS,
            ts,
            json_text(
                {
                    "rule": rule["name"],
                    "event": event,
                    "probe_model": probe_model,
                    "probe_payload": probe_payload,
                    "recent_disable_count": recent_disable_count,
                    "requires_manual_recovery": True,
                }
            ),
        ),
    )
    return True


def build_unstable_channel_notification_payload(
    event: Dict[str, Any],
    rule: Dict[str, Any],
    source_log_ids: List[int],
) -> Dict[str, str]:
    channel_id = int(event.get("channel_id") or 0)
    title = "New API 渠道稳定性告警"
    lines = [
        f"> 渠道 **{event.get('channel_name') or channel_id}** 已被标记为不稳定并禁用",
        f"> 渠道 ID：`{channel_id}`",
        f"> 规则：`{rule.get('name') or '-'}`",
        f"> 恢复策略：人工恢复",
        f"> 统计窗口：`{int(rule.get('window_seconds') or 0)}` 秒",
        f"> 禁用阈值：`{int(rule.get('threshold_count') or 0)}` 次自动禁用",
    ]
    if event.get("model_name"):
        lines.append(f"> 最近失败模型：`{event['model_name']}`")
    if event.get("error_code"):
        lines.append(f"> 错误码：`{event['error_code']}`")
    if event.get("status_code") is not None:
        lines.append(f"> 状态码：`{event['status_code']}`")
    if event.get("content"):
        lines.append(f"> 最近错误：{str(event['content'])[:160]}")
    if source_log_ids:
        lines.append(f"> 关联日志：`{', '.join(str(item) for item in source_log_ids[:5])}`")
    lines.append("")
    lines.append("请尽快人工确认该渠道状态，并在管理后台决定是否恢复。")
    return {"title": title, "body": "\n".join(lines)}


def send_wecom_markdown_async(title: str, body: str) -> None:
    if not WECOM_ROBOT_WEBHOOK:
        return
    thread = threading.Thread(
        target=_send_wecom_markdown,
        args=(title, body),
        name="wecom-robot-notify",
        daemon=True,
    )
    thread.start()


def _send_wecom_markdown(title: str, body: str) -> None:
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": f"## {title}\n{body}",
        },
    }
    req = urllib_request.Request(
        WECOM_ROBOT_WEBHOOK,
        data=json_text(payload).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=WECOM_NOTIFY_TIMEOUT_SECONDS) as response:
            raw_body = response.read().decode("utf-8", errors="replace")
            if response.getcode() >= 400:
                LOGGER.warning("wecom robot notify failed status=%s body=%s", response.getcode(), raw_body)
                return
            payload = parse_json_text(raw_body, {})
            if payload.get("errcode", 0) != 0:
                LOGGER.warning("wecom robot notify failed errcode=%s errmsg=%s", payload.get("errcode"), payload.get("errmsg"))
    except Exception as exc:
        LOGGER.warning("wecom robot notify error: %s", exc)


def apply_disable_model_action(
    cur: Any,
    event: Dict[str, Any],
    rule: Dict[str, Any],
    hit_count: int,
    source_log_ids: List[int],
) -> bool:
    model_name = str(event.get("model_name") or "").strip()
    if not model_name:
        return False

    cur.execute(
        """
        SELECT id, name, status, test_model, models, other_info, `group`
        FROM channels
        WHERE id = %s
        FOR UPDATE
        """,
        (event["channel_id"],),
    )
    channel = cur.fetchone()
    if not channel:
        return False

    other_info = parse_json_text(channel.get("other_info"), {})
    breaker = other_info.get("breaker", {}) if isinstance(other_info, dict) else {}
    if int(channel.get("status") or 0) == AUTO_DISABLED_STATUS and breaker.get("managed_by") == SERVICE_NAME:
        return False
    if int(channel.get("status") or 0) == AUTO_DISABLED_STATUS and breaker.get("managed_by") != SERVICE_NAME:
        return False

    cur.execute(
        """
        SELECT channel_id, model_name, last_probe_result
        FROM channel_model_recovery_state
        WHERE channel_id = %s AND model_name = %s
        """,
        (event["channel_id"], model_name),
    )
    if cur.fetchone():
        return False

    cur.execute(
        """
        SELECT `group`, model, channel_id, enabled, priority, weight, tag
        FROM abilities
        WHERE channel_id = %s AND model = %s
        FOR UPDATE
        """,
        (event["channel_id"], model_name),
    )
    ability_rows = cur.fetchall()

    original_models = parse_csv_items(channel.get("models"))
    new_models = remove_model_from_list(original_models, model_name)
    if new_models == original_models and not ability_rows:
        return False

    ts = now_ts()
    should_disable, probe_detail, probe_payload = should_execute_disable_action(event["channel_id"], model_name)
    if not should_disable:
        LOGGER.info(
            "skip model disable after active probe succeeded channel_id=%s model=%s detail=%s",
            event["channel_id"],
            model_name,
            probe_detail,
        )
        return False
    reason = (
        f"Rule {rule['name']} triggered by {hit_count} error logs "
        f"within {int(rule['window_seconds'])} seconds; model {model_name} removed from channel; active probe failed: {probe_detail}"
    )
    new_test_model = str(channel.get("test_model") or "").strip()
    if new_test_model == model_name:
        new_test_model = ""

    channel_disabled_due_to_model_depletion = len(new_models) == 0
    new_status = int(channel.get("status") or 0)
    new_other_info = other_info
    if channel_disabled_due_to_model_depletion:
        depletion_reason = f"All models removed after rule {rule['name']} disabled model {model_name}"
        new_status = AUTO_DISABLED_STATUS
        new_other_info = build_model_depleted_other_info(
            other_info=other_info,
            reason=depletion_reason,
            ts=ts,
            rule=rule,
            source_log_ids=source_log_ids,
            model_name=model_name,
        )

    cur.execute(
        "UPDATE channels SET models = %s, test_model = %s, status = %s, other_info = %s WHERE id = %s",
        (
            join_csv_items(new_models),
            new_test_model,
            new_status,
            json_text(new_other_info),
            event["channel_id"],
        ),
    )
    cur.execute("DELETE FROM abilities WHERE channel_id = %s AND model = %s", (event["channel_id"], model_name))

    snapshot = build_model_snapshot(
        channel=channel,
        ability_rows=ability_rows,
        model_name=model_name,
        source_log_ids=source_log_ids,
        rule=rule,
        channel_disabled_due_to_model_depletion=channel_disabled_due_to_model_depletion,
    )
    cur.execute(
        """
        INSERT INTO channel_action_audit (
          channel_id, action_type, reason, source_rule_id, source_log_ids,
          before_status, after_status, created_at, metadata_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            event["channel_id"],
            ACTION_DISABLE_MODEL,
            reason,
            rule["id"],
            json_text(source_log_ids),
            int(channel.get("status") or 0),
            new_status,
            ts,
            json_text(
                {
                    "rule": rule["name"],
                    "event": event,
                    "model_name": model_name,
                    "snapshot": snapshot,
                    "probe_payload": probe_payload,
                }
            ),
        ),
    )
    cur.execute(
        """
        INSERT INTO channel_model_recovery_state (
          channel_id, model_name, source_rule_id, consecutive_success_count,
          last_probe_at, last_probe_result, last_error, disabled_at, updated_at, snapshot_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          source_rule_id = VALUES(source_rule_id),
          consecutive_success_count = 0,
          last_probe_at = NULL,
          last_probe_result = NULL,
          last_error = NULL,
          disabled_at = VALUES(disabled_at),
          updated_at = VALUES(updated_at),
          snapshot_json = VALUES(snapshot_json)
        """,
        (
            event["channel_id"],
            model_name,
            rule["id"],
            0,
            None,
            None,
            None,
            ts,
            ts,
            json_text(snapshot),
        ),
    )
    if channel_disabled_due_to_model_depletion:
        cur.execute(
            """
            INSERT INTO channel_action_audit (
              channel_id, action_type, reason, source_rule_id, source_log_ids,
              before_status, after_status, created_at, metadata_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event["channel_id"],
                ACTION_DISABLE_CHANNEL,
                f"Channel auto-disabled because all models were removed after disabling {model_name}",
                rule["id"],
                json_text(source_log_ids),
                int(channel.get("status") or 0),
                AUTO_DISABLED_STATUS,
                ts,
                json_text(
                    {
                        "rule": rule["name"],
                        "event": event,
                        "trigger_model": model_name,
                        "source": ACTION_DISABLE_MODEL,
                        "probe_payload": probe_payload,
                    }
                ),
            ),
        )
    return True


def choose_probe_model(channel: Dict[str, Any], fallback_model: str) -> str:
    test_model = str(channel.get("test_model") or "").strip()
    if test_model:
        return test_model
    if fallback_model:
        return fallback_model
    models = str(channel.get("models") or "")
    for item in models.split(","):
        candidate = item.strip()
        if candidate:
            return candidate
    return ""


def build_disabled_other_info(
    other_info: Dict[str, Any],
    reason: str,
    ts: int,
    rule: Dict[str, Any],
    source_log_ids: List[int],
    probe_model: str,
) -> Dict[str, Any]:
    info = dict(other_info or {})
    info["status_reason"] = reason
    info["status_time"] = ts
    info["breaker"] = {
        "managed_by": SERVICE_NAME,
        "state": "auto_disabled",
        "disabled_at": ts,
        "recovered_at": None,
        "source_rule_id": rule["id"],
        "source_rule_name": rule["name"],
        "source_log_ids": source_log_ids,
        "probe_model": probe_model,
        "recovery_policy": {
            "interval_seconds": RECOVERY_INTERVAL_SECONDS,
            "required_success_count": RECOVERY_SUCCESS_TARGET,
        },
    }
    return info


def build_restored_other_info(other_info: Dict[str, Any], ts: int, reason: str) -> Dict[str, Any]:
    info = dict(other_info or {})
    breaker = info.get("breaker", {}) if isinstance(info.get("breaker"), dict) else {}
    breaker.update(
        {
            "managed_by": SERVICE_NAME,
            "state": "active",
            "recovered_at": ts,
            "last_restore_reason": reason,
            "auto_disabled_by_model_depletion": False,
            "requires_manual_recovery": False,
        }
    )
    info["breaker"] = breaker
    info["status_reason"] = ""
    info["status_time"] = ts
    info["recovery_reason"] = reason
    return info


def get_checkpoint(worker_name: str) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_log_id FROM worker_checkpoints WHERE worker_name = %s",
                (worker_name,),
            )
            row = cur.fetchone()
            if not row:
                return 0
            return int(row["last_log_id"] or 0)


def set_checkpoint(worker_name: str, last_log_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO worker_checkpoints (worker_name, last_log_id, updated_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  last_log_id = VALUES(last_log_id),
                  updated_at = VALUES(updated_at)
                """,
                (worker_name, last_log_id, now_ts()),
            )


def probe_recovery_candidates() -> Dict[str, int]:
    channel_states = list_recovery_states()
    channel_candidates = channel_states["channel_items"]
    model_candidates = channel_states["model_items"]

    channel_count = 0
    for candidate in channel_candidates:
        channel_count += 1
        try:
            probe_single_channel(candidate["id"])
        except Exception:
            LOGGER.exception("failed to probe channel=%s", candidate["id"])

    model_count = 0
    for candidate in model_candidates:
        model_count += 1
        try:
            probe_single_model(candidate["channel_id"], candidate["model_name"])
        except Exception:
            LOGGER.exception("failed to probe channel=%s model=%s", candidate["channel_id"], candidate["model_name"])

    return {
        "total": channel_count + model_count,
        "last_channel_probe_count": channel_count,
        "last_model_probe_count": model_count,
    }


def probe_single_channel(channel_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  c.id,
                  c.name,
                  c.status,
                  c.test_model,
                  c.models,
                  c.other_info,
                  rs.probe_model,
                  rs.source_rule_id,
                  rs.consecutive_success_count
                FROM channels c
                JOIN channel_recovery_state rs ON rs.channel_id = c.id
                WHERE c.id = %s
                """,
                (channel_id,),
            )
            channel = cur.fetchone()

    if not channel:
        delete_recovery_state(channel_id)
        return

    other_info = parse_json_text(channel.get("other_info"), {})
    breaker = other_info.get("breaker", {}) if isinstance(other_info, dict) else {}
    if int(channel.get("status") or 0) != AUTO_DISABLED_STATUS or breaker.get("managed_by") != SERVICE_NAME:
        delete_recovery_state(channel_id)
        return

    probe_model = str(channel.get("probe_model") or choose_probe_model(channel, "")).strip()
    if not probe_model:
        record_probe_failure(channel_id, "No test model available for probe")
        return

    success, detail, payload = perform_channel_probe(channel_id, probe_model)
    if success:
        record_probe_success(channel_id, probe_model, payload)
    else:
        record_probe_failure(channel_id, detail)


def probe_single_model(channel_id: int, model_name: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  c.id,
                  c.name,
                  c.status,
                  c.test_model,
                  c.models,
                  c.`group`,
                  c.other_info,
                  ms.model_name,
                  ms.source_rule_id,
                  ms.consecutive_success_count,
                  ms.snapshot_json
                FROM channels c
                JOIN channel_model_recovery_state ms
                  ON ms.channel_id = c.id AND ms.model_name = %s
                WHERE c.id = %s
                """,
                (model_name, channel_id),
            )
            channel = cur.fetchone()

    if not channel:
        delete_model_recovery_state(channel_id, model_name)
        return

    success, detail, payload = perform_channel_probe(channel_id, model_name)
    if success:
        record_model_probe_success(channel_id, model_name, payload)
    else:
        record_model_probe_failure(channel_id, model_name, detail)


def delete_recovery_state(channel_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM channel_recovery_state WHERE channel_id = %s", (channel_id,))


def delete_model_recovery_state(channel_id: int, model_name: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM channel_model_recovery_state WHERE channel_id = %s AND model_name = %s",
                (channel_id, model_name),
            )


def record_probe_failure(channel_id: int, error_message: str) -> None:
    ts = now_ts()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE channel_recovery_state
                SET consecutive_success_count = 0,
                    last_probe_at = %s,
                    last_probe_result = %s,
                    last_error = %s,
                    updated_at = %s
                WHERE channel_id = %s
                """,
                (ts, "failure", error_message[:2000], ts, channel_id),
            )


def record_model_probe_failure(channel_id: int, model_name: str, error_message: str) -> None:
    ts = now_ts()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE channel_model_recovery_state
                SET consecutive_success_count = 0,
                    last_probe_at = %s,
                    last_probe_result = %s,
                    last_error = %s,
                    updated_at = %s
                WHERE channel_id = %s AND model_name = %s
                """,
                (ts, "failure", error_message[:2000], ts, channel_id, model_name),
            )


def record_probe_success(channel_id: int, probe_model: str, payload: Dict[str, Any]) -> None:
    ts = now_ts()
    should_refresh_cache = False
    with get_conn(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT consecutive_success_count, source_rule_id
                    FROM channel_recovery_state
                    WHERE channel_id = %s
                    FOR UPDATE
                    """,
                    (channel_id,),
                )
                state = cur.fetchone()
                if not state:
                    conn.commit()
                    return

                next_count = int(state["consecutive_success_count"] or 0) + 1
                if next_count < RECOVERY_SUCCESS_TARGET:
                    cur.execute(
                        """
                        UPDATE channel_recovery_state
                        SET probe_model = %s,
                            consecutive_success_count = %s,
                            last_probe_at = %s,
                            last_probe_result = %s,
                            last_error = '',
                            updated_at = %s
                        WHERE channel_id = %s
                        """,
                        (probe_model, next_count, ts, "success", ts, channel_id),
                    )
                    conn.commit()
                    return

                cur.execute(
                    """
                    SELECT status, other_info
                    FROM channels
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (channel_id,),
                )
                channel = cur.fetchone()
                if not channel:
                    cur.execute("DELETE FROM channel_recovery_state WHERE channel_id = %s", (channel_id,))
                    conn.commit()
                    return

                other_info = parse_json_text(channel.get("other_info"), {})
                restore_reason = "Recovered automatically after 3 consecutive successful probes"
                new_other_info = build_restored_other_info(other_info, ts, restore_reason)
                before_status = int(channel.get("status") or 0)
                cur.execute(
                    "UPDATE channels SET status = %s, other_info = %s WHERE id = %s",
                    (ENABLED_STATUS, json_text(new_other_info), channel_id),
                )
                should_refresh_cache = True
                cur.execute(
                    """
                    INSERT INTO channel_action_audit (
                      channel_id, action_type, reason, source_rule_id, source_log_ids,
                      before_status, after_status, created_at, metadata_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        channel_id,
                        ACTION_RESTORE_CHANNEL,
                        restore_reason,
                        state.get("source_rule_id"),
                        json_text([]),
                        before_status,
                        ENABLED_STATUS,
                        ts,
                        json_text({"probe_model": probe_model, "probe_payload": payload}),
                    ),
                )
                cur.execute("DELETE FROM channel_recovery_state WHERE channel_id = %s", (channel_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if should_refresh_cache:
        refresh_new_api_channel_cache(channel_id)


def record_model_probe_success(channel_id: int, model_name: str, payload: Dict[str, Any]) -> None:
    ts = now_ts()
    should_refresh_cache = False
    with get_conn(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT consecutive_success_count, source_rule_id, snapshot_json
                    FROM channel_model_recovery_state
                    WHERE channel_id = %s AND model_name = %s
                    FOR UPDATE
                    """,
                    (channel_id, model_name),
                )
                state = cur.fetchone()
                if not state:
                    conn.commit()
                    return

                next_count = int(state["consecutive_success_count"] or 0) + 1
                if next_count < RECOVERY_SUCCESS_TARGET:
                    cur.execute(
                        """
                        UPDATE channel_model_recovery_state
                        SET consecutive_success_count = %s,
                            last_probe_at = %s,
                            last_probe_result = %s,
                            last_error = '',
                            updated_at = %s
                        WHERE channel_id = %s AND model_name = %s
                        """,
                        (next_count, ts, "success", ts, channel_id, model_name),
                    )
                    conn.commit()
                    return

                cur.execute(
                    """
                    SELECT status, models, test_model, other_info
                    FROM channels
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (channel_id,),
                )
                channel = cur.fetchone()
                if not channel:
                    cur.execute(
                        "DELETE FROM channel_model_recovery_state WHERE channel_id = %s AND model_name = %s",
                        (channel_id, model_name),
                    )
                    conn.commit()
                    return

                snapshot = parse_json_text(state.get("snapshot_json"), {})
                original_models = snapshot.get("original_models", [])
                current_models = parse_csv_items(channel.get("models"))
                restored_models = restore_model_to_list(current_models, original_models, model_name)

                original_test_model = str(snapshot.get("original_test_model") or "").strip()
                current_test_model = str(channel.get("test_model") or "").strip()
                restored_test_model = current_test_model
                if original_test_model == model_name and not current_test_model:
                    restored_test_model = model_name

                cur.execute(
                    "UPDATE channels SET models = %s, test_model = %s WHERE id = %s",
                    (join_csv_items(restored_models), restored_test_model, channel_id),
                )
                should_refresh_cache = True

                for ability_row in snapshot.get("original_abilities", []):
                    normalized = normalize_ability_row(ability_row)
                    cur.execute(
                        """
                        INSERT INTO abilities (`group`, model, channel_id, enabled, priority, weight, tag)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                          enabled = VALUES(enabled),
                          priority = VALUES(priority),
                          weight = VALUES(weight),
                          tag = VALUES(tag)
                        """,
                        (
                            normalized["group"],
                            normalized["model"],
                            normalized["channel_id"],
                            normalized["enabled"],
                            normalized["priority"],
                            normalized["weight"],
                            normalized["tag"],
                        ),
                    )

                cur.execute(
                    """
                    INSERT INTO channel_action_audit (
                      channel_id, action_type, reason, source_rule_id, source_log_ids,
                      before_status, after_status, created_at, metadata_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        channel_id,
                        ACTION_RESTORE_MODEL,
                        f"Model {model_name} recovered automatically after 3 consecutive successful probes",
                        state.get("source_rule_id"),
                        json_text(snapshot.get("source_log_ids", [])),
                        int(channel.get("status") or 0),
                        int(channel.get("status") or 0),
                        ts,
                        json_text({"model_name": model_name, "probe_payload": payload}),
                    ),
                )

                other_info = parse_json_text(channel.get("other_info"), {})
                if (
                    int(channel.get("status") or 0) == AUTO_DISABLED_STATUS
                    and is_service_managed_disabled(other_info)
                    and channel_disabled_by_model_depletion(other_info)
                ):
                    restore_reason = f"Recovered automatically after model {model_name} probe succeeded"
                    new_other_info = build_restored_other_info(other_info, ts, restore_reason)
                    cur.execute(
                        "UPDATE channels SET status = %s, other_info = %s WHERE id = %s",
                        (ENABLED_STATUS, json_text(new_other_info), channel_id),
                    )
                    should_refresh_cache = True
                    cur.execute(
                        """
                        INSERT INTO channel_action_audit (
                          channel_id, action_type, reason, source_rule_id, source_log_ids,
                          before_status, after_status, created_at, metadata_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            channel_id,
                            ACTION_RESTORE_CHANNEL,
                            restore_reason,
                            state.get("source_rule_id"),
                            json_text(snapshot.get("source_log_ids", [])),
                            AUTO_DISABLED_STATUS,
                            ENABLED_STATUS,
                            ts,
                            json_text({"model_name": model_name, "source": ACTION_RESTORE_MODEL, "probe_payload": payload}),
                        ),
                    )

                cur.execute(
                    "DELETE FROM channel_model_recovery_state WHERE channel_id = %s AND model_name = %s",
                    (channel_id, model_name),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if should_refresh_cache:
        refresh_new_api_channel_cache(channel_id)


def perform_channel_probe(channel_id: int, probe_model: str) -> tuple[bool, str, Dict[str, Any]]:
    url = PROBE_URL_TEMPLATE.format(
        channel_id=channel_id,
        model=urllib_parse.quote(probe_model, safe=""),
    )
    if PROBE_STREAM_ENABLED and "stream=" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}stream=true"
    req = urllib_request.Request(url=url, headers=PROBE_HEADERS, method="GET")
    try:
        with urllib_request.urlopen(req, timeout=PROBE_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            payload = interpret_probe_response(resp.getcode(), body)
            if payload["success"]:
                return True, "ok", payload
            return False, payload["detail"], payload
    except urllib_error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        payload = interpret_probe_response(exc.code, body)
        return False, payload["detail"], payload
    except urllib_error.URLError as exc:
        return False, f"probe network error: {exc.reason}", {"success": False, "detail": str(exc.reason)}


def manual_restore_channel(channel_id: int) -> Dict[str, Any]:
    ts = now_ts()
    should_refresh_cache = False
    with get_conn(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, other_info, name
                    FROM channels
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (channel_id,),
                )
                channel = cur.fetchone()
                if not channel:
                    raise ValueError(f"channel {channel_id} not found")

                other_info = parse_json_text(channel.get("other_info"), {})
                if not is_service_managed_unstable_disabled(other_info):
                    raise ValueError(f"channel {channel_id} is not in manual recovery state")

                before_status = int(channel.get("status") or 0)
                restore_reason = "Restored manually after unstable channel disable"
                new_other_info = build_restored_other_info(other_info, ts, restore_reason)
                cur.execute(
                    "UPDATE channels SET status = %s, other_info = %s WHERE id = %s",
                    (ENABLED_STATUS, json_text(new_other_info), channel_id),
                )
                should_refresh_cache = True
                cur.execute("DELETE FROM channel_recovery_state WHERE channel_id = %s", (channel_id,))
                cur.execute(
                    """
                    INSERT INTO channel_action_audit (
                      channel_id, action_type, reason, source_rule_id, source_log_ids,
                      before_status, after_status, created_at, metadata_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        channel_id,
                        ACTION_RESTORE_CHANNEL,
                        restore_reason,
                        None,
                        json_text([]),
                        before_status,
                        ENABLED_STATUS,
                        ts,
                        json_text({"source": "manual_restore"}),
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if should_refresh_cache:
        refresh_new_api_channel_cache(channel_id)
    return {"ok": True, "channel_id": channel_id}
