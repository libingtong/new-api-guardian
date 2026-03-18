import json
from typing import Any, Dict, List, Optional


def parse_json_text(raw: Any, default: Any) -> Any:
    if raw is None:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    text = str(raw).strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def normalize_str_list(values: List[Any]) -> List[str]:
    items: List[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in items:
            items.append(text)
    return items


def normalize_int_list(values: List[Any]) -> List[int]:
    items: List[int] = []
    for value in values:
        try:
            num = int(value)
        except (TypeError, ValueError):
            continue
        if num not in items:
            items.append(num)
    return items


def parse_csv_items(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        values = raw
    else:
        values = str(raw).split(",")
    items: List[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in items:
            items.append(text)
    return items


def join_csv_items(items: List[str]) -> str:
    return ",".join(normalize_str_list(items))


def remove_model_from_list(items: List[str], model_name: str) -> List[str]:
    target = str(model_name or "").strip()
    if not target:
        return normalize_str_list(items)
    return [item for item in normalize_str_list(items) if item != target]


def restore_model_to_list(current_items: List[str], original_items: List[str], model_name: str) -> List[str]:
    target = str(model_name or "").strip()
    current = normalize_str_list(current_items)
    if not target:
        return current
    if target in current:
        return current

    original = normalize_str_list(original_items)
    if target not in original:
        return current + [target]

    target_index = original.index(target)
    insert_at = len(current)
    for idx in range(target_index + 1, len(original)):
        follower = original[idx]
        if follower in current:
            insert_at = current.index(follower)
            break
    return current[:insert_at] + [target] + current[insert_at:]


def event_matches_rule(event: Dict[str, Any], rule: Dict[str, Any]) -> bool:
    if rule["match_channel_ids"] and event["channel_id"] not in normalize_int_list(rule["match_channel_ids"]):
        return False
    if rule["match_groups"] and event["group"] not in normalize_str_list(rule["match_groups"]):
        return False
    if rule["match_models"] and event["model_name"] not in normalize_str_list(rule["match_models"]):
        return False
    if rule["match_error_codes"] and event["error_code"] not in normalize_str_list(rule["match_error_codes"]):
        return False
    if rule["match_status_codes"]:
        status_codes = {str(v) for v in normalize_int_list(rule["match_status_codes"])}
        if str(event["status_code"]) not in status_codes:
            return False
    if rule["match_request_paths"] and event["request_path"] not in normalize_str_list(rule["match_request_paths"]):
        return False
    if rule["match_error_text"]:
        haystack = " ".join(
            [
                event["content"],
                event["error_code"],
                event["request_path"],
                event["channel_name"],
                json_text(event["other"]),
            ]
        ).lower()
        keywords = [keyword.lower() for keyword in normalize_str_list(rule["match_error_text"])]
        if not any(keyword in haystack for keyword in keywords):
            return False
    return True


def interpret_probe_response(status_code: int, body: str) -> Dict[str, Any]:
    detail = f"HTTP {status_code}"
    parsed = parse_json_text(body, None)
    if isinstance(parsed, dict):
        success_flag = infer_success_from_payload(parsed)
        if success_flag is False:
            detail = extract_probe_error_detail(parsed) or detail
            return {"success": False, "detail": detail, "body": parsed}
        if status_code >= 400:
            detail = extract_probe_error_detail(parsed) or detail
            return {"success": False, "detail": detail, "body": parsed}
        return {"success": True, "detail": "ok", "body": parsed}
    if status_code >= 400:
        return {"success": False, "detail": body[:300] or detail, "body": body}
    return {"success": True, "detail": "ok", "body": body[:300]}


def infer_success_from_payload(payload: Dict[str, Any]) -> Optional[bool]:
    for key in ("success", "ok", "passed"):
        if key in payload:
            return _normalize_bool(payload[key])
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("success", "ok", "passed"):
            if key in data:
                return _normalize_bool(data[key])
    return None


def extract_probe_error_detail(payload: Dict[str, Any]) -> str:
    for key in ("message", "error", "detail"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("message", "error", "detail"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    return text in {"1", "true", "ok", "success", "passed"}
