from typing import Any, Dict, List, Tuple


FilterParams = Dict[str, Any]


def build_logs_where(params: FilterParams) -> Tuple[str, List[Any]]:
    clauses = ["1=1"]
    values: List[Any] = []

    start_ts = params.get("start_ts")
    end_ts = params.get("end_ts")
    token_id = params.get("token_id")
    user_id = params.get("user_id")
    model_name = params.get("model_name")
    group = params.get("group")

    if start_ts is not None:
        clauses.append("created_at >= %s")
        values.append(start_ts)
    if end_ts is not None:
        clauses.append("created_at <= %s")
        values.append(end_ts)
    if token_id is not None:
        clauses.append("token_id = %s")
        values.append(token_id)
    if user_id is not None:
        clauses.append("user_id = %s")
        values.append(user_id)
    if model_name:
        clauses.append("model_name = %s")
        values.append(model_name)
    if group:
        clauses.append("`group` = %s")
        values.append(group)

    return " AND ".join(clauses), values


def overview_sql(params: FilterParams) -> Tuple[str, List[Any]]:
    where_sql, values = build_logs_where(params)
    sql = f"""
    SELECT
      COUNT(*) AS requests,
      COALESCE(SUM(quota), 0) AS total_quota,
      COALESCE(SUM(prompt_tokens), 0) AS total_prompt_tokens,
      COALESCE(SUM(completion_tokens), 0) AS total_completion_tokens,
      COALESCE(SUM(prompt_tokens + completion_tokens), 0) AS total_tokens,
      COALESCE(SUM(CASE WHEN type = 2 THEN 1 ELSE 0 END), 0) AS success_requests,
      COALESCE(SUM(CASE WHEN type = 5 THEN 1 ELSE 0 END), 0) AS error_requests,
      COALESCE(AVG(use_time), 0) AS avg_use_time_ms,
      COUNT(DISTINCT NULLIF(ip, '')) AS unique_ip_count,
      COALESCE(
        SUBSTRING_INDEX(
          GROUP_CONCAT(DISTINCT NULLIF(ip, '') ORDER BY NULLIF(ip, '') SEPARATOR ', '),
          ', ',
          5
        ),
        ''
      ) AS ip_list
    FROM logs
    WHERE {where_sql}
    """
    return sql, values


def token_model_usage_sql(params: FilterParams, limit: int) -> Tuple[str, List[Any]]:
    where_sql, values = build_logs_where(params)
    sql = f"""
    SELECT
      model_name,
      COUNT(*) AS requests,
      COALESCE(SUM(quota), 0) AS total_quota,
      COALESCE(SUM(prompt_tokens + completion_tokens), 0) AS total_tokens,
      COALESCE(SUM(CASE WHEN type = 5 THEN 1 ELSE 0 END), 0) AS error_requests,
      COUNT(DISTINCT NULLIF(ip, '')) AS unique_ip_count
    FROM logs
    WHERE {where_sql}
    GROUP BY model_name
    ORDER BY total_quota DESC
    LIMIT %s
    """
    values.append(limit)
    return sql, values


def rankings_sql(params: FilterParams, dimension: str, metric: str, limit: int) -> Tuple[str, List[Any]]:
    where_sql, values = build_logs_where(params)

    metric_map = {
        "quota": "total_quota",
        "requests": "requests",
        "tokens": "total_tokens",
    }
    order_metric = metric_map.get(metric, "total_quota")

    if dimension == "user":
        dim_cols = "user_id, username AS name"
        group_cols = "user_id, username"
    elif dimension == "model":
        dim_cols = "model_name AS name"
        group_cols = "model_name"
    else:
        dim_cols = "token_id, token_name AS name"
        group_cols = "token_id, token_name"

    sql = f"""
    SELECT
      {dim_cols},
      COUNT(*) AS requests,
      COALESCE(SUM(quota), 0) AS total_quota,
      COALESCE(SUM(prompt_tokens + completion_tokens), 0) AS total_tokens,
      COALESCE(SUM(CASE WHEN type = 5 THEN 1 ELSE 0 END), 0) AS error_requests,
      COUNT(DISTINCT NULLIF(ip, '')) AS unique_ip_count,
      COALESCE(
        SUBSTRING_INDEX(
          GROUP_CONCAT(DISTINCT NULLIF(ip, '') ORDER BY NULLIF(ip, '') SEPARATOR ', '),
          ', ',
          5
        ),
        ''
      ) AS ip_list
    FROM logs
    WHERE {where_sql}
    GROUP BY {group_cols}
    ORDER BY {order_metric} DESC
    LIMIT %s
    """
    values.append(limit)
    return sql, values


def recent_logs_sql(params: FilterParams, limit: int) -> Tuple[str, List[Any]]:
    where_sql, values = build_logs_where(params)
    sql = f"""
    SELECT
      id, created_at, type, username, token_name, token_id, model_name,
      quota, prompt_tokens, completion_tokens, use_time, channel_id, `group`, ip, request_id
    FROM logs
    WHERE {where_sql}
    ORDER BY id DESC
    LIMIT %s
    """
    values.append(limit)
    return sql, values


def leaderboard_token_summary_sql(params: FilterParams, limit: int) -> Tuple[str, List[Any]]:
    where_sql, values = build_logs_where(params)
    sql = f"""
    SELECT
      token_id,
      COALESCE(NULLIF(MAX(token_name), ''), CONCAT('token-', token_id)) AS token_name,
      COUNT(*) AS requests,
      COALESCE(SUM(prompt_tokens), 0) AS total_prompt_tokens,
      COALESCE(SUM(completion_tokens), 0) AS total_completion_tokens,
      COALESCE(SUM(prompt_tokens + completion_tokens), 0) AS total_tokens,
      COALESCE(SUM(quota), 0) AS total_quota,
      COUNT(DISTINCT model_name) AS model_count,
      COUNT(DISTINCT DATE(FROM_UNIXTIME(created_at))) AS active_days,
      COUNT(DISTINCT NULLIF(ip, '')) AS unique_ip_count,
      COALESCE(
        SUBSTRING_INDEX(
          GROUP_CONCAT(DISTINCT NULLIF(ip, '') ORDER BY NULLIF(ip, '') SEPARATOR ', '),
          ', ',
          5
        ),
        ''
      ) AS ip_list
    FROM logs
    WHERE {where_sql} AND token_id IS NOT NULL
    GROUP BY token_id
    ORDER BY total_tokens DESC
    LIMIT %s
    """
    values.append(limit)
    return sql, values


def leaderboard_token_model_sql(params: FilterParams) -> Tuple[str, List[Any]]:
    where_sql, values = build_logs_where(params)
    sql = f"""
    SELECT
      token_id,
      model_name,
      COUNT(*) AS requests,
      COALESCE(SUM(prompt_tokens + completion_tokens), 0) AS total_tokens
    FROM logs
    WHERE {where_sql} AND token_id IS NOT NULL
    GROUP BY token_id, model_name
    ORDER BY token_id ASC, total_tokens DESC
    """
    return sql, values


def ip_usage_summary_sql(params: FilterParams, dimension: str, value: Any) -> Tuple[str, List[Any]]:
    where_sql, values = build_logs_where(params)

    if dimension == "user":
        where_sql = f"{where_sql} AND user_id = %s"
    elif dimension == "model":
        where_sql = f"{where_sql} AND model_name = %s"
    else:
        where_sql = f"{where_sql} AND token_id = %s"
    values.append(value)

    sql = f"""
    SELECT
      COUNT(*) AS requests,
      COALESCE(SUM(quota), 0) AS total_quota,
      COALESCE(SUM(prompt_tokens), 0) AS total_prompt_tokens,
      COALESCE(SUM(completion_tokens), 0) AS total_completion_tokens,
      COALESCE(SUM(prompt_tokens + completion_tokens), 0) AS total_tokens,
      COALESCE(SUM(CASE WHEN type = 5 THEN 1 ELSE 0 END), 0) AS error_requests,
      COUNT(DISTINCT NULLIF(ip, '')) AS unique_ip_count
    FROM logs
    WHERE {where_sql} AND NULLIF(ip, '') IS NOT NULL
    """
    return sql, values


def ip_usage_detail_sql(
    params: FilterParams,
    dimension: str,
    value: Any,
    limit: int,
) -> Tuple[str, List[Any]]:
    where_sql, values = build_logs_where(params)

    if dimension == "user":
        where_sql = f"{where_sql} AND user_id = %s"
    elif dimension == "model":
        where_sql = f"{where_sql} AND model_name = %s"
    else:
        where_sql = f"{where_sql} AND token_id = %s"
    values.append(value)

    sql = f"""
    SELECT
      ip,
      COUNT(*) AS requests,
      COALESCE(SUM(quota), 0) AS total_quota,
      COALESCE(SUM(prompt_tokens), 0) AS total_prompt_tokens,
      COALESCE(SUM(completion_tokens), 0) AS total_completion_tokens,
      COALESCE(SUM(prompt_tokens + completion_tokens), 0) AS total_tokens,
      COALESCE(SUM(CASE WHEN type = 5 THEN 1 ELSE 0 END), 0) AS error_requests,
      MIN(created_at) AS first_seen_at,
      MAX(created_at) AS last_seen_at
    FROM logs
    WHERE {where_sql} AND NULLIF(ip, '') IS NOT NULL
    GROUP BY ip
    ORDER BY total_tokens DESC, requests DESC, ip ASC
    LIMIT %s
    """
    values.append(limit)
    return sql, values
