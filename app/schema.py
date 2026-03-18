import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from app.db import get_conn, get_server_conn
from app.sql_utils import split_sql_statements


DUMP_FILE = Path(__file__).resolve().parent.parent / "oneapi_2026-03-02_11-27-44_mysql_data_tNhek.sql"
REQUIRED_COLUMNS: Dict[str, Dict[str, str]] = {
    "logs": {
        "content": "LONGTEXT",
        "other": "LONGTEXT",
        "channel_name": "LONGTEXT",
        "is_stream": "TINYINT(1) DEFAULT NULL",
    },
    "channels": {
        "test_model": "LONGTEXT",
        "status": "BIGINT(20) DEFAULT '1'",
        "name": "VARCHAR(191) DEFAULT NULL",
        "base_url": "VARCHAR(191) DEFAULT ''",
        "models": "LONGTEXT",
        "`group`": "VARCHAR(64) DEFAULT 'default'",
        "auto_ban": "BIGINT(20) DEFAULT '1'",
        "other_info": "LONGTEXT",
    },
    "abilities": {
        "`group`": "VARCHAR(64) NOT NULL",
        "model": "VARCHAR(64) NOT NULL",
        "channel_id": "BIGINT(20) NOT NULL",
        "enabled": "TINYINT(1) DEFAULT NULL",
    },
}

INTERNAL_TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS rule_definitions (
      id BIGINT(20) NOT NULL AUTO_INCREMENT,
      name VARCHAR(191) NOT NULL,
      enabled TINYINT(1) NOT NULL DEFAULT 1,
      priority BIGINT(20) NOT NULL DEFAULT 0,
      match_channel_ids LONGTEXT,
      match_groups LONGTEXT,
      match_models LONGTEXT,
      match_error_text LONGTEXT,
      match_error_codes LONGTEXT,
      match_status_codes LONGTEXT,
      match_request_paths LONGTEXT,
      window_seconds INT NOT NULL DEFAULT 300,
      threshold_count INT NOT NULL DEFAULT 3,
      action_type VARCHAR(32) NOT NULL DEFAULT 'disable_channel',
      created_at BIGINT(20) NOT NULL,
      updated_at BIGINT(20) NOT NULL,
      PRIMARY KEY (id),
      KEY idx_rule_definitions_enabled_priority (enabled, priority)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS rule_hits (
      id BIGINT(20) NOT NULL AUTO_INCREMENT,
      rule_id BIGINT(20) NOT NULL,
      log_id BIGINT(20) NOT NULL,
      channel_id BIGINT(20) NOT NULL,
      matched_at BIGINT(20) NOT NULL,
      hit_key VARCHAR(255) NOT NULL,
      snapshot_json LONGTEXT,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_rule_log (rule_id, log_id),
      KEY idx_rule_hits_rule_channel_time (rule_id, channel_id, matched_at),
      KEY idx_rule_hits_log_id (log_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS channel_action_audit (
      id BIGINT(20) NOT NULL AUTO_INCREMENT,
      channel_id BIGINT(20) NOT NULL,
      action_type VARCHAR(32) NOT NULL,
      reason LONGTEXT,
      source_rule_id BIGINT(20) DEFAULT NULL,
      source_log_ids LONGTEXT,
      before_status BIGINT(20) DEFAULT NULL,
      after_status BIGINT(20) DEFAULT NULL,
      created_at BIGINT(20) NOT NULL,
      metadata_json LONGTEXT,
      PRIMARY KEY (id),
      KEY idx_channel_action_audit_channel_created (channel_id, created_at),
      KEY idx_channel_action_audit_action_created (action_type, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS worker_checkpoints (
      worker_name VARCHAR(64) NOT NULL,
      last_log_id BIGINT(20) NOT NULL DEFAULT 0,
      updated_at BIGINT(20) NOT NULL,
      PRIMARY KEY (worker_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS channel_recovery_state (
      channel_id BIGINT(20) NOT NULL,
      probe_model VARCHAR(191) DEFAULT NULL,
      source_rule_id BIGINT(20) DEFAULT NULL,
      consecutive_success_count INT NOT NULL DEFAULT 0,
      last_probe_at BIGINT(20) DEFAULT NULL,
      last_probe_result VARCHAR(16) DEFAULT NULL,
      last_error LONGTEXT,
      disabled_at BIGINT(20) NOT NULL,
      updated_at BIGINT(20) NOT NULL,
      PRIMARY KEY (channel_id),
      KEY idx_channel_recovery_state_updated_at (updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS channel_model_recovery_state (
      channel_id BIGINT(20) NOT NULL,
      model_name VARCHAR(191) NOT NULL,
      source_rule_id BIGINT(20) DEFAULT NULL,
      consecutive_success_count INT NOT NULL DEFAULT 0,
      last_probe_at BIGINT(20) DEFAULT NULL,
      last_probe_result VARCHAR(16) DEFAULT NULL,
      last_error LONGTEXT,
      disabled_at BIGINT(20) NOT NULL,
      updated_at BIGINT(20) NOT NULL,
      snapshot_json LONGTEXT,
      PRIMARY KEY (channel_id, model_name),
      KEY idx_channel_model_recovery_state_updated_at (updated_at),
      KEY idx_channel_model_recovery_state_channel_id (channel_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


def bootstrap_database(import_missing_business_tables: bool = True) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "database_created": False,
        "imported_tables": [],
        "missing_tables": [],
        "added_columns": [],
        "internal_tables_checked": [],
        "default_rule_seeded": False,
    }
    ensure_database_exists()
    report["database_created"] = True

    missing_tables = get_missing_business_tables()
    if missing_tables:
        report["missing_tables"] = missing_tables
        if import_missing_business_tables:
            import_tables_from_dump(missing_tables)
            report["imported_tables"] = missing_tables
        else:
            raise RuntimeError(
                "missing required business tables: "
                + ", ".join(missing_tables)
                + ". this service is running in attached mode and will not import schema dumps."
            )

    report["added_columns"] = ensure_required_columns()
    report["internal_tables_checked"] = ensure_internal_tables()
    return report


def ensure_database_exists() -> None:
    db_name = os.getenv("DB_NAME", "oneapi")
    with get_server_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )


def get_missing_business_tables() -> List[str]:
    required_tables = list(REQUIRED_COLUMNS.keys())
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name AS table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name IN (%s, %s, %s)
                """,
                (os.getenv("DB_NAME", "oneapi"), *required_tables),
            )
            existing = {row["table_name"] for row in cur.fetchall()}
    return [table for table in required_tables if table not in existing]


def ensure_required_columns() -> List[Dict[str, str]]:
    added_columns: List[Dict[str, str]] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name AS table_name, column_name AS column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name IN (%s, %s, %s)
                """,
                (os.getenv("DB_NAME", "oneapi"), "logs", "channels", "abilities"),
            )
            existing: Dict[str, set[str]] = {}
            for row in cur.fetchall():
                existing.setdefault(row["table_name"], set()).add(row["column_name"])

            for table_name, columns in REQUIRED_COLUMNS.items():
                table_columns = existing.get(table_name, set())
                for column_name, definition in columns.items():
                    normalized_name = column_name.replace("`", "")
                    if normalized_name in table_columns:
                        continue
                    cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN {column_name} {definition}")
                    added_columns.append({"table": table_name, "column": normalized_name})
    return added_columns


def ensure_internal_tables() -> List[str]:
    ensured_tables: List[str] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            for statement in INTERNAL_TABLE_STATEMENTS:
                cur.execute(statement)
                match = re.search(r"CREATE TABLE IF NOT EXISTS\s+([a-zA-Z0-9_]+)", statement)
                if match:
                    ensured_tables.append(match.group(1))
    return ensured_tables

def import_tables_from_dump(table_names: Iterable[str]) -> None:
    if not DUMP_FILE.exists():
        raise FileNotFoundError(f"missing dump file: {DUMP_FILE}")

    script = DUMP_FILE.read_text(encoding="utf-8")
    ordered_tables = [table for table in ("channels", "abilities", "logs") if table in set(table_names)]
    statements: List[str] = []
    for table_name in ordered_tables:
        statements.extend(extract_table_statements(script, table_name))

    with get_conn(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def extract_table_statements(script: str, table_name: str) -> List[str]:
    pattern = re.compile(
        rf"DROP TABLE IF EXISTS `{table_name}`;.*?UNLOCK TABLES;",
        re.DOTALL,
    )
    match = pattern.search(script)
    if not match:
        raise ValueError(f"unable to locate dump section for table {table_name}")
    return split_sql_statements(match.group(0))
