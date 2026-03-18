import os
from contextlib import contextmanager
from typing import Iterator

import pymysql
from pymysql.cursors import DictCursor
from pymysql.constants import CLIENT


def get_db_config(database: str | None = None, autocommit: bool = True) -> dict:
    config = {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "charset": "utf8mb4",
        "cursorclass": DictCursor,
        "autocommit": autocommit,
        "client_flag": CLIENT.MULTI_STATEMENTS,
    }
    db_name = database if database is not None else os.getenv("DB_NAME", "oneapi")
    if db_name:
        config["database"] = db_name
    return config


@contextmanager
def get_conn(
    database: str | None = None,
    autocommit: bool = True,
) -> Iterator[pymysql.connections.Connection]:
    conn = pymysql.connect(**get_db_config(database=database, autocommit=autocommit))
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_server_conn() -> Iterator[pymysql.connections.Connection]:
    conn = pymysql.connect(**get_db_config(database="", autocommit=True))
    try:
        yield conn
    finally:
        conn.close()
