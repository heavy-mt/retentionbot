"""Small SQL connection facade; production PostgreSQL and local SQLite use the same queries."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row


class Row(dict):
    def __getitem__(self, key):
        return list(self.values())[key] if isinstance(key, int) else super().__getitem__(key)


class Cursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def fetchone(self):
        row = self.cursor.fetchone()
        return None if row is None else Row(row)

    def fetchall(self):
        return [Row(row) for row in self.cursor.fetchall()]

    def __iter__(self):
        return iter(self.fetchall())


class Database:
    def __init__(self, target: str | Path, namespace: str):
        self.postgres = str(target).startswith(("postgresql://", "postgres://"))
        self.contexts = []
        if self.postgres:
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", namespace):
                raise ValueError("Invalid database namespace")
            self.connection = psycopg.connect(
                str(target), autocommit=True, row_factory=dict_row, connect_timeout=10
            )
            self.connection.execute("SELECT pg_advisory_lock(hashtextextended(%s,0))", (namespace,))
            self.connection.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(namespace))
            )
            self.connection.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(namespace))
            )
            self.connection.execute("SET statement_timeout TO '30s'")
            self.connection.execute("SET lock_timeout TO '5s'")
        else:
            path = Path(target)
            path.parent.mkdir(parents=True, exist_ok=True)
            self.connection = sqlite3.connect(path, timeout=5)
            self.connection.row_factory = sqlite3.Row
            self.connection.create_function("GREATEST", -1, max)
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=FULL")
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute("PRAGMA busy_timeout=5000")

    def initialized(self, namespace: str):
        self.connection.commit()
        if self.postgres:
            self.connection.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s,0))", (namespace,)
            )

    def columns(self, table: str):
        if self.postgres:
            return {
                r["column_name"]
                for r in self.connection.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=current_schema() AND table_name=%s",
                    (table,),
                )
            }
        return {r[1] for r in self.connection.execute(f"PRAGMA table_info({table})")}

    def execute(self, query: str, params=()):
        # Queries are fixed application SQL; values remain bound parameters on either backend.
        if self.postgres:
            query = query.replace("?", "%s")
            params = tuple(int(v) if isinstance(v, bool) else v for v in params)
        return Cursor(self.connection.execute(query, params))

    def executescript(self, script: str):
        for query in script.split(";"):
            if query.strip():
                self.execute(query)

    def __enter__(self):
        context = self.connection.transaction() if self.postgres else self.connection
        self.contexts.append(context)
        context.__enter__()
        return self

    def __exit__(self, *args):
        return self.contexts.pop().__exit__(*args)

    def close(self):
        self.connection.close()
