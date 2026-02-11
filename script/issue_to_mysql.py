#!/usr/bin/env python3
"""Sync GitHub issue events to MySQL."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import mysql.connector


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise SystemExit(f"Missing required env var: {name}")
    return value


def _parse_github_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    # GitHub timestamps are ISO 8601 with Z suffix.
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _load_event(event_path: str) -> dict[str, Any]:
    with open(event_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _connect_mysql() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=_env("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=_env("MYSQL_USER"),
        password=_env("MYSQL_PASSWORD"),
        database=_env("MYSQL_DATABASE"),
    )


def _ensure_schema(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS github_issues (
            id BIGINT PRIMARY KEY,
            number INT,
            title TEXT,
            body MEDIUMTEXT,
            state VARCHAR(20),
            created_at DATETIME,
            updated_at DATETIME,
            closed_at DATETIME NULL,
            url VARCHAR(500),
            user_login VARCHAR(100),
            labels JSON,
            assignees JSON,
            event_action VARCHAR(50),
            event_received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            payload JSON
        )
        """
    )


def main() -> None:
    event_path = _env("GITHUB_EVENT_PATH")
    event = _load_event(event_path)

    if "issue" not in event:
        raise SystemExit("Event payload missing 'issue'")

    issue = event["issue"]
    action = event.get("action", "unknown")

    labels = [label.get("name") for label in issue.get("labels", []) if label.get("name")]
    assignees = [
        assignee.get("login")
        for assignee in issue.get("assignees", [])
        if assignee.get("login")
    ]

    payload_json = json.dumps(event, ensure_ascii=True)
    labels_json = json.dumps(labels, ensure_ascii=True)
    assignees_json = json.dumps(assignees, ensure_ascii=True)

    created_at = _parse_github_timestamp(issue.get("created_at"))
    updated_at = _parse_github_timestamp(issue.get("updated_at"))
    closed_at = _parse_github_timestamp(issue.get("closed_at"))

    connection = _connect_mysql()
    try:
        cursor = connection.cursor()
        _ensure_schema(cursor)

        cursor.execute(
            """
            INSERT INTO github_issues (
                id,
                number,
                title,
                body,
                state,
                created_at,
                updated_at,
                closed_at,
                url,
                user_login,
                labels,
                assignees,
                event_action,
                payload
            ) VALUES (
                %(id)s,
                %(number)s,
                %(title)s,
                %(body)s,
                %(state)s,
                %(created_at)s,
                %(updated_at)s,
                %(closed_at)s,
                %(url)s,
                %(user_login)s,
                %(labels)s,
                %(assignees)s,
                %(event_action)s,
                %(payload)s
            )
            ON DUPLICATE KEY UPDATE
                number = VALUES(number),
                title = VALUES(title),
                body = VALUES(body),
                state = VALUES(state),
                created_at = VALUES(created_at),
                updated_at = VALUES(updated_at),
                closed_at = VALUES(closed_at),
                url = VALUES(url),
                user_login = VALUES(user_login),
                labels = VALUES(labels),
                assignees = VALUES(assignees),
                event_action = VALUES(event_action),
                payload = VALUES(payload),
                event_received_at = CURRENT_TIMESTAMP
            """,
            {
                "id": issue.get("id"),
                "number": issue.get("number"),
                "title": issue.get("title"),
                "body": issue.get("body"),
                "state": issue.get("state"),
                "created_at": created_at,
                "updated_at": updated_at,
                "closed_at": closed_at,
                "url": issue.get("html_url"),
                "user_login": issue.get("user", {}).get("login"),
                "labels": labels_json,
                "assignees": assignees_json,
                "event_action": action,
                "payload": payload_json,
            },
        )
        connection.commit()
    finally:
        connection.close()


if __name__ == "__main__":
    main()

