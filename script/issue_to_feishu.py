#!/usr/bin/env python3
"""Sync GitHub issue events to Feishu Bitable."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise SystemExit(f"Missing required env var: {name}")
    return value


def _parse_github_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc).isoformat()


def _load_event(event_path: str) -> dict[str, Any]:
    with open(event_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _request_json(method: str, url: str, headers: dict[str, str], payload: Any) -> dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise SystemExit(f"HTTP {error.code} {error.reason}: {detail}") from error
    return json.loads(body)


def _get_tenant_token(base_url: str, app_id: str, app_secret: str) -> str:
    url = f"{base_url}/open-apis/auth/v3/tenant_access_token/internal"
    response = _request_json(
        "POST",
        url,
        {"Content-Type": "application/json; charset=utf-8"},
        {"app_id": app_id, "app_secret": app_secret},
    )
    if response.get("code") != 0:
        raise SystemExit(f"Failed to get tenant token: {response}")
    return response["tenant_access_token"]


def _bitable_search(
    base_url: str,
    token: str,
    app_token: str,
    table_id: str,
    issue_id: int,
    field_name: str,
) -> str | None:
    url = f"{base_url}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
    payload = {
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": field_name,
                    "operator": "is",
                    "value": [str(issue_id)],
                }
            ],
        },
        "page_size": 1,
    }
    response = _request_json(
        "POST",
        url,
        {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        },
        payload,
    )
    if response.get("code") != 0:
        raise SystemExit(f"Failed to search bitable records: {response}")
    items = response.get("data", {}).get("items", [])
    if not items:
        return None
    return items[0].get("record_id")


def _bitable_upsert(
    base_url: str,
    token: str,
    app_token: str,
    table_id: str,
    issue_id: int,
    field_name: str,
    fields: dict[str, Any],
) -> None:
    record_id = _bitable_search(base_url, token, app_token, table_id, issue_id, field_name)
    if record_id:
        url = f"{base_url}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        method = "PUT"
    else:
        url = f"{base_url}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        method = "POST"
    response = _request_json(
        method,
        url,
        {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        },
        {"fields": fields},
    )
    if response.get("code") != 0:
        raise SystemExit(f"Failed to upsert bitable record: {response}")


def _bitable_create(
    base_url: str,
    token: str,
    app_token: str,
    table_id: str,
    fields: dict[str, Any],
) -> None:
    url = f"{base_url}/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    response = _request_json(
        "POST",
        url,
        {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        },
        {"fields": fields},
    )
    if response.get("code") != 0:
        raise SystemExit(f"Failed to create bitable record: {response}")


def _truncate(value: str | None, limit: int) -> str | None:
    if value is None:
        return None
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def main() -> None:
    event_path = _env("GITHUB_EVENT_PATH")
    base_url = os.getenv("FEISHU_BASE_URL") or "https://open.feishu.cn"
    app_id = _env("FEISHU_APP_ID")
    app_secret = _env("FEISHU_APP_SECRET")
    app_token = _env("FEISHU_APP_TOKEN")
    table_id = _env("FEISHU_TABLE_ID")
    upsert = os.getenv("FEISHU_UPSERT", "1") == "1"
    issue_id_field = os.getenv("FEISHU_FIELD_ISSUE_ID", "Issue ID")

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

    fields = {
        issue_id_field: str(issue.get("id")),
        "Issue Number": issue.get("number"),
        "Title": issue.get("title"),
        "State": issue.get("state"),
        "URL": issue.get("html_url"),
        "User": issue.get("user", {}).get("login"),
        "Labels": ", ".join(labels) if labels else None,
        "Assignees": ", ".join(assignees) if assignees else None,
        "Action": action,
        "Created At": _parse_github_timestamp(issue.get("created_at")),
        "Updated At": _parse_github_timestamp(issue.get("updated_at")),
        "Closed At": _parse_github_timestamp(issue.get("closed_at")),
        "Body": _truncate(issue.get("body"), 5000),
    }

    token = _get_tenant_token(base_url, app_id, app_secret)
    if upsert:
        _bitable_upsert(
            base_url, token, app_token, table_id, issue.get("id"), issue_id_field, fields
        )
    else:
        _bitable_create(base_url, token, app_token, table_id, fields)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

