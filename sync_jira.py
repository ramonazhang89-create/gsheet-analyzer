#!/usr/bin/env python3
"""Fetch Jira issues and save to local cache for Streamlit Cloud deployment.

Usage:
    python sync_jira.py              # fetch & save cache
    python sync_jira.py --push       # fetch, save, commit & push to GitHub

The cache file (jira_cache.json) should be committed to git so that
Streamlit Cloud can read it when jira.shopee.io is unreachable.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jira_cache.json")

JIRA_PROJECTS = ["SPCB", "SPCSP"]
JIRA_BASE_URL = "https://jira.shopee.io"
JIRA_YEAR = str(datetime.now().year)

JIRA_FIELD_IDS = {
    "product_manager": "customfield_10306",
    "estimated_prd_signoff": "customfield_36701",
    "prd_link": "customfield_15707",
    "product_line": "customfield_35604",
    "project_type": "customfield_12411",
    "prd_review_end": "customfield_11546",
    "key_project": "customfield_29500",
}

JIRA_SEARCH_FIELDS = [
    "summary", "status", "priority", "assignee", "components",
    JIRA_FIELD_IDS["product_manager"],
    JIRA_FIELD_IDS["estimated_prd_signoff"],
    JIRA_FIELD_IDS["prd_link"],
    JIRA_FIELD_IDS["product_line"],
    JIRA_FIELD_IDS["project_type"],
    JIRA_FIELD_IDS["prd_review_end"],
    JIRA_FIELD_IDS["key_project"],
]


def get_jira_token() -> str:
    result = subprocess.run(
        ["skynet-base", "key", "get", "JIRA_TOKEN"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        print("ERROR: JIRA_TOKEN not configured. Run: skynet-base setup token")
        sys.exit(1)
    m = re.search(r"JIRA_TOKEN:\s*(.+)", result.stdout)
    if not m:
        print("ERROR: Cannot parse JIRA_TOKEN")
        sys.exit(1)
    return m.group(1).strip()


def build_jql(projects: list[str], year: str = JIRA_YEAR) -> str:
    proj_clause = ", ".join(projects)
    return (
        f"project in ({proj_clause}) AND issuetype = Epic"
        f' AND "Project Type" = "Feature Project"'
        f' AND "POP Request Link" is not EMPTY'
        f" AND status not in (Closed, Icebox)"
        f" AND created >= {int(year) - 1}-01-01"
        f" AND ("
        f"priority in (Highest, High)"
        f' OR (priority in (Medium, Low) AND "Key Project" is not EMPTY)'
        f")"
    )


def fetch_all_issues(token: str, jql: str) -> list[dict]:
    import urllib.request
    import urllib.parse
    import urllib.error

    all_issues: list[dict] = []
    start_at = 0
    max_results = 1500

    while True:
        params = urllib.parse.urlencode({
            "jql": jql,
            "fields": ",".join(JIRA_SEARCH_FIELDS),
            "maxResults": min(100, max_results - len(all_issues)),
            "startAt": start_at,
        })
        url = f"{JIRA_BASE_URL}/rest/api/2/search?{params}"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })

        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        issues = data.get("issues", [])
        all_issues.extend(issues)
        total = data.get("total", 0)
        print(f"  fetched {len(all_issues)}/{total} issues...")

        if len(all_issues) >= total or len(all_issues) >= max_results or not issues:
            break
        start_at += len(issues)

    return all_issues


def main():
    parser = argparse.ArgumentParser(description="Sync Jira data to local cache")
    parser.add_argument("--push", action="store_true", help="git add, commit & push after sync")
    args = parser.parse_args()

    print(f"Fetching Jira issues for projects: {JIRA_PROJECTS}")
    token = get_jira_token()
    jql = build_jql(JIRA_PROJECTS)
    print(f"JQL: {jql}")

    issues = fetch_all_issues(token, jql)
    print(f"Total issues fetched: {len(issues)}")

    cache = {
        "synced_at": datetime.now().isoformat(),
        "jql": jql,
        "total": len(issues),
        "issues": issues,
    }

    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    print(f"Cache saved to {CACHE_FILE}")

    if args.push:
        repo_dir = os.path.dirname(os.path.abspath(__file__))
        subprocess.run(["git", "add", "jira_cache.json"], cwd=repo_dir, check=True)
        msg = f"chore: sync Jira cache ({len(issues)} issues, {datetime.now().strftime('%Y-%m-%d %H:%M')})"
        subprocess.run(["git", "commit", "-m", msg], cwd=repo_dir, check=True)
        subprocess.run(["git", "push"], cwd=repo_dir, check=True)
        print("Pushed to GitHub.")


if __name__ == "__main__":
    main()
