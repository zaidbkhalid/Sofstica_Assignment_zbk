"""
GitHub GraphQL API - Fetch star counts for 100,000 repos.

Usage:
    export GITHUB_TOKEN=your_personal_access_token
    python github_stars.py

Output:
    - Prints a live table to the terminal as repos are fetched
    - Saves all results to github_stars.csv

Rate-limit strategy:
    - GitHub GraphQL: 5,000 points/hour. Each `search` costs 1 point + 1 per node.
      We request 100 nodes per call → ~101 points/call → ~49 calls/hour safely.
      We use the rateLimit field to track remaining points and sleep when low.
    - Secondary rate limit: no more than ~20 requests/second.
      We add a small inter-request delay.
    - Retry: exponential back-off on 429 / 5xx / network errors (up to 5 retries).
"""

import os
import sys
import csv
import time
import random
import textwrap
import requests
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN          = os.environ.get("GITHUB_TOKEN", "")
TOTAL_REPOS    = 100_000
PAGE_SIZE      = 100          # nodes per GraphQL call (max 100)
MIN_POINTS_LEFT = 200         # pause when remaining points drop below this
INTER_REQ_DELAY = 0.5         # seconds between requests (secondary rate limit)
MAX_RETRIES    = 5
BASE_BACKOFF   = 2            # seconds; doubles on each retry
OUTPUT_CSV     = "github_stars.csv"
PRINT_EVERY    = 500          # print a summary row every N repos

GRAPHQL_URL = "https://api.github.com/graphql"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/json",
    "Accept":        "application/vnd.github+json",
}

# GraphQL query — uses `search` with `after` cursor for pagination.
# We search repos ordered by stars desc so we get the most-starred first,
# but you can change the query string to anything valid.
QUERY_TEMPLATE = """
query($after: String) {{
  rateLimit {{
    cost
    remaining
    resetAt
  }}
  search(
    query: "stars:>0 sort:stars-desc"
    type: REPOSITORY
    first: {page_size}
    after: $after
  ) {{
    pageInfo {{
      hasNextPage
      endCursor
    }}
    nodes {{
      ... on Repository {{
        nameWithOwner
        stargazerCount
        primaryLanguage {{ name }}
        isArchived
        isFork
        createdAt
      }}
    }}
  }}
}}
""".format(page_size=PAGE_SIZE)

# ── Helpers ───────────────────────────────────────────────────────────────────

def gql_request(cursor: str | None, attempt: int = 0) -> dict:
    """POST the GraphQL query; retry with exponential back-off on errors."""
    variables = {"after": cursor}
    try:
        resp = requests.post(
            GRAPHQL_URL,
            json={"query": QUERY_TEMPLATE, "variables": variables},
            headers=HEADERS,
            timeout=30,
        )
    except requests.RequestException as exc:
        if attempt >= MAX_RETRIES:
            raise
        wait = BASE_BACKOFF ** attempt + random.uniform(0, 1)
        print(f"  [network error] {exc}. Retrying in {wait:.1f}s …", file=sys.stderr)
        time.sleep(wait)
        return gql_request(cursor, attempt + 1)

    if resp.status_code == 429 or resp.status_code >= 500:
        if attempt >= MAX_RETRIES:
            resp.raise_for_status()
        retry_after = int(resp.headers.get("Retry-After", BASE_BACKOFF ** attempt))
        wait = max(retry_after, BASE_BACKOFF ** attempt) + random.uniform(0, 1)
        print(f"  [HTTP {resp.status_code}] rate-limited. Sleeping {wait:.0f}s …", file=sys.stderr)
        time.sleep(wait)
        return gql_request(cursor, attempt + 1)

    resp.raise_for_status()
    payload = resp.json()

    if "errors" in payload:
        # GraphQL-level errors (e.g. bad token, timeout)
        err_msgs = "; ".join(e.get("message", str(e)) for e in payload["errors"])
        if attempt >= MAX_RETRIES:
            raise RuntimeError(f"GraphQL errors: {err_msgs}")
        wait = BASE_BACKOFF ** attempt + random.uniform(0, 1)
        print(f"  [GraphQL error] {err_msgs}. Retrying in {wait:.1f}s …", file=sys.stderr)
        time.sleep(wait)
        return gql_request(cursor, attempt + 1)

    return payload["data"]


def handle_rate_limit(rate_limit: dict) -> None:
    """Sleep if we're running low on points."""
    remaining = rate_limit["remaining"]
    reset_at   = rate_limit["resetAt"]          # ISO-8601 UTC string
    if remaining < MIN_POINTS_LEFT:
        reset_dt  = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
        now_dt    = datetime.now(timezone.utc)
        sleep_sec = max(0, (reset_dt - now_dt).total_seconds()) + 5   # +5s buffer
        print(
            f"\n  [rate limit] Only {remaining} points left. "
            f"Sleeping {sleep_sec:.0f}s until reset at {reset_at} …",
            file=sys.stderr,
        )
        time.sleep(sleep_sec)


def print_header() -> None:
    print(
        f"\n{'#':>7}  {'Repository':<45}  {'Stars':>9}  "
        f"{'Language':<16}  {'Archived':<8}  {'Fork':<5}  {'Created'}"
    )
    print("-" * 115)


def print_row(rank: int, repo: dict) -> None:
    lang      = (repo.get("primaryLanguage") or {}).get("name", "—")
    archived  = "yes" if repo["isArchived"]  else "no"
    fork      = "yes" if repo["isFork"]      else "no"
    created   = repo["createdAt"][:10]
    name      = textwrap.shorten(repo["nameWithOwner"], width=45, placeholder="…")
    print(
        f"{rank:>7}  {name:<45}  {repo['stargazerCount']:>9,}  "
        f"{lang:<16}  {archived:<8}  {fork:<5}  {created}"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not TOKEN:
        sys.exit(
            "ERROR: Set the GITHUB_TOKEN environment variable to a "
            "GitHub personal access token with 'public_repo' scope."
        )

    print(f"Fetching star counts for up to {TOTAL_REPOS:,} GitHub repos …")
    print(f"Results will also be saved to: {OUTPUT_CSV}\n")
    print_header()

    collected = 0
    cursor    = None
    rows      = []           # accumulate for CSV

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csv_fh:
        writer = csv.writer(csv_fh)
        writer.writerow(
            ["rank", "nameWithOwner", "stars", "language",
             "isArchived", "isFork", "createdAt"]
        )

        while collected < TOTAL_REPOS:
            data       = gql_request(cursor)
            rate_limit = data["rateLimit"]
            search     = data["search"]
            nodes      = search["nodes"]
            page_info  = search["pageInfo"]

            for repo in nodes:
                if collected >= TOTAL_REPOS:
                    break
                collected += 1
                lang = (repo.get("primaryLanguage") or {}).get("name", "")
                writer.writerow([
                    collected,
                    repo["nameWithOwner"],
                    repo["stargazerCount"],
                    lang,
                    repo["isArchived"],
                    repo["isFork"],
                    repo["createdAt"],
                ])

                # Print every Nth repo to the terminal (plus always the first)
                if collected == 1 or collected % PRINT_EVERY == 0:
                    print_row(collected, repo)

            # Rate-limit bookkeeping
            handle_rate_limit(rate_limit)

            if not page_info["hasNextPage"]:
                print(f"\n[info] No more pages after {collected:,} repos.")
                break

            cursor = page_info["endCursor"]
            time.sleep(INTER_REQ_DELAY)

    print("-" * 115)
    print(f"\nDone. {collected:,} repos written to {OUTPUT_CSV}.")


if __name__ == "__main__":
    main()
