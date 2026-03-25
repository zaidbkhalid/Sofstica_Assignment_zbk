import os
import sys
import csv
import time
import queue
import random
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ---------------- Config ----------------
TOKEN = os.environ.get("GITHUB_TOKEN", "")
GRAPHQL_URL = "https://api.github.com/graphql"

TOTAL_REPOS = 10_000
PAGE_SIZE = 100
MAX_WORKERS = 4
INTER_REQUEST_DELAY = 0.25
MIN_POINTS_LEFT = 250
MAX_RETRIES = 5
BASE_BACKOFF = 2.0
OUTPUT_CSV = "github_stars.csv"
PRINT_EVERY = 500

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/vnd.github+json",
}

QUERY = """
query($after: String, $q: String!, $first: Int!) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  search(query: $q, type: REPOSITORY, first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    repositoryCount
    nodes {
      ... on Repository {
        nameWithOwner
        stargazerCount
        primaryLanguage { name }
        isArchived
        isFork
        createdAt
      }
    }
  }
}
"""

# Initial shards. If any shard is still too large, we split it automatically.
INITIAL_STAR_SHARDS = [
    "stars:>=500000",
    "stars:100000..499999",
    "stars:50000..99999",
    "stars:20000..49999",
    "stars:10000..19999",
    "stars:5000..9999",
    "stars:2000..4999",
    "stars:1000..1999",
    "stars:500..999",
    "stars:200..499",
    "stars:100..199",
    "stars:50..99",
    "stars:1..49",
]


@dataclass(frozen=True)
class RepoRow:
    namewithowner: str
    stars: int
    language: str
    isarchived: bool
    isfork: bool
    createdat: str


class RateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.remaining = None
        self.reset_at = None

    def update(self, rate_limit: dict) -> None:
        with self._lock:
            self.remaining = rate_limit["remaining"]
            self.reset_at = rate_limit["resetAt"]

    def maybe_wait(self) -> None:
        with self._lock:
            remaining = self.remaining
            reset_at = self.reset_at

        if remaining is None or reset_at is None:
            return

        if remaining < MIN_POINTS_LEFT:
            reset_dt = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
            now_dt = datetime.now(timezone.utc)
            sleep_sec = max(0, (reset_dt - now_dt).total_seconds()) + 5
            print(
                f"\n[rate-limit] Only {remaining} points left. Sleeping {sleep_sec:.0f}s until {reset_at} ...",
                file=sys.stderr,
            )
            time.sleep(sleep_sec)


rate_limiter = RateLimiter()


def gql_request(search_query: str, cursor: str | None = None, attempt: int = 0) -> dict:
    variables = {
        "q": search_query,
        "after": cursor,
        "first": PAGE_SIZE,
    }

    try:
        resp = requests.post(
            GRAPHQL_URL,
            json={"query": QUERY, "variables": variables},
            headers=HEADERS,
            timeout=30,
        )
    except requests.RequestException as exc:
        if attempt >= MAX_RETRIES:
            raise
        wait = (BASE_BACKOFF ** attempt) + random.uniform(0, 1)
        print(f"[network error] {exc}. Retrying in {wait:.1f}s ...", file=sys.stderr)
        time.sleep(wait)
        return gql_request(search_query, cursor, attempt + 1)

    if resp.status_code == 429 or resp.status_code >= 500:
        if attempt >= MAX_RETRIES:
            resp.raise_for_status()
        retry_after = int(resp.headers.get("Retry-After", BASE_BACKOFF ** attempt))
        wait = max(retry_after, BASE_BACKOFF ** attempt) + random.uniform(0, 1)
        print(f"[HTTP {resp.status_code}] Retrying in {wait:.1f}s ...", file=sys.stderr)
        time.sleep(wait)
        return gql_request(search_query, cursor, attempt + 1)

    resp.raise_for_status()
    payload = resp.json()

    if "errors" in payload:
        msg = "; ".join(e.get("message", str(e)) for e in payload["errors"])
        if attempt >= MAX_RETRIES:
            raise RuntimeError(msg)
        wait = (BASE_BACKOFF ** attempt) + random.uniform(0, 1)
        print(f"[GraphQL error] {msg}. Retrying in {wait:.1f}s ...", file=sys.stderr)
        time.sleep(wait)
        return gql_request(search_query, cursor, attempt + 1)

    data = payload["data"]
    rate_limiter.update(data["rateLimit"])
    return data


def parse_star_range(q: str) -> tuple[int, int | None]:
    s = q.replace("stars:", "")
    if s.startswith(">="):
        return int(s[2:]), None
    lo, hi = s.split("..")
    return int(lo), int(hi)


def split_shard(q: str) -> list[str]:
    lo, hi = parse_star_range(q)

    if hi is None:
        mid = lo + 100000
        return [f"stars:{mid}..999999999", f"stars:{lo}..{mid-1}"]

    if lo == hi:
        return [q]

    mid = (lo + hi) // 2
    if mid < lo or mid >= hi:
        return [q]

    return [f"stars:{lo}..{mid}", f"stars:{mid+1}..{hi}"]


def discover_leaf_shards(initial_shards: list[str]) -> list[str]:
    pending = list(initial_shards)
    leaves: list[str] = []

    while pending:
        shard = pending.pop(0)
        data = gql_request(shard, None)
        time.sleep(INTER_REQUEST_DELAY)

        count = data["search"]["repositoryCount"]

        if count > 1000:
            parts = split_shard(shard)
            if parts == [shard]:
                leaves.append(shard)
            else:
                pending.extend(parts)
        else:
            leaves.append(shard)

        rate_limiter.maybe_wait()

    return leaves


def crawl_shard(
    shard_query: str,
    target_queue: "queue.Queue[RepoRow]",
    seen: set[str],
    seen_lock: threading.Lock,
    stop_event: threading.Event,
) -> int:
    cursor = None
    count = 0

    while not stop_event.is_set():
        data = gql_request(shard_query, cursor)
        search = data["search"]
        nodes = search["nodes"]
        page_info = search["pageInfo"]

        for repo in nodes:
            if stop_event.is_set():
                break

            row = RepoRow(
                namewithowner=repo["nameWithOwner"],
                stars=repo["stargazerCount"],
                language=(repo.get("primaryLanguage") or {}).get("name", ""),
                isarchived=repo["isArchived"],
                isfork=repo["isFork"],
                createdat=repo["createdAt"],
            )

            with seen_lock:
                if row.namewithowner in seen:
                    continue
                seen.add(row.namewithowner)

            target_queue.put(row)
            count += 1

        rate_limiter.maybe_wait()

        if stop_event.is_set() or not page_info["hasNextPage"]:
            break

        cursor = page_info["endCursor"]
        time.sleep(INTER_REQUEST_DELAY)

    return count


def print_header() -> None:
    print(
        f"\n{'#':>7}  {'Repository':<45}  {'Stars':>9}  "
        f"{'Language':<16}  {'Archived':<8}  {'Fork':<5}  {'Created'}"
    )
    print("-" * 115)


def writer_thread_fn(
    out_q: "queue.Queue[RepoRow]",
    stop_event: threading.Event,
    total_counter: dict,
) -> None:
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["rank", "nameWithOwner", "stars", "language", "isArchived", "isFork", "createdAt"]
        )

        while not stop_event.is_set() or not out_q.empty():
            try:
                row = out_q.get(timeout=0.5)
            except queue.Empty:
                continue

            total_counter["written"] += 1
            rank = total_counter["written"]

            writer.writerow([
                rank,
                row.namewithowner,
                row.stars,
                row.language,
                row.isarchived,
                row.isfork,
                row.createdat,
            ])

            if rank == 1 or rank % PRINT_EVERY == 0:
                archived = "yes" if row.isarchived else "no"
                fork = "yes" if row.isfork else "no"
                print(
                    f"{rank:>7}  {row.namewithowner:<45.45}  "
                    f"{row.stars:>9,}  {row.language:<16}  "
                    f"{archived:<8}  {fork:<5}  {row.createdat[:10]}"
                )

            if total_counter["written"] >= TOTAL_REPOS:
                stop_event.set()


def main() -> None:
    if not TOKEN:
        sys.exit("ERROR: Set the GITHUB_TOKEN environment variable first.")

    started = time.time()

    print(f"Fetching star counts for up to {TOTAL_REPOS:,} GitHub repos ...")
    print(f"Results will also be saved to: {OUTPUT_CSV}")
    print_header()

    print("\nDiscovering shards ...", file=sys.stderr)
    leaf_shards = discover_leaf_shards(INITIAL_STAR_SHARDS)
    print(f"[info] Using {len(leaf_shards)} leaf shards", file=sys.stderr)

    out_q: "queue.Queue[RepoRow]" = queue.Queue(maxsize=5000)
    stop_event = threading.Event()
    total_counter = {"written": 0}
    seen: set[str] = set()
    seen_lock = threading.Lock()

    writer = threading.Thread(
        target=writer_thread_fn,
        args=(out_q, stop_event, total_counter),
        daemon=True,
    )
    writer.start()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            pool.submit(crawl_shard, shard, out_q, seen, seen_lock, stop_event)
            for shard in leaf_shards
        ]

        for fut in as_completed(futures):
            fut.result()
            if total_counter["written"] >= TOTAL_REPOS:
                stop_event.set()
                break

    stop_event.set()
    writer.join()

    elapsed = time.time() - started
    print("-" * 115)
    print(f"\nDone. {total_counter['written']:,} repos written to {OUTPUT_CSV}.")
    print(f"Elapsed time: {elapsed/60:.1f} minutes.")


if __name__ == "__main__":
    main()