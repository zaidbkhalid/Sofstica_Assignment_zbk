import os
import sys
import csv
import json
import time
import queue
import random
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ---------------- Config ----------------
TOKEN = os.environ.get("GITHUB_TOKEN", "")
GRAPHQL_URL = "https://api.github.com/graphql"

TOTAL_REPOS = 50_000
PAGE_SIZE = 100
MAX_WORKERS = 4
INTER_REQUEST_DELAY = 0.25
MIN_POINTS_LEFT = 250
MAX_RETRIES = 5
BASE_BACKOFF = 2.0

OUTPUT_CSV = "github_stars.csv"
STATE_FILE = "crawl_state.json"
SEEN_FILE = "seen_repos.json"
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
    rank: int
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


class ProgressStore:
    def __init__(self, state_file: str, seen_file: str) -> None:
        self.state_file = state_file
        self.seen_file = seen_file
        self.lock = threading.Lock()

        self.state = {
            "mode": "discovering",
            "written": 0,
            "leaf_shards": [],
            "shards": {},
        }
        self.seen = set()

    def load(self) -> None:
        if os.path.exists(self.state_file):
            with open(self.state_file, "r", encoding="utf-8") as fh:
                self.state = json.load(fh)

        if os.path.exists(self.seen_file):
            with open(self.seen_file, "r", encoding="utf-8") as fh:
                seen_list = json.load(fh)
                self.seen = set(seen_list)

    def save(self) -> None:
        with self.lock:
            with open(self.state_file, "w", encoding="utf-8") as fh:
                json.dump(self.state, fh, indent=2)

            with open(self.seen_file, "w", encoding="utf-8") as fh:
                json.dump(sorted(self.seen), fh)

    def set_mode(self, mode: str) -> None:
        with self.lock:
            self.state["mode"] = mode
        self.save()

    def set_leaf_shards(self, shards: list[str]) -> None:
        with self.lock:
            self.state["leaf_shards"] = shards
            for shard in shards:
                if shard not in self.state["shards"]:
                    self.state["shards"][shard] = {
                        "status": "pending",
                        "cursor": None,
                    }
        self.save()

    def update_shard(self, shard: str, status: str | None = None, cursor: str | None = None) -> None:
        with self.lock:
            if shard not in self.state["shards"]:
                self.state["shards"][shard] = {"status": "pending", "cursor": None}
            if status is not None:
                self.state["shards"][shard]["status"] = status
            if cursor is not None or cursor is None:
                self.state["shards"][shard]["cursor"] = cursor
        self.save()

    def get_shard_cursor(self, shard: str) -> str | None:
        return self.state["shards"].get(shard, {}).get("cursor")

    def get_pending_shards(self) -> list[str]:
        shards = self.state.get("leaf_shards", [])
        out = []
        for shard in shards:
            status = self.state["shards"].get(shard, {}).get("status", "pending")
            if status in {"pending", "paused", "running"}:
                out.append(shard)
        return out

    def add_seen(self, name: str) -> bool:
        with self.lock:
            if name in self.seen:
                return False
            self.seen.add(name)
            return True

    def increment_written(self) -> int:
        with self.lock:
            self.state["written"] += 1
            return self.state["written"]

    def get_written(self) -> int:
        return int(self.state.get("written", 0))


progress = ProgressStore(STATE_FILE, SEEN_FILE)


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
    if progress.state.get("mode") == "crawling" and progress.state.get("leaf_shards"):
        print("[info] Reusing previously discovered leaf shards.", file=sys.stderr)
        return progress.state["leaf_shards"]

    progress.set_mode("discovering")

    pending = list(initial_shards)
    leaves: list[str] = []

    while pending:
        shard = pending.pop(0)
        print(f"[discover] checking shard: {shard}", file=sys.stderr)

        data = gql_request(shard, None)
        time.sleep(INTER_REQUEST_DELAY)

        count = data["search"]["repositoryCount"]
        print(f"[discover] repositoryCount={count} for shard {shard}", file=sys.stderr)

        if count > 1000:
            parts = split_shard(shard)
            if parts == [shard]:
                leaves.append(shard)
            else:
                print(f"[discover] splitting {shard} into {parts}", file=sys.stderr)
                pending.extend(parts)
        else:
            leaves.append(shard)

        rate_limiter.maybe_wait()

    progress.set_leaf_shards(leaves)
    progress.set_mode("crawling")
    return leaves


def load_existing_rows() -> None:
    """
    If CSV already exists, restore state from it when possible.
    This keeps resume behavior sane even after interruption.
    """
    if not os.path.exists(OUTPUT_CSV):
        return

    # If state already knows written > 0, trust state.
    if progress.get_written() > 0:
        return

    written = 0
    with open(OUTPUT_CSV, "r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            name = row["nameWithOwner"].strip()
            progress.seen.add(name)
            written += 1

    progress.state["written"] = written
    progress.save()


def ensure_csv_header() -> None:
    if os.path.exists(OUTPUT_CSV):
        return

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["rank", "nameWithOwner", "stars", "language", "isArchived", "isFork", "createdAt"]
        )


def crawl_shard(
    shard_query: str,
    target_queue: "queue.Queue[RepoRow]",
    stop_event: threading.Event,
) -> int:
    cursor = progress.get_shard_cursor(shard_query)
    progress.update_shard(shard_query, status="running", cursor=cursor)

    count = 0
    exhausted = False

    while not stop_event.is_set():
        data = gql_request(shard_query, cursor)
        search = data["search"]
        nodes = search["nodes"]
        page_info = search["pageInfo"]

        for repo in nodes:
            if stop_event.is_set():
                break

            name = repo["nameWithOwner"]
            added = progress.add_seen(name)
            if not added:
                continue

            rank = progress.increment_written()
            row = RepoRow(
                rank=rank,
                namewithowner=name,
                stars=repo["stargazerCount"],
                language=(repo.get("primaryLanguage") or {}).get("name", ""),
                isarchived=repo["isArchived"],
                isfork=repo["isFork"],
                createdat=repo["createdAt"],
            )

            target_queue.put(row)
            count += 1

            if rank >= TOTAL_REPOS:
                stop_event.set()
                break

        rate_limiter.maybe_wait()

        if not page_info["hasNextPage"]:
            exhausted = True
            break

        if stop_event.is_set():
            break

        cursor = page_info["endCursor"]
        progress.update_shard(shard_query, status="running", cursor=cursor)
        time.sleep(INTER_REQUEST_DELAY)

    if exhausted:
        progress.update_shard(shard_query, status="done", cursor=cursor)
    else:
        progress.update_shard(shard_query, status="paused", cursor=cursor)

    progress.save()
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
) -> None:
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)

        while not stop_event.is_set() or not out_q.empty():
            try:
                row = out_q.get(timeout=0.5)
            except queue.Empty:
                continue

            writer.writerow([
                row.rank,
                row.namewithowner,
                row.stars,
                row.language,
                row.isarchived,
                row.isfork,
                row.createdat,
            ])
            fh.flush()

            if row.rank == 1 or row.rank % PRINT_EVERY == 0:
                archived = "yes" if row.isarchived else "no"
                fork = "yes" if row.isfork else "no"
                print(
                    f"{row.rank:>7}  {row.namewithowner:<45.45}  "
                    f"{row.stars:>9,}  {row.language:<16}  "
                    f"{archived:<8}  {fork:<5}  {row.createdat[:10]}"
                )

            if row.rank % 100 == 0:
                progress.save()

            if row.rank >= TOTAL_REPOS:
                stop_event.set()


def main() -> None:
    if not TOKEN:
        sys.exit("ERROR: Set the GITHUB_TOKEN environment variable first.")

    started = time.time()

    progress.load()
    ensure_csv_header()
    load_existing_rows()

    print(f"Fetching star counts for up to {TOTAL_REPOS:,} GitHub repos ...")
    print(f"Results will be saved to: {OUTPUT_CSV}")
    print(f"Checkpoint state: {STATE_FILE}")
    print_header()

    leaf_shards = discover_leaf_shards(INITIAL_STAR_SHARDS)
    print(f"[info] Using {len(leaf_shards)} leaf shards", file=sys.stderr)

    pending_shards = progress.get_pending_shards()
    print(f"[info] Pending shards to crawl: {len(pending_shards)}", file=sys.stderr)
    print(f"[info] Already written: {progress.get_written()}", file=sys.stderr)

    if progress.get_written() >= TOTAL_REPOS:
        print("[info] Target already reached. Nothing to do.", file=sys.stderr)
        return

    out_q: "queue.Queue[RepoRow]" = queue.Queue(maxsize=5000)
    stop_event = threading.Event()

    writer = threading.Thread(
        target=writer_thread_fn,
        args=(out_q, stop_event),
        daemon=True,
    )
    writer.start()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            pool.submit(crawl_shard, shard, out_q, stop_event)
            for shard in pending_shards
        ]

        for fut in as_completed(futures):
            fut.result()
            if progress.get_written() >= TOTAL_REPOS:
                stop_event.set()
                break

    stop_event.set()
    writer.join()
    progress.save()

    elapsed = time.time() - started
    print("-" * 115)
    print(f"\nDone. {progress.get_written():,} repos written to {OUTPUT_CSV}.")
    print(f"Elapsed time: {elapsed/60:.1f} minutes.")


if __name__ == "__main__":
    main()