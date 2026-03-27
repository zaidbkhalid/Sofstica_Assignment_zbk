import os
import sys
import time
import random
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db import engine, SessionLocal, Base
from models import RepositoryCore, RepositoryMeta, CrawlShardProgress

# ---------------- Config ----------------
TOKEN = os.environ.get("GITHUB_TOKEN", "")
GRAPHQL_URL = "https://api.github.com/graphql"

TOTAL_REPOS = 100_000
PAGE_SIZE = 100
MAX_WORKERS = 4
INTER_REQUEST_DELAY = 0.25
MIN_POINTS_LEFT = 250
MAX_RETRIES = 5
BASE_BACKOFF = 2.0
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


def ensure_schema() -> None:
    Base.metadata.create_all(bind=engine)


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


def get_current_repo_count() -> int:
    with SessionLocal() as session:
        return int(session.scalar(select(func.count(RepositoryCore.id))) or 0)


def get_next_rank_start() -> int:
    with SessionLocal() as session:
        max_rank = session.scalar(select(func.max(RepositoryMeta.rank)))
        return int(max_rank or 0) + 1


def load_existing_leaf_shards() -> list[str]:
    with SessionLocal() as session:
        rows = session.scalars(
            select(CrawlShardProgress.shard_key).order_by(CrawlShardProgress.shard_key)
        ).all()
        return list(rows)


def save_leaf_shards(shards: list[str]) -> None:
    if not shards:
        return

    with SessionLocal.begin() as session:
        stmt = pg_insert(CrawlShardProgress.__table__).values(
            [{"shard_key": shard, "status": "pending", "cursor": None} for shard in shards]
        )
        stmt = stmt.on_conflict_do_nothing(index_elements=["shard_key"])
        session.execute(stmt)


def set_shard_state(
    shard_key: str,
    status: str | None = None,
    cursor: str | None = None,
    discovered_count: int | None = None,
) -> None:
    with SessionLocal.begin() as session:
        values = {"shard_key": shard_key}
        if status is not None:
            values["status"] = status
        if cursor is not None or cursor is None:
            values["cursor"] = cursor
        if discovered_count is not None:
            values["discovered_count"] = discovered_count

        stmt = pg_insert(CrawlShardProgress.__table__).values(values)
        update_set = {}
        if status is not None:
            update_set["status"] = status
        if cursor is not None or cursor is None:
            update_set["cursor"] = cursor
        if discovered_count is not None:
            update_set["discovered_count"] = discovered_count

        if update_set:
            stmt = stmt.on_conflict_do_update(
                index_elements=["shard_key"],
                set_=update_set,
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=["shard_key"])

        session.execute(stmt)


def get_pending_shards() -> list[str]:
    with SessionLocal() as session:
        rows = session.scalars(
            select(CrawlShardProgress.shard_key)
            .where(CrawlShardProgress.status.in_(["pending", "paused", "running"]))
            .order_by(CrawlShardProgress.shard_key)
        ).all()
        return list(rows)


def get_shard_cursor(shard_key: str) -> str | None:
    with SessionLocal() as session:
        row = session.get(CrawlShardProgress, shard_key)
        return None if row is None else row.cursor


def discover_leaf_shards(initial_shards: list[str]) -> list[str]:
    existing = load_existing_leaf_shards()
    if existing:
        print("[info] Reusing previously discovered leaf shards.", file=sys.stderr)
        return existing

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

        set_shard_state(shard, status="pending", cursor=None, discovered_count=count)
        rate_limiter.maybe_wait()

    save_leaf_shards(leaves)
    return leaves


def upsert_batch(nodes: list[dict], rank_start: int) -> None:
    if not nodes:
        return

    core_rows = []
    for repo in nodes:
        core_rows.append(
            {
                "namewithowner": repo["nameWithOwner"],
                "stars": repo["stargazerCount"],
                "createdat": repo["createdAt"],
            }
        )

    with SessionLocal.begin() as session:
        core_stmt = pg_insert(RepositoryCore.__table__).values(core_rows)
        core_stmt = core_stmt.on_conflict_do_update(
            index_elements=["namewithowner"],
            set_={
                "stars": core_stmt.excluded.stars,
                "createdat": core_stmt.excluded.createdat,
            },
        ).returning(
            RepositoryCore.id,
            RepositoryCore.namewithowner,
        )

        returned = session.execute(core_stmt).all()
        name_to_id = {row.namewithowner: row.id for row in returned}

        if len(name_to_id) < len(nodes):
            missing_names = [
                repo["nameWithOwner"]
                for repo in nodes
                if repo["nameWithOwner"] not in name_to_id
            ]
            if missing_names:
                rows = session.execute(
                    select(RepositoryCore.id, RepositoryCore.namewithowner).where(
                        RepositoryCore.namewithowner.in_(missing_names)
                    )
                ).all()
                for row in rows:
                    name_to_id[row.namewithowner] = row.id

        meta_rows = []
        for i, repo in enumerate(nodes):
            repo_id = name_to_id[repo["nameWithOwner"]]
            meta_rows.append(
                {
                    "id": repo_id,
                    "rank": rank_start + i,
                    "language": (repo.get("primaryLanguage") or {}).get("name"),
                    "isarchived": repo["isArchived"],
                    "isfork": repo["isFork"],
                }
            )

        meta_stmt = pg_insert(RepositoryMeta.__table__).values(meta_rows)
        meta_stmt = meta_stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "language": meta_stmt.excluded.language,
                "isarchived": meta_stmt.excluded.isarchived,
                "isfork": meta_stmt.excluded.isfork,
                # preserve existing rank on updates; only new rows use inserted rank
            },
        )
        session.execute(meta_stmt)


def print_header() -> None:
    print(
        f"\n{'#':>7}  {'Repository':<45}  {'Stars':>9}  "
        f"{'Language':<16}  {'Archived':<8}  {'Fork':<5}  {'Created'}"
    )
    print("-" * 115)


def print_repo(rank: int, repo: dict) -> None:
    lang = (repo.get("primaryLanguage") or {}).get("name", "")
    archived = "yes" if repo["isArchived"] else "no"
    fork = "yes" if repo["isFork"] else "no"
    print(
        f"{rank:>7}  {repo['nameWithOwner'][:45]:<45}  "
        f"{repo['stargazerCount']:>9,}  {lang:<16}  "
        f"{archived:<8}  {fork:<5}  {repo['createdAt'][:10]}"
    )


counter_lock = threading.Lock()
state = {
    "current_count": 0,
    "next_rank": 1,
}


def crawl_shard(shard_query: str, stop_event: threading.Event) -> None:
    cursor = get_shard_cursor(shard_query)
    set_shard_state(shard_query, status="running", cursor=cursor)

    exhausted = False

    while not stop_event.is_set():
        with counter_lock:
            if state["current_count"] >= TOTAL_REPOS:
                stop_event.set()
                break

        data = gql_request(shard_query, cursor)
        search = data["search"]
        nodes = search["nodes"]
        page_info = search["pageInfo"]

        if not nodes:
            exhausted = True
            break

        with counter_lock:
            rank_start = state["next_rank"]
            state["next_rank"] += len(nodes)

        upsert_batch(nodes, rank_start)

        new_count = get_current_repo_count()
        with counter_lock:
            previous = state["current_count"]
            state["current_count"] = max(state["current_count"], new_count)
            current = state["current_count"]

        if current // PRINT_EVERY > previous // PRINT_EVERY:
            last_repo = nodes[-1]
            print_repo(current, last_repo)

        if current >= TOTAL_REPOS:
            stop_event.set()

        rate_limiter.maybe_wait()

        if not page_info["hasNextPage"]:
            exhausted = True
            break

        if stop_event.is_set():
            break

        cursor = page_info["endCursor"]
        set_shard_state(shard_query, status="running", cursor=cursor)
        time.sleep(INTER_REQUEST_DELAY)

    if exhausted:
        set_shard_state(shard_query, status="done", cursor=cursor)
    else:
        set_shard_state(shard_query, status="paused", cursor=cursor)


def main() -> None:
    if not TOKEN:
        sys.exit("ERROR: Set the GITHUB_TOKEN environment variable first.")

    ensure_schema()

    started = time.time()
    state["current_count"] = get_current_repo_count()
    state["next_rank"] = get_next_rank_start()

    print(f"Fetching star counts for up to {TOTAL_REPOS:,} GitHub repos directly into PostgreSQL ...")
    print_header()

    leaf_shards = discover_leaf_shards(INITIAL_STAR_SHARDS)
    print(f"[info] Using {len(leaf_shards)} leaf shards", file=sys.stderr)

    pending_shards = get_pending_shards()
    print(f"[info] Pending shards to crawl: {len(pending_shards)}", file=sys.stderr)
    print(f"[info] Already written in DB: {state['current_count']}", file=sys.stderr)

    if state["current_count"] >= TOTAL_REPOS:
        print("[info] Target already reached. Nothing to do.", file=sys.stderr)
        return

    stop_event = threading.Event()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(crawl_shard, shard, stop_event) for shard in pending_shards]

        for fut in as_completed(futures):
            fut.result()
            with counter_lock:
                if state["current_count"] >= TOTAL_REPOS:
                    stop_event.set()
                    break

    elapsed = time.time() - started
    final_count = get_current_repo_count()

    print("-" * 115)
    print(f"\nDone. {final_count:,} repos stored in PostgreSQL.")
    print(f"Elapsed time: {elapsed/60:.1f} minutes.")


if __name__ == "__main__":
    main()