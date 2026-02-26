#!/usr/bin/env python3
"""
od-thread-cache.py - Build GLN cache from OD-2006 forum threads

Reads thread titles + first-post content from posts_markdown.db,
runs each through gln-resolver scoring, and writes a JSONL cache
in the same format as gln-cache.jsonl for use with gln-ranker.py.

Usage:
  python3 scripts/od-thread-cache.py
  python3 scripts/od-thread-cache.py --limit 500
  python3 scripts/od-thread-cache.py --db /path/to/posts_markdown.db --output data/od-cache.jsonl
"""

import argparse
import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_DB = Path("/home/john/od-2006/posts_markdown.db")
DEFAULT_OUTPUT = REPO_ROOT / "data" / "od-cache.jsonl"
CONTENT_CHARS = 800   # chars of first post to feed resolver
SNIPPET_CHARS = 200   # chars of snippet to store in cache


def load_resolver():
    """Dynamically import gln-resolver.py as a module."""
    resolver_path = SCRIPT_DIR / "gln-resolver.py"
    spec = importlib.util.spec_from_file_location("gln_resolver", resolver_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def fetch_threads(db_path, limit=0):
    """
    Join thread_first_post with posts to get title + first-post content.
    Returns list of dicts: threadid, title, username, dateline, content, post_count.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    limit_clause = f"LIMIT {limit}" if limit > 0 else ""

    cur.execute(f"""
        SELECT
            t.threadid,
            t.title,
            t.username,
            t.userid,
            t.dateline,
            p.pagetext_md AS content,
            (SELECT COUNT(*) FROM posts WHERE threadid = t.threadid) AS post_count
        FROM thread_first_post t
        JOIN posts p ON p.postid = t.postid
        WHERE t.title IS NOT NULL AND t.title != ''
        ORDER BY t.dateline
        {limit_clause}
    """)

    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def make_snippet(text, max_chars=SNIPPET_CHARS):
    """Extract a clean snippet from BBCode/markdown text."""
    if not text:
        return ""
    # Strip common BBCode tags
    import re
    text = re.sub(r'\[quote[^\]]*\].*?\[/quote\]', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'\[[^\]]+\]', '', text)
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rsplit(' ', 1)[0] + '...'


def main():
    parser = argparse.ArgumentParser(
        description="Build GLN cache from OD-2006 forum threads"
    )
    parser.add_argument("--db", default=str(DEFAULT_DB),
                        help=f"Path to posts_markdown.db (default: {DEFAULT_DB})")
    parser.add_argument("--output", "-o", default=str(DEFAULT_OUTPUT),
                        help=f"Output JSONL path (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--limit", "-n", type=int, default=0,
                        help="Limit to first N threads (0 = all)")
    parser.add_argument("--index", "-i", default=str(REPO_ROOT / "llms-N200-index.yaml"),
                        help="Path to llms-N200-index.yaml")
    args = parser.parse_args()

    db_path = Path(args.db)
    output_path = Path(args.output)

    if not db_path.exists():
        print(f"ERROR: DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    # Load resolver
    print("Loading GLN resolver...", file=sys.stderr)
    resolver = load_resolver()

    print(f"Loading GLN tree from {args.index}...", file=sys.stderr)
    nodes = resolver.load_gln_tree(args.index)
    print(f"  {len(nodes)} GLN nodes loaded", file=sys.stderr)

    # Fetch threads
    print(f"Fetching threads from {db_path}...", file=sys.stderr)
    threads = fetch_threads(str(db_path), limit=args.limit)
    print(f"  {len(threads)} threads to process", file=sys.stderr)

    # Process
    output_path.parent.mkdir(parents=True, exist_ok=True)
    results = []

    for i, t in enumerate(threads):
        title = t["title"] or ""
        content = (t["content"] or "")[:CONTENT_CHARS]

        # Synthetic path for compatibility with gln-ranker.py
        path = f"od-2006/threads/{t['threadid']}"

        result = resolver.resolve_gln(title, content, path, nodes)

        pred = result.get("prediction", {})
        axes = result.get("axes", {})
        candidates = result.get("candidates", [])

        entry = {
            "path": path,
            "title": title,
            "gln": pred.get("gln", ""),
            "gln_name": pred.get("name", ""),
            "confidence": pred.get("confidence", 0.0),
            "cec": axes.get("cec", {}).get("class"),
            "cec_label": axes.get("cec", {}).get("label"),
            "jdn": axes.get("cec", {}).get("jdn"),
            "fgid_detected": axes.get("fgid", {}).get("detected", []),
            "fgid_node": axes.get("fgid", {}).get("node_fgid", []),
            "archetype_prefix": axes.get("archetype", {}).get("prefix"),
            "candidates": [c.get("gln", "") for c in candidates[:5]],
            "snippet": make_snippet(t["content"] or ""),
            # OD-specific metadata
            "threadid": t["threadid"],
            "username": t["username"],
            "dateline": t["dateline"],
            "post_count": t["post_count"],
        }
        results.append(entry)

        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{len(threads)} processed...", file=sys.stderr)

    with open(output_path, "w", encoding="utf-8") as f:
        for entry in results:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    # Quick stats
    from collections import Counter
    cec_dist = Counter(e["cec"] for e in results if e["cec"])
    print(f"\nWrote {len(results)} entries to {output_path}", file=sys.stderr)
    print(f"CEC distribution (top 10): {cec_dist.most_common(10)}", file=sys.stderr)


if __name__ == "__main__":
    main()
