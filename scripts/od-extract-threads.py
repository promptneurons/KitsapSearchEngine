#!/usr/bin/env python3
"""
od-extract-threads.py - Extract OD-2006 threads to per-thread YAML files

Creates one YAML file per thread in output_dir/, enriched with up to 80 lines
of the opening post (OP). Also writes a thread-index.yaml compatible with
gln-precompute.py for downstream ranking.

Usage:
  python3 scripts/od-extract-threads.py
  python3 scripts/od-extract-threads.py --limit 500 --output data/od-threads/
  python3 scripts/od-extract-threads.py --db /path/to/posts_markdown.db
"""

import argparse
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required. pip3 install pyyaml", file=sys.stderr)
    sys.exit(1)

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_DB = Path("/home/john/od-2006/posts_markdown.db")
DEFAULT_OUTPUT = REPO_ROOT / "data" / "od-threads"
DEFAULT_INDEX = REPO_ROOT / "data" / "od-thread-index.yaml"
OP_MAX_LINES = 80


def clean_bbcode(text):
    """Strip BBCode tags and normalize whitespace."""
    if not text:
        return ""
    # Remove quote blocks entirely (too noisy for classification)
    text = re.sub(r'\[quote[^\]]*\].*?\[/quote\]', '[quoted text]', text,
                  flags=re.DOTALL | re.IGNORECASE)
    # Strip remaining tags
    text = re.sub(r'\[[^\]]+\]', '', text)
    # Normalize whitespace
    text = re.sub(r'\r\n', '\n', text)
    text = re.sub(r'\r', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def get_op_lines(text, max_lines=OP_MAX_LINES):
    """Return up to max_lines lines of cleaned OP content."""
    cleaned = clean_bbcode(text)
    lines = cleaned.splitlines()
    # Drop blank-only lines at start
    while lines and not lines[0].strip():
        lines.pop(0)
    return lines[:max_lines]


def fetch_threads(db_path, limit=0):
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
            p.pagetext_md AS op_text,
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


def dateline_to_iso(ts):
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d')
    except Exception:
        return str(ts)


def write_thread_yaml(thread, output_dir):
    """Write a single thread YAML file. Returns the path."""
    tid = thread['threadid']
    fname = f"thread-{tid:06d}.yaml"
    fpath = output_dir / fname

    op_lines = get_op_lines(thread.get('op_text') or '')

    doc = {
        'threadid': tid,
        'title': thread['title'],
        'username': thread.get('username', ''),
        'userid': thread.get('userid'),
        'date': dateline_to_iso(thread.get('dateline', 0)),
        'dateline': thread.get('dateline'),
        'post_count': thread.get('post_count', 0),
        'op': '\n'.join(op_lines),
    }

    with open(fpath, 'w', encoding='utf-8') as f:
        yaml.dump(doc, f, allow_unicode=True, default_flow_style=False,
                  sort_keys=False)

    return fpath


def write_index(threads, output_dir, index_path):
    """
    Write od-thread-index.yaml in a format gln-precompute.py can walk.
    Sections are grouped by year for loose subject structure.
    """
    from collections import defaultdict

    by_year = defaultdict(list)
    for t in threads:
        year = dateline_to_iso(t.get('dateline', 0))[:4]
        by_year[year].append(t)

    sections = []
    for year in sorted(by_year.keys()):
        files = []
        for t in by_year[year]:
            fname = f"thread-{t['threadid']:06d}.yaml"
            files.append({
                'path': str((output_dir / fname).relative_to(Path('/home/john/KitsapSearchEngine'))),
                'title': t['title'],
            })
        sections.append({
            'name': f"od-{year}",
            'gln': '',
            'fgid': [],
            'files': files,
            'children': [],
        })

    index = {
        'name': 'OD-2006 Thread Archive',
        'source': 'posts_markdown.db',
        'sections': sections,
    }

    with open(index_path, 'w', encoding='utf-8') as f:
        yaml.dump(index, f, allow_unicode=True, default_flow_style=False,
                  sort_keys=False)

    print(f"Index written: {index_path} ({len(threads)} threads, "
          f"{len(sections)} year sections)", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Extract OD-2006 threads to per-thread YAML files"
    )
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--output", "-o", default=str(DEFAULT_OUTPUT),
                        help=f"Output directory for YAML files (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--index", default=str(DEFAULT_INDEX),
                        help=f"Output path for thread index YAML (default: {DEFAULT_INDEX})")
    parser.add_argument("--limit", "-n", type=int, default=0,
                        help="Limit to first N threads (0 = all)")
    args = parser.parse_args()

    db_path = Path(args.db)
    output_dir = Path(args.output)
    index_path = Path(args.index)

    if not db_path.exists():
        print(f"ERROR: DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching threads from {db_path}...", file=sys.stderr)
    threads = fetch_threads(str(db_path), limit=args.limit)
    print(f"  {len(threads)} threads", file=sys.stderr)

    print(f"Writing YAML files to {output_dir}...", file=sys.stderr)
    for i, t in enumerate(threads):
        write_thread_yaml(t, output_dir)
        if (i + 1) % 500 == 0:
            print(f"  {i+1}/{len(threads)}...", file=sys.stderr)

    write_index(threads, output_dir, index_path)
    print(f"Done. {len(threads)} thread YAMLs + index.", file=sys.stderr)


if __name__ == "__main__":
    main()
