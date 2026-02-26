#!/usr/bin/env python3
"""
cache2sqlite.py - Package precomputed KSE cache as portable SQLite DLC

Converts *-cache.jsonl → search.sqlite with:
  - documents table (all cache fields as columns)
  - FTS5 virtual table for full-text keyword search
  - corpus_meta table (provenance + stats)

The .sqlite is the customer deliverable — no precompute needed on their box.
OpenPlanter loads it directly; gln-ranker can query it in --sqlite mode.

Usage:
  python3 scripts/cache2sqlite.py --cache data/od-cache.jsonl --out data/od.sqlite
  python3 scripts/cache2sqlite.py --cache data/salo-cache.jsonl --out data/salo.sqlite
  python3 scripts/cache2sqlite.py --all   # packages all *-cache.jsonl in data/

Stdlib only. No pip required.
"""

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR  = REPO_ROOT / "data"

SCHEMA_DOCUMENTS = """
CREATE TABLE IF NOT EXISTS documents (
    id          INTEGER PRIMARY KEY,
    path        TEXT NOT NULL,
    title       TEXT,
    gln         TEXT,
    gln_name    TEXT,
    confidence  REAL,
    cec         TEXT,
    cec_label   TEXT,
    jdn         TEXT,
    fgid_detected TEXT,      -- JSON array
    fgid_node   TEXT,
    archetype_prefix TEXT,
    candidates  TEXT,        -- JSON array
    snippet     TEXT,
    keywords    TEXT,        -- JSON array (top-20)
    sumo_concepts TEXT,      -- JSON array (precomputed SUMO)
    lang        TEXT,
    op          TEXT         -- first 80 lines of source document
);
"""

SCHEMA_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
    title,
    snippet,
    keywords,
    op,
    content=documents,
    content_rowid=id
);
"""

SCHEMA_META = """
CREATE TABLE IF NOT EXISTS corpus_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

# Trigger to keep FTS in sync (insert only; this is a static DLC)
TRIGGER_INSERT = """
CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
    INSERT INTO documents_fts(rowid, title, snippet, keywords, op)
    VALUES (new.id, new.title, new.snippet, new.keywords, new.op);
END;
"""


def jsonl_field(value):
    """Serialize a list/dict field to JSON string, or pass through scalar."""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def load_jsonl(path):
    entries = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"  WARN: skipping malformed line: {e}", file=sys.stderr)
    return entries


def build_sqlite(cache_path: Path, out_path: Path, corpus_name: str = None):
    print(f"Loading {cache_path} ...", file=sys.stderr)
    t0 = time.time()
    entries = load_jsonl(cache_path)
    print(f"  {len(entries):,} entries loaded ({time.time()-t0:.1f}s)", file=sys.stderr)

    if not entries:
        print("  ERROR: no entries found", file=sys.stderr)
        return False

    corpus_name = corpus_name or cache_path.stem.replace("-cache", "")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing db
    if out_path.exists():
        out_path.unlink()

    conn = sqlite3.connect(str(out_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Create schema
    conn.execute(SCHEMA_DOCUMENTS)
    conn.execute(SCHEMA_FTS)
    conn.execute(SCHEMA_META)
    conn.execute(TRIGGER_INSERT)

    # Sample the first entry to learn available keys
    sample = entries[0]
    all_keys = {
        "path", "title", "gln", "gln_name", "confidence", "cec", "cec_label",
        "jdn", "fgid_detected", "fgid_node", "archetype_prefix", "candidates",
        "snippet", "keywords", "sumo_concepts", "lang", "op",
    }

    # Insert documents
    print(f"  Inserting {len(entries):,} rows ...", file=sys.stderr)
    t1 = time.time()
    rows_inserted = 0

    with conn:
        for i, entry in enumerate(entries):
            row = {}
            for key in all_keys:
                val = entry.get(key)
                row[key] = jsonl_field(val) if val is not None else None

            # Extract op from snippet if not stored separately
            if not row.get("op") and row.get("snippet"):
                row["op"] = row["snippet"]

            conn.execute(
                """INSERT INTO documents
                   (path, title, gln, gln_name, confidence, cec, cec_label, jdn,
                    fgid_detected, fgid_node, archetype_prefix, candidates,
                    snippet, keywords, sumo_concepts, lang, op)
                   VALUES
                   (:path, :title, :gln, :gln_name, :confidence, :cec, :cec_label, :jdn,
                    :fgid_detected, :fgid_node, :archetype_prefix, :candidates,
                    :snippet, :keywords, :sumo_concepts, :lang, :op)
                """,
                row,
            )
            rows_inserted += 1
            if (i + 1) % 2000 == 0:
                print(f"    {i+1:,}/{len(entries):,}", file=sys.stderr)

    elapsed = time.time() - t1
    print(f"  Inserted {rows_inserted:,} rows ({elapsed:.1f}s)", file=sys.stderr)

    # FTS rebuild (explicit optimize for large corpora)
    print("  Building FTS index ...", file=sys.stderr)
    conn.execute("INSERT INTO documents_fts(documents_fts) VALUES('rebuild')")
    conn.execute("INSERT INTO documents_fts(documents_fts) VALUES('optimize')")
    conn.commit()

    # CEC distribution
    cec_dist = {}
    for row in conn.execute("SELECT cec, COUNT(*) FROM documents GROUP BY cec ORDER BY 2 DESC LIMIT 10"):
        cec_dist[row[0] or "?"] = row[1]

    lang_dist = {}
    for row in conn.execute("SELECT lang, COUNT(*) FROM documents GROUP BY lang ORDER BY 2 DESC LIMIT 5"):
        lang_dist[row[0] or "?"] = row[1]

    # Metadata
    meta = {
        "corpus":       corpus_name,
        "source_cache": str(cache_path),
        "doc_count":    str(rows_inserted),
        "built_at":     str(int(time.time())),
        "cec_top10":    json.dumps(cec_dist),
        "lang_dist":    json.dumps(lang_dist),
        "schema_ver":   "1.0",
    }
    with conn:
        for k, v in meta.items():
            conn.execute("INSERT OR REPLACE INTO corpus_meta (key, value) VALUES (?, ?)", (k, v))

    conn.execute("PRAGMA optimize")
    conn.close()

    size_mb = out_path.stat().st_size / 1_048_576
    total = time.time() - t0
    print(f"  Done: {out_path} ({size_mb:.1f} MB, {total:.1f}s total)", file=sys.stderr)
    return True


def main():
    p = argparse.ArgumentParser(description="Package KSE cache as portable SQLite DLC")
    p.add_argument("--cache", "-c", help="Path to *-cache.jsonl")
    p.add_argument("--out",   "-o", help="Output .sqlite path")
    p.add_argument("--name",  "-n", help="Corpus name (default: derived from cache filename)")
    p.add_argument("--all",   "-a", action="store_true",
                   help=f"Package all *-cache.jsonl in {DATA_DIR}/")
    args = p.parse_args()

    if args.all:
        caches = list(DATA_DIR.glob("*-cache.jsonl"))
        if not caches:
            print(f"No *-cache.jsonl found in {DATA_DIR}", file=sys.stderr)
            sys.exit(1)
        ok = 0
        for cache_path in sorted(caches):
            corpus = cache_path.stem.replace("-cache", "")
            out_path = DATA_DIR / f"{corpus}.sqlite"
            if build_sqlite(cache_path, out_path, corpus):
                ok += 1
        print(f"\nPackaged {ok}/{len(caches)} corpora.", file=sys.stderr)

    elif args.cache:
        cache_path = Path(args.cache)
        if not cache_path.exists():
            print(f"ERROR: {cache_path} not found", file=sys.stderr)
            sys.exit(1)
        corpus = args.name or cache_path.stem.replace("-cache", "")
        out_path = Path(args.out) if args.out else cache_path.with_suffix(".sqlite")
        build_sqlite(cache_path, out_path, corpus)

    else:
        p.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
