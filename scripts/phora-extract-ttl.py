#!/usr/bin/env python3
"""
phora-extract-ttl.py - Build Phora Nova thread YAMLs from phora-threads.ttl

Uses TTL metadata (MEC categories, NER, forum categories) as primary source.
No local HTML available — OP text comes from live fetch if --fetch flag set,
otherwise metadata-only.

Sources:
  data/phora-threads.ttl   — thread index from phora2rdf.py

Output:
  data/phora-threads/thread-NNNN.yaml
  data/phora-thread-index.yaml

Usage:
  python3 scripts/phora-extract-ttl.py
  python3 scripts/phora-extract-ttl.py --ttl /path/to/phora-threads.ttl
  python3 scripts/phora-extract-ttl.py --limit 20
"""

import argparse
import html as _html
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required.", file=sys.stderr)
    sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TTL = REPO_ROOT / "data" / "phora-threads.ttl"
OUT_DIR     = REPO_ROOT / "data" / "phora-threads"
INDEX_OUT   = REPO_ROOT / "data" / "phora-thread-index.yaml"
OP_MAX_LINES = 80


# ─────────────────────────────────────────────────────────────────────────────
# TTL parser — Phora variant of salo-extract-ttl parser
# ─────────────────────────────────────────────────────────────────────────────

def _unquote(s):
    s = s.strip()
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return s.replace('\\"', '"').replace("\\n", "\n").strip()


def parse_ttl(path):
    """
    Parse phora-threads.ttl → dict: threadId → metadata dict.

    Returns {
      "1875": {
        "title": "...",
        "url": "...",
        "creator": "...",
        "comment_count": 164,
        "forum_category": "Political arena- North America",
        "mec_categories": ["general", ...],
        "entities_person": [...],
        "entities_org": [...],
        "entities_place": [...],
      }, ...
    }
    """
    threads = {}

    re_thread  = re.compile(r'^phora:thread-(\d+)\s')
    re_title   = re.compile(r'dct:title\s+"([^"]*)"')
    re_label   = re.compile(r'rdfs:label\s+"([^"]*)"')
    re_url     = re.compile(r'schema:url\s+<([^>]+)>')
    re_creator = re.compile(r'dct:creator\s+"([^"]*)"')
    re_count   = re.compile(r'schema:commentCount\s+(\d+)')
    re_forum   = re.compile(r'salo:forumCategory\s+"([^"]*)"')
    re_mec     = re.compile(r'salo:mecCategory\s+mec:(\S+?)[\s;.]')
    re_person  = re.compile(r'salo:mentionsPerson\s+"([^"]+)"')
    re_org     = re.compile(r'salo:mentionsOrg\s+"([^"]+)"')
    re_place   = re.compile(r'salo:mentionsPlace\s+"([^"]+)"')

    text   = Path(path).read_text(encoding="utf-8", errors="replace")
    blocks = re.split(r'\n(?=phora:thread-)', text)

    for block in blocks:
        m = re_thread.match(block)
        if not m:
            continue
        tid = m.group(1)

        title = ""
        m2 = re_title.search(block)
        if m2:
            title = _html.unescape(m2.group(1))
        else:
            m2 = re_label.search(block)
            if m2:
                title = _html.unescape(m2.group(1))

        url = ""
        m3 = re_url.search(block)
        if m3:
            url = m3.group(1)

        creator = ""
        m4 = re_creator.search(block)
        if m4:
            creator = m4.group(1)

        comment_count = 0
        m5 = re_count.search(block)
        if m5:
            comment_count = int(m5.group(1))

        forum_cat = ""
        m6 = re_forum.search(block)
        if m6:
            forum_cat = m6.group(1)

        mec_cats   = re_mec.findall(block)
        persons    = re_person.findall(block)
        orgs       = re_org.findall(block)
        places     = re_place.findall(block)

        threads[tid] = {
            "title":           title,
            "url":             url,
            "creator":         creator,
            "comment_count":   comment_count,
            "forum_category":  forum_cat,
            "mec_categories":  mec_cats,
            "entities_person": persons,
            "entities_org":    orgs,
            "entities_place":  places,
        }

    return threads


# ─────────────────────────────────────────────────────────────────────────────
# Optional live OP fetch
# ─────────────────────────────────────────────────────────────────────────────

def fetch_op(url, max_lines=OP_MAX_LINES, timeout=10):
    """Fetch the first post from a Phora thread URL. Returns text or ''."""
    try:
        import urllib.request
        import html as _h
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; KitsapSearch/1.0)"
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", errors="replace")

        # Extract first .message-body equivalent — Xenforo .message-content
        m = re.search(
            r'class="[^"]*bbWrapper[^"]*"[^>]*>(.*?)</div>',
            raw, re.DOTALL
        )
        if not m:
            # fallback: any <article> or .post-content
            m = re.search(r'<article[^>]*>(.*?)</article>', raw, re.DOTALL)
        if not m:
            return ""

        text = re.sub(r'<[^>]+>', ' ', m.group(1))
        text = _h.unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()

        lines = []
        for line in text.split('. '):
            lines.append(line.strip())
            if len(lines) >= max_lines:
                break
        return ". ".join(lines)[:2000]
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Extract Phora threads from TTL")
    parser.add_argument("--ttl",      default=str(DEFAULT_TTL))
    parser.add_argument("--out-dir",  default=str(OUT_DIR))
    parser.add_argument("--index",    default=str(INDEX_OUT))
    parser.add_argument("--fetch",    action="store_true",
                        help="Fetch OP text from live Phora URLs (slow, ~1s/thread)")
    parser.add_argument("--limit", "-n", type=int, default=0)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Parsing TTL: {args.ttl}", file=sys.stderr)
    threads = parse_ttl(args.ttl)
    print(f"  {len(threads)} threads found", file=sys.stderr)

    tids = sorted(threads.keys(), key=int)
    if args.limit:
        tids = tids[:args.limit]

    print(f"Writing {len(tids)} YAMLs{' (with live OP fetch)' if args.fetch else ''}...",
          file=sys.stderr)

    entries = []
    fetch_count = 0

    for i, tid in enumerate(tids):
        meta = threads[tid]
        op = ""

        if args.fetch and meta["url"]:
            import time
            op = fetch_op(meta["url"])
            if op:
                fetch_count += 1
            time.sleep(0.5)

        rel_path = f"data/phora-threads/thread-{tid}.yaml"
        doc = {
            "threadid":        tid,
            "title":           meta["title"],
            "url":             meta["url"],
            "creator":         meta["creator"],
            "comment_count":   meta["comment_count"],
            "forum_category":  meta["forum_category"],
            "mec_categories":  meta["mec_categories"],
            "entities_person": meta["entities_person"],
            "entities_org":    meta["entities_org"],
            "entities_place":  meta["entities_place"],
            "source":          "phora",
            "op":              op,
        }

        with open(REPO_ROOT / rel_path, "w", encoding="utf-8") as f:
            yaml.dump(doc, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

        entries.append({"path": rel_path, "title": meta["title"]})

        if (i + 1) % 100 == 0 or (i + 1) == len(tids):
            print(f"  {i+1}/{len(tids)}", file=sys.stderr)

    # Build index
    index = {
        "name": "phora",
        "source": "phora-nova",
        "sections": [{
            "name":     "phora-all",
            "gln":      "",
            "fgid":     [],
            "files":    [{"path": e["path"], "title": e["title"]} for e in entries],
            "children": [],
        }],
    }
    with open(args.index, "w", encoding="utf-8") as f:
        yaml.dump(index, f, allow_unicode=True, default_flow_style=False)

    print(f"Done: {len(entries)} threads, {fetch_count} with OP text", file=sys.stderr)
    print(f"Index: {args.index}", file=sys.stderr)


if __name__ == "__main__":
    main()
