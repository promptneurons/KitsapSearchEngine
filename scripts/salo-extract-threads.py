#!/usr/bin/env python3
"""
salo-extract-threads.py - Extract Salo Archive threads → per-YAML files

Parses XenForo-style archive HTML (salo-macrobius/) and produces:
  - data/salo-threads/{thread-id}.yaml  (one per thread, 80-line OP cap)
  - data/salo-thread-index.yaml          (index for gln-precompute)

Usage:
  python3 scripts/salo-extract-threads.py
  python3 scripts/salo-extract-threads.py --limit 10
"""

import argparse
import html as _html
import os
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required.", file=sys.stderr)
    sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parent.parent
SALO_DIR  = Path("/home/john/salo-macrobius")
OUT_DIR   = REPO_ROOT / "data" / "salo-threads"
INDEX_OUT = REPO_ROOT / "data" / "salo-thread-index.yaml"
OP_MAX_LINES = 80


# Void elements that are self-closing (no end tag in HTML)
_VOID_TAGS = frozenset(["area","base","br","col","embed","hr","img","input",
                        "link","meta","param","source","track","wbr"])

# ---------------------------------------------------------------------------
# HTML parser for XenForo archive pages
# ---------------------------------------------------------------------------

class SaloParser(HTMLParser):
    """
    State machine parser for Salo archive XenForo HTML.

    Tracks nesting depth carefully:
    - void tags (br, hr, img...) never increment depth
    - </a> and </time> are handled independently from body depth
    """

    def __init__(self):
        super().__init__()
        self.title      = ""
        self.post_count = 0
        self.messages   = []   # list of (author, date, text)

        self._in_h1         = False
        self._in_h2         = False
        self._in_username   = False
        self._in_timestamp  = False
        self._in_body       = False
        self._body_depth    = 0
        self._body_buf      = []

        self._cur_author = ""
        self._cur_date   = ""

    def handle_starttag(self, tag, attrs):
        adict = dict(attrs)
        cls   = adict.get("class", "")

        if tag == "h1":
            self._in_h1 = True
        elif tag == "h2":
            self._in_h2 = True
        elif not self._in_body and tag == "a" and cls == "username":
            self._in_username = True
        elif not self._in_body and tag == "time" and "u-dt" in cls:
            self._in_timestamp = True
        elif tag == "div" and "messageTextEndMarker" in cls:
            # Sentinel: end of message body content
            if self._in_body:
                self._finish_message()
        elif tag == "article" and "message-body" in cls:
            self._in_body   = True
            self._body_depth = 1
            self._body_buf  = []
        elif self._in_body and tag not in _VOID_TAGS:
            self._body_depth += 1
            if tag == "br":   # shouldn't reach here but guard anyway
                self._body_buf.append("\n")

    def handle_endtag(self, tag):
        if tag == "h1":
            self._in_h1 = False
        elif tag == "h2":
            self._in_h2 = False
        elif not self._in_body and tag == "a":
            self._in_username = False
        elif not self._in_body and tag == "time":
            self._in_timestamp = False
        elif self._in_body:
            if tag not in _VOID_TAGS:
                self._body_depth -= 1
            if self._body_depth <= 0:
                self._finish_message()

    def _finish_message(self):
        self._in_body = False
        self._body_depth = 0
        text = "".join(self._body_buf).strip()
        self.messages.append((self._cur_author, self._cur_date, text))

    def handle_data(self, data):
        if self._in_h1:
            self.title += data
        elif self._in_h2:
            m = re.search(r"(\d+)\s+post", data)
            if m:
                self.post_count = int(m.group(1))
        elif self._in_username:
            self._cur_author = data.strip()
        elif self._in_timestamp:
            self._cur_date = data.strip()
        elif self._in_body:
            self._body_buf.append(data)


def parse_html(path):
    """Parse a Salo archive HTML file → dict."""
    text = Path(path).read_text(encoding="utf-8", errors="replace")
    parser = SaloParser()
    parser.feed(text)

    title = _html.unescape(parser.title.replace("- Salo Archive", "").strip())

    author, date, op_raw = "", "", ""
    if parser.messages:
        author, date, op_raw = parser.messages[0]

    # Clean OP: strip URLs, collapse blank lines, cap at OP_MAX_LINES
    op_lines_raw = op_raw.splitlines()
    op_lines = []
    for line in op_lines_raw:
        line = line.strip()
        # Skip bare URL lines
        if re.match(r"^https?://\S+$", line):
            continue
        op_lines.append(line)
    # Collapse consecutive blank lines to one
    cleaned = []
    prev_blank = False
    for line in op_lines:
        if not line:
            if not prev_blank:
                cleaned.append("")
            prev_blank = True
        else:
            cleaned.append(line)
            prev_blank = False
    op = "\n".join(cleaned[:OP_MAX_LINES]).strip()

    return {
        "title":      title,
        "author":     author,
        "date":       date,
        "post_count": parser.post_count,
        "op":         op,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Extract Salo archive threads to YAML")
    parser.add_argument("--salo-dir", default=str(SALO_DIR))
    parser.add_argument("--out-dir",  default=str(OUT_DIR))
    parser.add_argument("--index",    default=str(INDEX_OUT))
    parser.add_argument("--limit", "-n", type=int, default=0)
    args = parser.parse_args()

    salo_dir = Path(args.salo_dir)
    out_dir  = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Only process first-page files
    html_files = sorted(salo_dir.glob("*_p001_*.html"))
    if args.limit:
        html_files = html_files[:args.limit]

    print(f"Processing {len(html_files)} Salo threads...", file=sys.stderr)

    entries = []
    for i, html_path in enumerate(html_files):
        # Extract thread_id from filename: NNNNNN_slug_p001_o.html
        stem = html_path.stem   # e.g. 000192_whitespace-reopens..._p001_o
        parts = stem.split("_")
        thread_id = parts[0]
        slug      = "_".join(parts[1:-2]) if len(parts) > 3 else parts[1]

        try:
            data = parse_html(html_path)
        except Exception as e:
            print(f"  WARN: {html_path.name}: {e}", file=sys.stderr)
            continue

        rel_path = f"data/salo-threads/thread-{thread_id}.yaml"
        out_path = REPO_ROOT / rel_path

        doc = {
            "threadid":   thread_id,
            "slug":       slug,
            "title":      data["title"],
            "author":     data["author"],
            "date":       data["date"],
            "post_count": data["post_count"],
            "source":     "salo-macrobius",
            "op":         data["op"],
        }

        with open(out_path, "w", encoding="utf-8") as f:
            yaml.dump(doc, f, allow_unicode=True, default_flow_style=False,
                      sort_keys=False)

        entries.append({
            "path":  rel_path,
            "title": data["title"],
            "date":  data["date"],
        })

        if (i + 1) % 50 == 0 or (i + 1) == len(html_files):
            print(f"  {i+1}/{len(html_files)}", file=sys.stderr)

    # Build index (single section — Salo doesn't have a useful year breakdown yet)
    index = {
        "name": "salo-macrobius",
        "source": "salo-archive",
        "sections": [{
            "name": "salo-all",
            "gln":  "",
            "fgid": [],
            "files": [{"path": e["path"], "title": e["title"]} for e in entries],
            "children": [],
        }],
    }
    with open(args.index, "w", encoding="utf-8") as f:
        yaml.dump(index, f, allow_unicode=True, default_flow_style=False)

    print(f"Done: {len(entries)} threads → {out_dir}", file=sys.stderr)
    print(f"Index: {args.index}", file=sys.stderr)


if __name__ == "__main__":
    main()
