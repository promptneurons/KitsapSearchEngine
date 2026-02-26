#!/usr/bin/env python3
"""
salo-extract-ttl.py - Build Salo thread YAMLs from salo-threads.ttl

Uses the pre-existing TTL metadata (MEC categories, NER entities) as
the primary source, merging in OP text from local HTML files where available.

Sources:
  /home/john/salo-macrobius/salo-threads.ttl   — 5,292 threads
  /home/john/salo-macrobius/*_p001_*.html       — 221 local OP files

Output:
  data/salo-threads/thread-NNNNNN.yaml          — one per thread
  data/salo-thread-index.yaml                   — index for gln-precompute

Usage:
  python3 scripts/salo-extract-ttl.py
  python3 scripts/salo-extract-ttl.py --limit 20
"""

import argparse
import html as _html
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required.", file=sys.stderr)
    sys.exit(1)

REPO_ROOT  = Path(__file__).resolve().parent.parent
SALO_DIR   = Path("/home/john/salo-macrobius")
TTL_PATH   = SALO_DIR / "salo-threads.ttl"
OUT_DIR    = REPO_ROOT / "data" / "salo-threads"
INDEX_OUT  = REPO_ROOT / "data" / "salo-thread-index.yaml"
OP_MAX_LINES = 80

# Void tags for HTML parser
_VOID_TAGS = frozenset(["area","base","br","col","embed","hr","img","input",
                        "link","meta","param","source","track","wbr"])


# ─────────────────────────────────────────────────────────────────────────────
# TTL parser (stdlib only — no rdflib)
# ─────────────────────────────────────────────────────────────────────────────

def _unquote(s):
    """Strip surrounding quotes and unescape \\n, \\'."""
    s = s.strip()
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return s.replace('\\"', '"').replace("\\n", "\n").strip()


def parse_ttl(path):
    """
    Parse salo-threads.ttl → dict: threadId → metadata dict.

    Only processes salo:Thread subjects (skips ontology declarations).
    Returns {
      "000001": {
        "title": "...",
        "url": "...",
        "mec_categories": ["politics", "spengler", ...],
        "entities_person": [...],
        "entities_org": [...],
        "entities_place": [...],
      }, ...
    }
    """
    threads = {}
    current_id = None
    current = None

    # Regex patterns
    re_thread = re.compile(r'^salo:thread-(\d+)\s')
    re_title  = re.compile(r'dct:title\s+"([^"]*)"')
    re_label  = re.compile(r'rdfs:label\s+"([^"]*)"')
    re_url    = re.compile(r'schema:url\s+<([^>]+)>')
    re_mec    = re.compile(r'salo:mecCategory\s+mec:(\S+?)[\s;.]')
    re_person = re.compile(r'salo:mentionsPerson\s+"([^"]+)"')
    re_org    = re.compile(r'salo:mentionsOrg\s+"([^"]+)"')
    re_place  = re.compile(r'salo:mentionsPlace\s+"([^"]+)"')

    text = Path(path).read_text(encoding="utf-8", errors="replace")

    # Split on thread subject lines — each block ends at the next subject or EOF
    blocks = re.split(r'\n(?=salo:thread-)', text)

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

        mec_cats   = re_mec.findall(block)
        persons    = re_person.findall(block)
        orgs       = re_org.findall(block)
        places     = re_place.findall(block)

        threads[tid] = {
            "title":           title,
            "url":             url,
            "mec_categories":  mec_cats,
            "entities_person": persons,
            "entities_org":    orgs,
            "entities_place":  places,
        }

    return threads


# ─────────────────────────────────────────────────────────────────────────────
# HTML OP extractor (same as before, with void-tag-aware depth)
# ─────────────────────────────────────────────────────────────────────────────

class SaloHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.author = ""
        self.date   = ""
        self.messages = []
        self._in_h1 = self._in_h2 = self._in_username = self._in_timestamp = self._in_body = False
        self._body_depth = 0
        self._body_buf = []
        self._cur_author = self._cur_date = ""

    def handle_starttag(self, tag, attrs):
        adict = dict(attrs)
        cls   = adict.get("class", "")
        if tag == "h1": self._in_h1 = True
        elif not self._in_body and tag == "a" and cls == "username": self._in_username = True
        elif not self._in_body and tag == "time" and "u-dt" in cls:  self._in_timestamp = True
        elif tag == "div" and "messageTextEndMarker" in cls:
            if self._in_body: self._finish()
        elif tag == "article" and "message-body" in cls:
            self._in_body = True; self._body_depth = 1; self._body_buf = []
        elif self._in_body and tag not in _VOID_TAGS:
            self._body_depth += 1

    def handle_endtag(self, tag):
        if tag == "h1": self._in_h1 = False
        elif not self._in_body and tag == "a":    self._in_username  = False
        elif not self._in_body and tag == "time": self._in_timestamp = False
        elif self._in_body:
            if tag not in _VOID_TAGS: self._body_depth -= 1
            if self._body_depth <= 0: self._finish()

    def _finish(self):
        self._in_body = False; self._body_depth = 0
        text = "".join(self._body_buf).strip()
        self.messages.append((self._cur_author, self._cur_date, text))
        if not self.author: self.author = self._cur_author
        if not self.date:   self.date   = self._cur_date

    def handle_data(self, data):
        if self._in_h1: pass  # title comes from TTL
        elif self._in_username:  self._cur_author = data.strip()
        elif self._in_timestamp: self._cur_date   = data.strip()
        elif self._in_body:      self._body_buf.append(data)


def extract_op(html_path):
    """Return (author, date, op_text) from a Salo HTML file."""
    text = Path(html_path).read_text(encoding="utf-8", errors="replace")
    p = SaloHTMLParser()
    p.feed(text)
    author, date, op_raw = (p.author, p.date, "")
    if p.messages:
        _, _, op_raw = p.messages[0]
    # Clean: strip bare URLs, collapse blank lines
    lines = []
    for line in op_raw.splitlines():
        line = line.strip()
        if re.match(r"^https?://\S+$", line):
            continue
        lines.append(line)
    cleaned = []
    prev_blank = False
    for line in lines:
        if not line:
            if not prev_blank: cleaned.append("")
            prev_blank = True
        else:
            cleaned.append(line); prev_blank = False
    return author, date, "\n".join(cleaned[:OP_MAX_LINES]).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Build local HTML index: threadId → html_path
# ─────────────────────────────────────────────────────────────────────────────

def build_html_index(salo_dir):
    html_index = {}
    for path in sorted(Path(salo_dir).glob("*_p001_*.html")):
        tid = path.stem.split("_")[0]
        html_index[tid] = str(path)
    return html_index


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Extract Salo threads from TTL + HTML")
    parser.add_argument("--ttl",      default=str(TTL_PATH))
    parser.add_argument("--salo-dir", default=str(SALO_DIR))
    parser.add_argument("--out-dir",  default=str(OUT_DIR))
    parser.add_argument("--index",    default=str(INDEX_OUT))
    parser.add_argument("--limit", "-n", type=int, default=0)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Parsing TTL: {args.ttl}", file=sys.stderr)
    threads = parse_ttl(args.ttl)
    print(f"  {len(threads)} threads found", file=sys.stderr)

    html_index = build_html_index(args.salo_dir)
    print(f"  {len(html_index)} HTML files available locally", file=sys.stderr)

    tids = sorted(threads.keys())
    if args.limit:
        tids = tids[:args.limit]

    print(f"Writing {len(tids)} YAMLs...", file=sys.stderr)
    entries = []
    op_count = 0

    for i, tid in enumerate(tids):
        meta = threads[tid]
        author = date = op = ""

        if tid in html_index:
            try:
                author, date, op = extract_op(html_index[tid])
                op_count += 1
            except Exception as e:
                print(f"  WARN {tid}: {e}", file=sys.stderr)

        rel_path = f"data/salo-threads/thread-{tid}.yaml"
        doc = {
            "threadid":        tid,
            "title":           meta["title"],
            "url":             meta["url"],
            "author":          author,
            "date":            date,
            "mec_categories":  meta["mec_categories"],
            "entities_person": meta["entities_person"],
            "entities_org":    meta["entities_org"],
            "entities_place":  meta["entities_place"],
            "source":          "salo",
            "op":              op,
        }

        with open(REPO_ROOT / rel_path, "w", encoding="utf-8") as f:
            yaml.dump(doc, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

        entries.append({"path": rel_path, "title": meta["title"]})

        if (i + 1) % 500 == 0 or (i + 1) == len(tids):
            print(f"  {i+1}/{len(tids)}  (OP text: {op_count})", file=sys.stderr)

    # Build index grouped by MEC top categories
    index = {
        "name": "salo",
        "source": "salo-archive",
        "sections": [{
            "name":     "salo-all",
            "gln":      "",
            "fgid":     [],
            "files":    [{"path": e["path"], "title": e["title"]} for e in entries],
            "children": [],
        }],
    }
    with open(args.index, "w", encoding="utf-8") as f:
        yaml.dump(index, f, allow_unicode=True, default_flow_style=False)

    print(f"Done: {len(entries)} threads, {op_count} with OP text", file=sys.stderr)
    print(f"Index: {args.index}", file=sys.stderr)


if __name__ == "__main__":
    main()
