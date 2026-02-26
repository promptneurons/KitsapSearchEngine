#!/usr/bin/env python3
"""
llms-txt-extract.py - Extract llms.txt corpus to per-domain YAML + index

Each domain dir under scrape2/ contains llms.txt (compact) and optionally
llms-full.txt. We extract from llms.txt only: product name, description,
section names, link count — enough for CEC + keyword + SUMO classification.

Input:  <scrape-dir>/   (e.g. kitsaplabs.com:public_html/tools/scrape2/)
Output: data/llms-txt/  — one YAML per domain
        data/llms-txt-index.yaml

Usage:
  # Run after rsyncing scrape2/ to BLUE:
  rsync -av --include='*/' --include='llms.txt' --exclude='*' \\
      kitsaplabs@kitsaplabs.com:public_html/tools/scrape2/ \\
      /home/john/llms-txt-scrape/

  python3 scripts/llms-txt-extract.py \\
      --scrape-dir /home/john/llms-txt-scrape \\
      [--limit 50]
"""

import argparse
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required.", file=sys.stderr)
    sys.exit(1)

REPO_ROOT   = Path(__file__).resolve().parent.parent
OUT_DIR     = REPO_ROOT / "data" / "llms-txt"
INDEX_OUT   = REPO_ROOT / "data" / "llms-txt-index.yaml"
OP_MAX_LINES = 80


# ─────────────────────────────────────────────────────────────────────────────
# Parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_llms_txt(text, max_op_lines=OP_MAX_LINES):
    """
    Parse llms.txt markdown into structured fields.

    Returns {
      title, description, sections: [{name, links:[{title,url,desc}]}],
      op (first max_op_lines lines of meaningful content),
      link_count
    }
    """
    lines = text.splitlines()

    title       = ""
    description = []
    sections    = []
    cur_section = None
    op_lines    = []
    in_header   = True   # still in pre-section intro

    for line in lines:
        stripped = line.strip()

        # H1 = product/service title
        if stripped.startswith("# ") and not title:
            title = stripped[2:].strip()
            op_lines.append(stripped)
            continue

        # H2 = section break
        if stripped.startswith("## "):
            in_header = False
            sec_name  = stripped[3:].strip()
            cur_section = {"name": sec_name, "links": []}
            sections.append(cur_section)
            if len(op_lines) < max_op_lines:
                op_lines.append(stripped)
            continue

        # Link line: - [Title](url): desc  or  - [Title](url)
        if stripped.startswith("- [") and cur_section is not None:
            m = re.match(
                r'- \[([^\]]+)\]\(([^)]+)\)(?::\s*(.*))?',
                stripped
            )
            if m:
                link = {
                    "title": m.group(1).strip(),
                    "url":   m.group(2).strip(),
                    "desc":  (m.group(3) or "").strip(),
                }
                cur_section["links"].append(link)
            if len(op_lines) < max_op_lines:
                op_lines.append(stripped)
            continue

        # Description text (pre-first-section)
        if in_header and stripped and not stripped.startswith("#"):
            description.append(stripped)
            if len(op_lines) < max_op_lines:
                op_lines.append(stripped)
            continue

        if len(op_lines) < max_op_lines and stripped:
            op_lines.append(stripped)

    link_count = sum(len(s["links"]) for s in sections)

    return {
        "title":       title or "",
        "description": " ".join(description),
        "sections":    [s["name"] for s in sections],
        "link_count":  link_count,
        "op":          "\n".join(op_lines[:max_op_lines]),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Extract llms.txt corpus to YAML")
    parser.add_argument("--scrape-dir", required=True,
                        help="Path to rsynced scrape2/ directory")
    parser.add_argument("--out-dir",  default=str(OUT_DIR))
    parser.add_argument("--index",    default=str(INDEX_OUT))
    parser.add_argument("--limit", "-n", type=int, default=0)
    parser.add_argument("--prefer-full", action="store_true",
                        help="Use llms-full.txt when present (slower, larger OP)")
    args = parser.parse_args()

    scrape = Path(args.scrape_dir)
    out    = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Collect all domain dirs
    domains = sorted(d for d in scrape.iterdir() if d.is_dir())
    if args.limit:
        domains = domains[:args.limit]

    print(f"Extracting {len(domains)} domains from {scrape}", file=sys.stderr)

    entries      = []
    skipped      = 0
    link_total   = 0

    for i, domain_dir in enumerate(domains):
        domain = domain_dir.name

        # Prefer llms-full.txt if requested and present, else llms.txt
        candidates = (["llms-full.txt", "llms.txt"] if args.prefer_full
                      else ["llms.txt", "llms-full.txt"])
        txt_file = None
        for cand in candidates:
            p = domain_dir / cand
            if p.exists():
                txt_file = p
                break

        if txt_file is None:
            skipped += 1
            continue

        try:
            text = txt_file.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"  WARN: {domain}: {e}", file=sys.stderr)
            skipped += 1
            continue

        parsed = parse_llms_txt(text)

        # Derive URL from domain dir name
        domain_clean = domain.lstrip("www.")
        url = f"https://{domain}"

        rel_path = f"data/llms-txt/{domain}.yaml"

        doc = {
            "domain":      domain,
            "title":       parsed["title"] or domain_clean,
            "url":         url,
            "source":      "llms-txt",
            "sections":    parsed["sections"],
            "link_count":  parsed["link_count"],
            "description": parsed["description"],
            "op":          parsed["op"],
        }

        with open(REPO_ROOT / rel_path, "w", encoding="utf-8") as f:
            yaml.dump(doc, f, allow_unicode=True,
                      default_flow_style=False, sort_keys=False)

        entries.append({
            "path":  rel_path,
            "title": doc["title"],
        })
        link_total += parsed["link_count"]

        if (i + 1) % 200 == 0 or (i + 1) == len(domains):
            print(f"  {i+1}/{len(domains)}", file=sys.stderr)

    # Build index — group by first letter of domain for navigability
    from collections import defaultdict
    by_alpha = defaultdict(list)
    for e in entries:
        key = e["path"].split("/")[-1][0].upper()
        by_alpha[key].append({"path": e["path"], "title": e["title"]})

    sections = []
    for letter in sorted(by_alpha):
        sections.append({
            "name":     f"llms-txt-{letter}",
            "gln":      "",
            "fgid":     [],
            "files":    by_alpha[letter],
            "children": [],
        })

    index = {
        "name":     "llms-txt",
        "source":   "llms-txt-scrape",
        "sections": sections,
    }
    with open(args.index, "w", encoding="utf-8") as f:
        yaml.dump(index, f, allow_unicode=True,
                  default_flow_style=False)

    print(f"\nDone: {len(entries)} extracted, {skipped} skipped", file=sys.stderr)
    print(f"Total links indexed: {link_total:,}", file=sys.stderr)
    print(f"Index: {args.index}", file=sys.stderr)


if __name__ == "__main__":
    main()
