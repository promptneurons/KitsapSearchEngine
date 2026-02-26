#!/usr/bin/env python3
"""
gln-precompute.py - Build GLN cache for all indexed documents

Runs gln-resolver scoring against every file in llms-N200-index.yaml
and writes a JSONL cache at data/gln-cache.jsonl. Each line contains
the resolver output + an extracted snippet for search-engine display.

Usage:
  python3 scripts/gln-precompute.py
  python3 scripts/gln-precompute.py --output data/gln-cache.jsonl
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

# Add scripts/ to path so we can import gln-resolver
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

# Import from gln-resolver (the module defines everything at top level)
import importlib.util
spec = importlib.util.spec_from_file_location("gln_resolver", SCRIPT_DIR / "gln-resolver.py")
gln_resolver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gln_resolver)

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required. Install with: pip3 install pyyaml", file=sys.stderr)
    sys.exit(1)

REPO_ROOT = SCRIPT_DIR.parent
INDEX_PATH = REPO_ROOT / "llms-N200-index.yaml"
DEFAULT_OUTPUT = REPO_ROOT / "data" / "gln-cache.jsonl"
SNIPPET_MAX_CHARS = 200
KEYWORD_TOP_N = 20

# Common English stopwords (no external deps)
_STOPWORDS = set("""
a an the and or but in on at to of for with by from as is was are were be been
being have has had do does did will would could should may might shall can its
it this that these those i me my we our you your he she his her they them their
not no nor so if then when where what who how all any each some such only just
also very more most over about up out into after before while during between
""".split())


def extract_keywords(text, top_n=KEYWORD_TOP_N):
    """Extract top-N content keywords from text (stopword-filtered, lowercased)."""
    import re
    if not text:
        return []
    words = re.findall(r"[a-z]{3,}", text.lower())
    freq = {}
    for w in words:
        if w not in _STOPWORDS:
            freq[w] = freq.get(w, 0) + 1
    return [w for w, _ in sorted(freq.items(), key=lambda x: -x[1])[:top_n]]



def extract_snippet(file_path, max_chars=SNIPPET_MAX_CHARS):
    """Extract the first non-heading, non-frontmatter paragraph from a markdown file."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except (OSError, IOError):
        return ""

    in_frontmatter = False
    snippet_lines = []
    found_content = False

    for line in lines:
        stripped = line.strip()

        # Skip YAML frontmatter
        if stripped == "---":
            in_frontmatter = not in_frontmatter
            continue
        if in_frontmatter:
            continue

        # Skip headings
        if stripped.startswith("#"):
            if found_content:
                break  # stop at next heading after we found content
            continue

        # Skip empty lines before content
        if not stripped:
            if found_content:
                break  # paragraph break = end of snippet
            continue

        # Skip markdown artifacts
        if stripped.startswith(("```", "---", "| ", "- [", "> **")):
            if found_content:
                break
            continue

        # This is content
        found_content = True
        snippet_lines.append(stripped)

        # Check length
        if sum(len(s) for s in snippet_lines) >= max_chars:
            break

    snippet = " ".join(snippet_lines)
    if len(snippet) > max_chars:
        snippet = snippet[:max_chars].rsplit(" ", 1)[0] + "..."
    return snippet


def collect_all_files(yaml_path):
    """Walk the index and collect all file entries with their GLN context."""
    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    files = []

    def walk(sections):
        for s in sections:
            gln = s.get("gln", "")
            name = s.get("name", "")
            fgid = s.get("fgid", [])
            if isinstance(fgid, str):
                fgid = [fgid]
            if not isinstance(fgid, list):
                fgid = []

            for f_entry in s.get("files", []):
                files.append({
                    "path": f_entry.get("path", ""),
                    "title": f_entry.get("title", ""),
                    "section_gln": gln,
                    "section_name": name,
                    "section_fgid": fgid,
                })

            walk(s.get("children", []))

    walk(data.get("sections", []))
    return files


def main():
    parser = argparse.ArgumentParser(
        description="GLN Precompute - Build cache for all indexed documents",
    )
    parser.add_argument(
        "--output", "-o",
        default=str(DEFAULT_OUTPUT),
        help=f"Output JSONL path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--index", "-i",
        default=str(INDEX_PATH),
        help=f"Path to llms-N200-index.yaml (default: {INDEX_PATH})",
    )
    parser.add_argument(
        "--limit", "-n",
        type=int, default=0,
        help="Limit to first N files (0 = all, useful for testing)",
    )

    args = parser.parse_args()
    index_path = Path(args.index)
    output_path = Path(args.output)

    if not index_path.exists():
        print(f"ERROR: Index file not found: {index_path}", file=sys.stderr)
        sys.exit(1)

    # Load GLN tree (shared by all resolver calls)
    print(f"Loading GLN tree from {index_path}...", file=sys.stderr)
    nodes = gln_resolver.load_gln_tree(str(index_path))
    print(f"Loaded {len(nodes)} GLN nodes", file=sys.stderr)

    # Collect all files
    all_files = collect_all_files(str(index_path))
    if args.limit > 0:
        all_files = all_files[:args.limit]
    print(f"Processing {len(all_files)} files...", file=sys.stderr)

    # Process each file
    results = []
    for i, entry in enumerate(all_files):
        path = entry["path"]
        title = entry["title"]
        full_path = REPO_ROOT / path

        # Read content
        content = ""
        if full_path.exists():
            content = gln_resolver.read_file_content(str(full_path))

        # Run resolver
        result = gln_resolver.resolve_gln(title, content, path, nodes)

        # Extract snippet
        snippet = ""
        if full_path.exists():
            snippet = extract_snippet(str(full_path))

        # Build cache entry
        pred = result.get("prediction", {})
        axes = result.get("axes", {})
        candidates = result.get("candidates", [])

        cache_entry = {
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
            "snippet": snippet,
            "keywords": extract_keywords(content),
        }

        results.append(cache_entry)

        if (i + 1) % 50 == 0:
            print(f"  {i + 1}/{len(all_files)} processed...", file=sys.stderr)

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for entry in results:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print(f"Wrote {len(results)} entries to {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
