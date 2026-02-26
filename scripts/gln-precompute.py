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

# Import SUMO/WordNet bridge (optional — only used when --wordnet-path is set)
try:
    import importlib.util as _ilu
    _sw_spec = _ilu.spec_from_file_location("sumo_wordnet", SCRIPT_DIR / "sumo_wordnet.py")
    sumo_wordnet = _ilu.module_from_spec(_sw_spec)
    _sw_spec.loader.exec_module(sumo_wordnet)
    _SUMO_AVAILABLE = True
except Exception as _e:
    sumo_wordnet = None
    _SUMO_AVAILABLE = False

# Import language detector (stdlib-only, always available)
try:
    import importlib.util as _ilu2
    _ld_spec = _ilu2.spec_from_file_location("lang_detect", SCRIPT_DIR / "lang_detect.py")
    lang_detect = _ilu2.module_from_spec(_ld_spec)
    _ld_spec.loader.exec_module(lang_detect)
    _LANG_DETECT_AVAILABLE = True
except Exception:
    lang_detect = None
    _LANG_DETECT_AVAILABLE = False

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required. Install with: pip3 install pyyaml", file=sys.stderr)
    sys.exit(1)

REPO_ROOT = SCRIPT_DIR.parent
INDEX_PATH = REPO_ROOT / "llms-N200-index.yaml"
DEFAULT_OUTPUT = REPO_ROOT / "data" / "gln-cache.jsonl"
DEFAULT_WORDNET = REPO_ROOT / "data" / "wordnet-mappings"
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
    """Extract readable snippet from a file. Handles YAML (op: field) and markdown."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            raw = f.read()
    except (OSError, IOError):
        return ""

    # YAML file: extract op: field
    if file_path.endswith(".yaml") or file_path.endswith(".yml"):
        try:
            doc = yaml.safe_load(raw)
            if isinstance(doc, dict) and doc.get("op"):
                text = str(doc["op"]).replace("\n", " ").strip()
                if len(text) > max_chars:
                    text = text[:max_chars].rsplit(" ", 1)[0] + "..."
                return text
        except Exception:
            pass
        # Fallback: first substantial line
        for line in raw.splitlines():
            line = line.strip()
            if len(line) > 40 and not line.endswith(":") and "://" not in line:
                return line[:max_chars]
        return ""

    lines = raw.splitlines(keepends=True)

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
    parser.add_argument(
        "--wordnet-path", "-w",
        default=str(DEFAULT_WORDNET),
        help=f"Path to WordNetMappings directory for SUMO expansion (default: {DEFAULT_WORDNET}). Pass 'none' to disable.",
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

    # Load SUMO/WordNet mapping (optional)
    sumo_index = {}
    sumo_map = {}
    wordnet_path_str = args.wordnet_path.strip()
    if wordnet_path_str.lower() != "none" and _SUMO_AVAILABLE:
        wn_path = Path(wordnet_path_str)
        if wn_path.exists():
            import time as _time
            _t0 = _time.time()
            print(f"Loading SUMO/WordNet db from {wn_path}...", file=sys.stderr)
            sumo_index, sumo_map = sumo_wordnet.load_sumo_db(wn_path)
            print(f"  {len(sumo_index):,} lemmas, {len(sumo_map):,} synsets  ({_time.time()-_t0:.1f}s)", file=sys.stderr)
        else:
            print(f"WARNING: WordNet path not found: {wn_path} (skipping SUMO expansion)", file=sys.stderr)
    elif not _SUMO_AVAILABLE:
        print("WARNING: sumo_wordnet.py not loadable — SUMO expansion disabled", file=sys.stderr)

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
        # Fallback: index may reference paths outside REPO_ROOT (e.g. --out-dir to openplanter)
        if not full_path.exists():
            alt = index_path.parent.parent / path
            if alt.exists():
                full_path = alt

        # Read content + obsidian metadata (dir_path, sprint, quarter, links)
        content = ""
        doc_meta = {}
        if full_path.exists():
            content = gln_resolver.read_file_content(str(full_path))
            try:
                doc_meta = yaml.safe_load(
                    full_path.read_text(encoding="utf-8", errors="replace")
                ) or {}
                if not isinstance(doc_meta, dict):
                    doc_meta = {}
            except Exception:
                doc_meta = {}

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

        kw = extract_keywords(content)

        # Language detection — gate SUMO on English only
        detected_lang = "unknown"
        if _LANG_DETECT_AVAILABLE and content:
            detected_lang, _conf = lang_detect.detect_language(content)

        sumo_concepts = (
            sorted(sumo_wordnet.words_to_sumo(kw, sumo_index, sumo_map))
            if sumo_index and detected_lang in ("en", "unknown") else []
        )

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
            "keywords": kw,
            "sumo_concepts": sumo_concepts,
            "lang": detected_lang,
            "dir_path": doc_meta.get("dir_path", ""),
            "sprint": doc_meta.get("sprint"),
            "lang": doc_meta.get("lang"),      # ISO 639-1 (en/de/fr/ja...)
            "region": doc_meta.get("region"),  # ISO 3166-1 alpha-2 (US/DE/GB...)
            "quarter": doc_meta.get("quarter"),
            "links": doc_meta.get("links", []),
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
