#!/usr/bin/env python3
"""
gln-ranker.py - GLN Relevance Ranker

Ranks documents by relevance to a query document using precomputed
GLN resolver vectors as a 4-axis metric space:

  1. GLN proximity  (40%) — shared prefix in GLN tree
  2. FGID Jaccard   (25%) — department code overlap
  3. CEC match      (20%) — subject classification match
  4. Candidate overlap (15%) — shared candidate GLN sets

Usage:
  # Random mode: pick 11 docs, rank 10 against the first
  python3 scripts/gln-ranker.py

  # Specific query
  python3 scripts/gln-ranker.py --query projects/BUSINESS-PLAN.md

  # Reproducible
  python3 scripts/gln-ranker.py --seed 42

  # Custom pool size
  python3 scripts/gln-ranker.py --pool 20
"""

import argparse
import json
import os
import random
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_CACHE = REPO_ROOT / "data" / "gln-cache.jsonl"
DEFAULT_WORDNET = REPO_ROOT / "data" / "wordnet-mappings"

# SUMO/WordNet bridge (loaded lazily in main() when W_SUMO > 0)
try:
    import importlib.util as _ilu
    _sw_spec = _ilu.spec_from_file_location("sumo_wordnet", SCRIPT_DIR / "sumo_wordnet.py")
    _sumo_wordnet = _ilu.module_from_spec(_sw_spec)
    _sw_spec.loader.exec_module(_sumo_wordnet)
    _SUMO_AVAILABLE = True
except Exception:
    _sumo_wordnet = None
    _SUMO_AVAILABLE = False

# Global SUMO state — populated in main() when active profile includes SUMO weight
_SUMO_INDEX    = {}   # word → set[offset]
_SUMO_MAPPINGS = {}   # offset → SUMO concept

# Distance function weight profiles
PROFILES = {
    "internal": {  # clawd/N200 private documents — GLN tree is meaningful
        "gln": 0.40, "fgid": 0.25, "cec": 0.20, "candidates": 0.15, "keywords": 0.00, "sumo": 0.00,
    },
    "external": {  # customer/third-party data — CEC + keyword surface match, no SUMO
        "gln": 0.00, "fgid": 0.00, "cec": 0.70, "candidates": 0.00, "keywords": 0.30, "sumo": 0.00,
    },
    "external_sumo": {  # external data + SUMO semantic expansion (requires WordNet)
        "gln": 0.00, "fgid": 0.00, "cec": 0.50, "candidates": 0.00, "keywords": 0.20, "sumo": 0.30,
    },
}

# Active weights (overridden by --profile at startup)
W_GLN = 0.40
W_FGID = 0.25
W_CEC = 0.20
W_CANDIDATES = 0.15
W_KEYWORDS = 0.00
W_SUMO = 0.00

# CEC → JDN crosswalk (subset for match scoring)
CEC_TO_JDN = {
    "A": 45, "B": 27, "C": 14, "D": 5, "E": None,
    "F": 1, "G": None, "H": 16, "J": 16, "K": 12,
    "L": 6, "M": None, "N": None, "O": None, "Q": None,
    "R": None, "U": None, "V": None, "W": None,
    "X": None, "Y": 40, "Z": None,
}


# ---------------------------------------------------------------------------
# Distance Functions
# ---------------------------------------------------------------------------

def gln_proximity(gln_a, gln_b):
    """Shared prefix length normalized by max length. Same GLN = 1.0."""
    if not gln_a or not gln_b:
        return 0.0
    if gln_a == gln_b:
        return 1.0
    shared = len(os.path.commonprefix([gln_a, gln_b]))
    max_len = max(len(gln_a), len(gln_b))
    return shared / max_len if max_len > 0 else 0.0


def fgid_jaccard(fgid_a, fgid_b):
    """Jaccard similarity of detected FGID department codes."""
    set_a = set(fgid_a) if fgid_a else set()
    set_b = set(fgid_b) if fgid_b else set()
    if not set_a and not set_b:
        return 0.5  # both empty = neutral
    if not set_a or not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)


def cec_match(cec_a, cec_b, jdn_a, jdn_b):
    """CEC class similarity: 1.0 same class, 0.5 same JDN, 0.0 otherwise."""
    if not cec_a or not cec_b:
        return 0.0
    if cec_a == cec_b:
        return 1.0
    # Check JDN domain match via crosswalk
    jdn_a_val = jdn_a if jdn_a else CEC_TO_JDN.get(cec_a)
    jdn_b_val = jdn_b if jdn_b else CEC_TO_JDN.get(cec_b)
    if jdn_a_val and jdn_b_val and jdn_a_val == jdn_b_val:
        return 0.5
    # Check if CEC classes share a letter prefix (e.g., B and B3)
    if cec_a[0] == cec_b[0]:
        return 0.3
    return 0.0


def candidate_overlap(cands_a, cands_b):
    """Jaccard similarity of top-5 candidate GLN sets."""
    set_a = set(cands_a) if cands_a else set()
    set_b = set(cands_b) if cands_b else set()
    if not set_a and not set_b:
        return 0.5
    if not set_a or not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)


def keyword_jaccard(kw_a, kw_b):
    set_a = set(kw_a) if kw_a else set()
    set_b = set(kw_b) if kw_b else set()
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


def sumo_jaccard_from_words(kw_a, kw_b):
    """
    Expand two keyword lists to SUMO concept sets, then compute Jaccard.
    Uses the globally-loaded _SUMO_INDEX / _SUMO_MAPPINGS.
    If either set is empty (no mappings found), returns 0.0.
    """
    if not _SUMO_INDEX or not _SUMO_MAPPINGS:
        return 0.0
    concepts_a = _sumo_wordnet.words_to_sumo(kw_a or [], _SUMO_INDEX, _SUMO_MAPPINGS)
    concepts_b = _sumo_wordnet.words_to_sumo(kw_b or [], _SUMO_INDEX, _SUMO_MAPPINGS)
    return _sumo_wordnet.sumo_jaccard(concepts_a, concepts_b)


def sumo_jaccard_from_cache(sumo_a, kw_b):
    """
    When the doc has precomputed sumo_concepts and the query supplies keywords,
    expand query keywords at runtime and compare against doc's cached SUMO set.
    """
    if not sumo_a and not kw_b:
        return 0.0
    if sumo_a:
        # doc has precomputed concepts; expand query keywords on the fly
        concepts_b = (
            _sumo_wordnet.words_to_sumo(kw_b or [], _SUMO_INDEX, _SUMO_MAPPINGS)
            if _SUMO_INDEX else frozenset()
        )
        return _sumo_wordnet.sumo_jaccard(frozenset(sumo_a), concepts_b) if _sumo_wordnet else 0.0
    # Neither has SUMO cache — fall back to word-level expansion
    return sumo_jaccard_from_words(kw_b, [])


def relevance(query, doc):
    """Compute relevance score between query doc and candidate doc."""
    r_gln  = gln_proximity(query["gln"], doc["gln"])
    r_fgid = fgid_jaccard(query["fgid_detected"], doc["fgid_detected"])
    r_cec  = cec_match(query["cec"], doc["cec"], query.get("jdn"), doc.get("jdn"))
    r_cand = candidate_overlap(query["candidates"], doc["candidates"])
    r_kw   = keyword_jaccard(query.get("keywords", []), doc.get("keywords", []))

    # SUMO semantic axis: prefer precomputed doc concepts; expand query keywords live
    if W_SUMO > 0:
        doc_sumo = doc.get("sumo_concepts")
        q_kw     = query.get("keywords", [])
        if doc_sumo is not None:
            r_sumo = sumo_jaccard_from_cache(doc_sumo, q_kw)
        else:
            # Neither side precomputed — expand both from keywords
            r_sumo = sumo_jaccard_from_words(q_kw, doc.get("keywords", []))
    else:
        r_sumo = 0.0

    score = (W_GLN * r_gln + W_FGID * r_fgid + W_CEC * r_cec
             + W_CANDIDATES * r_cand + W_KEYWORDS * r_kw + W_SUMO * r_sumo)

    return {
        "score": round(score, 4),
        "gln_proximity": round(r_gln, 4),
        "fgid_jaccard": round(r_fgid, 4),
        "cec_match": round(r_cec, 4),
        "candidate_overlap": round(r_cand, 4),
        "keyword_jaccard": round(r_kw, 4),
        "sumo_jaccard": round(r_sumo, 4),
    }


# ---------------------------------------------------------------------------
# Cache Loading
# ---------------------------------------------------------------------------

def load_cache(cache_path):
    """Load precomputed GLN cache from JSONL."""
    entries = []
    with open(cache_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


# ---------------------------------------------------------------------------
# Markdown Output
# ---------------------------------------------------------------------------

def format_fgid(fgid_list):
    """Format FGID list for display."""
    if not fgid_list:
        return "none"
    return ", ".join(fgid_list)


def format_cec(entry):
    """Format CEC class for display."""
    cec = entry.get("cec")
    label = entry.get("cec_label", "")
    if not cec:
        return "unclassified"
    if label:
        return f"{cec} ({label})"
    return cec


def extract_yaml_op(snippet, max_chars=200):
    """If snippet looks like raw YAML, parse out the op: content."""
    import html as _h
    import re as _re
    if not snippet:
        return ""
    s = _h.unescape(snippet)
    if not (s.startswith("threadid:") or s.startswith("title:") or s.startswith("source:")):
        return s
    # Try yaml parse
    try:
        import yaml as _yaml
        doc = _yaml.safe_load(s)
        if isinstance(doc, dict) and "op" in doc:
            text = str(doc["op"] or "").replace("\n", " ").strip()
            if len(text) > max_chars:
                text = text[:max_chars].rsplit(" ", 1)[0] + "..."
            return text
    except Exception:
        pass
    # Fallback regex
    m = _re.search(r"op: (.+)", s)
    if m:
        return m.group(1).strip()[:max_chars]
    return s
def truncate_snippet(snippet, max_len=180):
    """Truncate snippet cleanly."""
    if not snippet:
        return "*No snippet available*"
    if len(snippet) <= max_len:
        return snippet
    return snippet[:max_len].rsplit(" ", 1)[0] + "..."


def render_markdown(query, ranked, permutation_order, top_n=0):
    """Render the ranking as markdown."""
    lines = []
    lines.append("# GLN Relevance Ranking")
    lines.append("")
    import html as _html
    lines.append(f"**Query document:** `{query['path']}`")
    lines.append(f"**Title:** {_html.unescape(query['title'])}")
    lines.append(f"**GLN:** `{query['gln']}` ({query.get('gln_name', '')}) "
                 f"| **CEC:** {format_cec(query)} "
                 f"| **FGID:** [{format_fgid(query.get('fgid_detected', []))}]")
    lines.append(f"**Confidence:** {query.get('confidence', 0):.2f}")
    lines.append("")

    if query.get("snippet"):
        lines.append(f"> {truncate_snippet(extract_yaml_op(query['snippet']))}")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(f"## Candidates (ranked from {len(ranked)} randomly selected documents)")
    lines.append("")
    lines.append(f"*Original permutation order: "
                 f"{', '.join(str(i+1) for i in permutation_order)}*")
    lines.append("")

    detail = ranked[:top_n] if top_n > 0 else ranked
    for rank, (entry, scores) in enumerate(detail, 1):
        rel = scores["score"]

        # Relevance bar (visual indicator)
        bar_len = int(rel * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)

        lines.append(f"### {rank}. `{_html.unescape(entry['title'])}` — relevance: **{rel:.2f}**")
        lines.append(f"`{bar}`")
        lines.append("")
        lines.append(f"**Path:** `{entry['path']}`")
        lines.append(f"**GLN:** `{entry['gln']}` ({entry.get('gln_name', '')}) "
                     f"| **CEC:** {format_cec(entry)} "
                     f"| **FGID:** [{format_fgid(entry.get('fgid_detected', []))}]")
        lines.append("")

        # Score breakdown
        lines.append(f"| Axis | Score | Weight | Contribution |")
        lines.append(f"|------|-------|--------|--------------|")
        for axis, weight, key in [
            ("GLN proximity", W_GLN, "gln_proximity"),
            ("FGID Jaccard", W_FGID, "fgid_jaccard"),
            ("CEC match", W_CEC, "cec_match"),
            ("Candidate overlap", W_CANDIDATES, "candidate_overlap"),
            ("Keyword Jaccard", W_KEYWORDS, "keyword_jaccard"),
            ("SUMO semantic", W_SUMO, "sumo_jaccard"),
        ]:
            raw = scores[key]
            contrib = raw * weight
            lines.append(f"| {axis} | {raw:.2f} | {weight:.2f} | {contrib:.3f} |")
        lines.append("")

        # Snippet
        snippet = truncate_snippet(extract_yaml_op(entry.get("snippet", "")))
        lines.append(f"> {snippet}")
        lines.append("")

    # Summary
    lines.append("---")
    lines.append("")
    lines.append("## Ranking Summary")
    lines.append("")
    lines.append("| Rank | Document | Relevance | Primary Signal |")
    lines.append("|------|----------|-----------|----------------|")
    for rank, (entry, scores) in enumerate(ranked, 1):
        # Identify primary signal
        axis_scores = {
            "GLN": scores["gln_proximity"] * W_GLN,
            "FGID": scores["fgid_jaccard"] * W_FGID,
            "CEC": scores["cec_match"] * W_CEC,
            "Candidates": scores["candidate_overlap"] * W_CANDIDATES,
            "Keywords": scores["keyword_jaccard"] * W_KEYWORDS,
            "SUMO": scores.get("sumo_jaccard", 0.0) * W_SUMO,
        }
        primary = max(axis_scores, key=axis_scores.get)
        lines.append(f"| {rank} | `{entry['title']}` | {scores['score']:.2f} | {primary} |")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="GLN Relevance Ranker - rank documents by GLN metric space distance",
        epilog="Requires precomputed cache from gln-precompute.py",
    )
    parser.add_argument(
        "--cache", "-c",
        default=str(DEFAULT_CACHE),
        help=f"Path to gln-cache.jsonl (default: {DEFAULT_CACHE})",
    )
    parser.add_argument(
        "--query", "-q",
        help="Path of the query document (from index). If omitted, picked randomly.",
    )
    parser.add_argument(
        "--pool", "-n",
        type=int, default=10,
        help="Number of candidate documents to rank (default: 10)",
    )
    parser.add_argument(
        "--seed", "-s",
        type=int, default=None,
        help="Random seed for reproducible selection",
    )
    parser.add_argument(
        "--top", "-t",
        type=int, default=0,
        help="Show only top N results in detail (0 = all). Summary table always shows all.",
    )
    parser.add_argument(
        "--profile", "-p",
        choices=list(PROFILES.keys()), default="internal",
        help=(
            "Scoring profile. internal=GLN-weighted (N200 docs); "
            "external=CEC+keywords (third-party, no SUMO); "
            "external_sumo=CEC+SUMO semantic (requires WordNet). Default: internal"
        ),
    )
    parser.add_argument(
        "--wordnet-path", "-w",
        default=str(DEFAULT_WORDNET),
        help=f"Path to WordNetMappings directory (default: {DEFAULT_WORDNET}). Used by external_sumo profile.",
    )

    args = parser.parse_args()

    # Apply profile weights globally
    global W_GLN, W_FGID, W_CEC, W_CANDIDATES, W_KEYWORDS, W_SUMO
    global _SUMO_INDEX, _SUMO_MAPPINGS
    W_GLN        = PROFILES[args.profile]["gln"]
    W_FGID       = PROFILES[args.profile]["fgid"]
    W_CEC        = PROFILES[args.profile]["cec"]
    W_CANDIDATES = PROFILES[args.profile]["candidates"]
    W_KEYWORDS   = PROFILES[args.profile]["keywords"]
    W_SUMO       = PROFILES[args.profile]["sumo"]

    # Load SUMO/WordNet if the profile uses it
    if W_SUMO > 0:
        if not _SUMO_AVAILABLE:
            print("WARNING: sumo_wordnet.py not available — SUMO weight set to 0", file=sys.stderr)
            W_SUMO = 0.0
        else:
            wn_path = Path(args.wordnet_path)
            if wn_path.exists():
                import time as _time
                _t0 = _time.time()
                print(f"Loading SUMO/WordNet db from {wn_path}...", file=sys.stderr)
                _SUMO_INDEX, _SUMO_MAPPINGS = _sumo_wordnet.load_sumo_db(wn_path)
                print(f"  {len(_SUMO_INDEX):,} lemmas, {len(_SUMO_MAPPINGS):,} synsets  ({_time.time()-_t0:.1f}s)",
                      file=sys.stderr)
            else:
                print(f"WARNING: WordNet path not found: {wn_path} — SUMO weight set to 0", file=sys.stderr)
                W_SUMO = 0.0

    cache_path = Path(args.cache)

    if not cache_path.exists():
        print(f"ERROR: Cache file not found: {cache_path}", file=sys.stderr)
        print("Run gln-precompute.py first to generate the cache.", file=sys.stderr)
        sys.exit(1)

    # Load cache
    entries = load_cache(str(cache_path))
    if len(entries) < args.pool + 1:
        print(f"ERROR: Cache has {len(entries)} entries, need at least {args.pool + 1}",
              file=sys.stderr)
        sys.exit(1)

    # Set random seed
    if args.seed is not None:
        random.seed(args.seed)

    # Select query document
    if args.query:
        # Find by path
        query_entry = None
        for e in entries:
            if e["path"] == args.query or e["path"].endswith(args.query):
                query_entry = e
                break
        if not query_entry:
            print(f"ERROR: Query document not found in cache: {args.query}", file=sys.stderr)
            sys.exit(1)
        # Pick random candidates (excluding query)
        pool = [e for e in entries if e["path"] != query_entry["path"]]
        candidates = random.sample(pool, min(args.pool, len(pool)))
    else:
        # Pick 11 random, first is query
        selection = random.sample(entries, args.pool + 1)
        query_entry = selection[0]
        candidates = selection[1:]

    # Randomly permute candidates (the "shuffled deck" the ranker must sort)
    random.shuffle(candidates)
    # Record the permutation order (original indices before sorting)
    permutation_indices = list(range(len(candidates)))

    # Score each candidate against the query
    scored = []
    for entry in candidates:
        scores = relevance(query_entry, entry)
        scored.append((entry, scores))

    # Sort by relevance descending
    scored.sort(key=lambda x: x[1]["score"], reverse=True)

    # Track permutation: map each ranked doc back to its shuffled position
    ranked_paths = [e["path"] for e, _ in scored]
    shuffled_paths = [e["path"] for e in candidates]
    permutation_order = [shuffled_paths.index(p) for p in ranked_paths]

    # Render markdown
    md = render_markdown(query_entry, scored, permutation_order, top_n=args.top)
    print(md)


if __name__ == "__main__":
    main()
