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

# Distance function weights
W_GLN = 0.40
W_FGID = 0.25
W_CEC = 0.20
W_CANDIDATES = 0.15

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


def relevance(query, doc):
    """Compute relevance score between query doc and candidate doc."""
    r_gln = gln_proximity(query["gln"], doc["gln"])
    r_fgid = fgid_jaccard(query["fgid_detected"], doc["fgid_detected"])
    r_cec = cec_match(query["cec"], doc["cec"], query.get("jdn"), doc.get("jdn"))
    r_cand = candidate_overlap(query["candidates"], doc["candidates"])

    score = W_GLN * r_gln + W_FGID * r_fgid + W_CEC * r_cec + W_CANDIDATES * r_cand

    return {
        "score": round(score, 4),
        "gln_proximity": round(r_gln, 4),
        "fgid_jaccard": round(r_fgid, 4),
        "cec_match": round(r_cec, 4),
        "candidate_overlap": round(r_cand, 4),
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


def truncate_snippet(snippet, max_len=180):
    """Truncate snippet cleanly."""
    if not snippet:
        return "*No snippet available*"
    if len(snippet) <= max_len:
        return snippet
    return snippet[:max_len].rsplit(" ", 1)[0] + "..."


def render_markdown(query, ranked, permutation_order):
    """Render the ranking as markdown."""
    lines = []
    lines.append("# GLN Relevance Ranking")
    lines.append("")
    lines.append(f"**Query document:** `{query['path']}`")
    lines.append(f"**Title:** {query['title']}")
    lines.append(f"**GLN:** `{query['gln']}` ({query.get('gln_name', '')}) "
                 f"| **CEC:** {format_cec(query)} "
                 f"| **FGID:** [{format_fgid(query.get('fgid_detected', []))}]")
    lines.append(f"**Confidence:** {query.get('confidence', 0):.2f}")
    lines.append("")

    if query.get("snippet"):
        lines.append(f"> {truncate_snippet(query['snippet'])}")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(f"## Candidates (ranked from {len(ranked)} randomly selected documents)")
    lines.append("")
    lines.append(f"*Original permutation order: "
                 f"{', '.join(str(i+1) for i in permutation_order)}*")
    lines.append("")

    for rank, (entry, scores) in enumerate(ranked, 1):
        rel = scores["score"]

        # Relevance bar (visual indicator)
        bar_len = int(rel * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)

        lines.append(f"### {rank}. `{entry['title']}` — relevance: **{rel:.2f}**")
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
        ]:
            raw = scores[key]
            contrib = raw * weight
            lines.append(f"| {axis} | {raw:.2f} | {weight:.2f} | {contrib:.3f} |")
        lines.append("")

        # Snippet
        snippet = truncate_snippet(entry.get("snippet", ""))
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

    args = parser.parse_args()
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
    md = render_markdown(query_entry, scored, permutation_order)
    print(md)


if __name__ == "__main__":
    main()
