#!/usr/bin/env python3
"""
extract-mec-candidates.py — Bootstrap MEC sub-ontology from KSE caches.

Signals:
  1. Keyword frequency (counter over keywords[] field, per CEC letter)
  2. PageRank over wikilink graph (Beads-style: dependencies → importance)
     Combined score: mec_score(kw, cec) = Σ pagerank[note] × freq(kw, note)
                     for all notes in that CEC letter bucket.

Output: mec-candidates.yaml grouped by CEC top-level letter, ranked by score.

Usage:
  python3 extract-mec-candidates.py [--obsidian CACHE] [--daynotes CACHE]
                                     [--output mec-candidates.yaml]
                                     [--top N] [--min-freq M]
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required (pip install pyyaml)", file=sys.stderr)
    sys.exit(1)

# ── defaults ──────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
REPO_ROOT    = SCRIPT_DIR.parent
DATA_DIR     = REPO_ROOT / "data"
OPENPLANTER  = Path("/home/john/openplanter/data")

DEFAULT_CACHES = [
    OPENPLANTER / "obsidian-local-cache.jsonl",
    DATA_DIR    / "daynotes-cache.jsonl",
]

DAMPING      = 0.85
MAX_ITER     = 100
CONVERGENCE  = 1e-6
TOP_N        = 30    # candidates per CEC letter
MIN_FREQ     = 2     # minimum raw keyword occurrence to be listed
SCORE_BLEND  = 0.6   # weight for pagerank-boosted score vs raw freq (0=raw,1=pr)
# Keywords to suppress — YAML field names / URL fragments / function words
# that bleed into keyword extraction when gln-precompute reads raw .yaml files
YAML_STOPWORDS = {
    "null", "path", "daynotes", "obsidian", "title", "source", "tags",
    "frontmatter", "dir", "sprint", "quarter", "region", "lang", "links",
    "op", "gln", "cec", "fgid", "jdn", "snippet", "keywords", "candidates",
    "https", "http", "www", "com", "org", "net", "gov", "edu",
    "true", "false", "none", "new", "old", "via", "see", "also",
    "data", "file", "files", "list", "note", "notes", "page", "pages",
    "index", "ref", "refs", "use", "used", "using",
}


# ── data loading ──────────────────────────────────────────────────────────────
def load_caches(paths):
    docs = []
    for p in paths:
        p = Path(p)
        if not p.exists():
            print(f"WARN: cache not found: {p}", file=sys.stderr)
            continue
        count = 0
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    docs.append(json.loads(line))
                    count += 1
                except json.JSONDecodeError:
                    pass
        print(f"  loaded {count:,} entries from {p.name}", file=sys.stderr)
    return docs


# ── pagerank over wikilink graph ──────────────────────────────────────────────
def build_pagerank(docs):
    """
    Build a PageRank score for each document, using wikilinks as edges.
    Links are title-based; resolve title → doc index via a title map.
    Returns: dict title → pagerank_score
    """
    # Build title index (case-insensitive, strip spaces)
    title_to_idx = {}
    for i, d in enumerate(docs):
        t = (d.get("title") or "").strip().lower()
        if t:
            title_to_idx[t] = i

    n = len(docs)
    if n == 0:
        return {}

    # Adjacency: out-edges per node (doc index → list of target indices)
    out_edges = defaultdict(list)
    in_edges  = defaultdict(list)   # for dangling node handling
    for i, d in enumerate(docs):
        links = d.get("links") or []
        for lnk in links:
            j = title_to_idx.get(lnk.strip().lower())
            if j is not None and j != i:
                out_edges[i].append(j)
                in_edges[j].append(i)

    # Power iteration
    pr     = {i: 1.0 / n for i in range(n)}
    for iteration in range(MAX_ITER):
        new_pr = {}
        dangling_sum = sum(pr[i] for i in range(n) if not out_edges[i])
        for i in range(n):
            # Contribution from dangling nodes (distribute evenly)
            rank = (1.0 - DAMPING) / n + DAMPING * dangling_sum / n
            # Contribution from in-edges
            for j in in_edges[i]:
                out_count = len(out_edges[j])
                if out_count > 0:
                    rank += DAMPING * pr[j] / out_count
            new_pr[i] = rank
        # Check convergence
        delta = sum(abs(new_pr[i] - pr[i]) for i in range(n))
        pr = new_pr
        if delta < CONVERGENCE:
            print(f"  PageRank converged at iteration {iteration+1} (Δ={delta:.2e})", file=sys.stderr)
            break

    # Return title → score mapping
    result = {}
    for i, d in enumerate(docs):
        t = (d.get("title") or "").strip()
        if t:
            result[t] = pr[i]
    return result


# ── scoring ───────────────────────────────────────────────────────────────────
def score_candidates(docs, pagerank, top_n, min_freq, blend):
    """
    For each CEC top-level letter, produce a ranked list of keyword candidates.

    mec_score(kw, cec) = (1-blend) * raw_freq(kw, cec)
                       +    blend  * Σ pagerank[note] * in_note(kw, note)
    """
    # Per-CEC: raw keyword counter + pagerank-weighted counter
    cec_raw   = defaultdict(Counter)   # cec_letter → Counter{kw: count}
    cec_pr    = defaultdict(Counter)   # cec_letter → Counter{kw: pr_weight}
    cec_docs  = defaultdict(int)       # cec_letter → doc count

    for d in docs:
        cec = (d.get("cec") or "?").strip().upper()[:1]
        keywords = d.get("keywords") or []
        title = (d.get("title") or "").strip()
        pr_score = pagerank.get(title, 1.0 / max(len(docs), 1))

        cec_docs[cec] += 1
        for kw in keywords:
            kw = kw.strip().lower()
            if not kw or len(kw) < 3 or kw in YAML_STOPWORDS:
                continue
            cec_raw[cec][kw] += 1
            cec_pr[cec][kw]  += pr_score

    # Normalize and blend
    results = {}
    for cec in sorted(cec_raw.keys()):
        raw_cnt   = cec_raw[cec]
        pr_cnt    = cec_pr[cec]
        doc_count = cec_docs[cec]

        # Normalise raw freq → [0,1]
        max_raw = max(raw_cnt.values()) if raw_cnt else 1
        # Normalise pr-weighted sum → [0,1]
        max_pr  = max(pr_cnt.values()) if pr_cnt else 1

        scored = {}
        all_kws = set(raw_cnt.keys()) | set(pr_cnt.keys())
        for kw in all_kws:
            rf = raw_cnt.get(kw, 0)
            if rf < min_freq:
                continue
            norm_raw = rf / max_raw
            norm_pr  = pr_cnt.get(kw, 0) / max_pr
            scored[kw] = (1 - blend) * norm_raw + blend * norm_pr

        ranked = sorted(scored.items(), key=lambda x: -x[1])[:top_n]
        results[cec] = {
            "doc_count": doc_count,
            "candidates": [
                {
                    "keyword":    kw,
                    "score":      round(score, 4),
                    "raw_freq":   raw_cnt.get(kw, 0),
                    "pr_weight":  round(pr_cnt.get(kw, 0), 6),
                }
                for kw, score in ranked
            ],
        }
    return results


# ── in-degree stats (informational) ──────────────────────────────────────────
def link_stats(docs):
    title_to_doc = {(d.get("title") or "").strip().lower(): d for d in docs}
    in_degree = Counter()
    for d in docs:
        for lnk in (d.get("links") or []):
            k = lnk.strip().lower()
            if k in title_to_doc:
                in_degree[title_to_doc[k].get("title", k)] += 1
    return in_degree.most_common(20)


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--obsidian", default=str(DEFAULT_CACHES[0]),
                   help="Path to obsidian-local-cache.jsonl")
    p.add_argument("--daynotes", default=str(DEFAULT_CACHES[1]),
                   help="Path to daynotes-cache.jsonl")
    p.add_argument("--extra", nargs="*", default=[],
                   help="Additional cache paths to include")
    p.add_argument("--output", "-o",
                   default=str(REPO_ROOT / "data" / "mec-candidates.yaml"),
                   help="Output path for mec-candidates.yaml")
    p.add_argument("--top", "-n", type=int, default=TOP_N,
                   help=f"Candidates per CEC letter (default {TOP_N})")
    p.add_argument("--min-freq", type=int, default=MIN_FREQ,
                   help=f"Minimum keyword occurrences (default {MIN_FREQ})")
    p.add_argument("--blend", type=float, default=SCORE_BLEND,
                   help=f"PageRank blend weight 0=raw-freq, 1=pr-only (default {SCORE_BLEND})")
    p.add_argument("--no-pagerank", action="store_true",
                   help="Skip PageRank (use raw keyword frequency only)")
    args = p.parse_args()

    cache_paths = [args.obsidian, args.daynotes] + (args.extra or [])

    print("Loading caches...", file=sys.stderr)
    docs = load_caches(cache_paths)
    print(f"Total: {len(docs):,} docs", file=sys.stderr)

    if not docs:
        print("ERROR: no documents loaded", file=sys.stderr)
        sys.exit(1)

    if args.no_pagerank:
        print("Skipping PageRank (--no-pagerank)", file=sys.stderr)
        pagerank = {}
        blend = 0.0
    else:
        print(f"Building PageRank graph (D={DAMPING}, max_iter={MAX_ITER})...", file=sys.stderr)
        link_count = sum(len(d.get("links") or []) for d in docs)
        print(f"  {link_count:,} total link edges across {len(docs):,} docs", file=sys.stderr)
        pagerank = build_pagerank(docs)
        blend = args.blend

    print(f"Scoring candidates (blend={blend}, top={args.top}, min_freq={args.min_freq})...", file=sys.stderr)
    results = score_candidates(docs, pagerank, args.top, args.min_freq, blend)

    # Top linked notes (informational)
    top_linked = link_stats(docs)

    output = {
        "meta": {
            "sources":    cache_paths,
            "total_docs": len(docs),
            "cec_letters": len(results),
            "pagerank":   not args.no_pagerank,
            "damping":    DAMPING,
            "blend":      blend,
            "top_n":      args.top,
            "min_freq":   args.min_freq,
        },
        "top_linked_notes": [
            {"title": title, "in_degree": deg} for title, deg in top_linked
        ],
        "cec_groups": {
            cec: {
                "doc_count":  data["doc_count"],
                "candidates": data["candidates"],
            }
            for cec, data in sorted(results.items())
        },
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(output, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    print(f"\nWrote {out_path}", file=sys.stderr)
    print(f"CEC letters covered: {', '.join(sorted(results.keys()))}", file=sys.stderr)
    total_candidates = sum(len(v["candidates"]) for v in results.values())
    print(f"Total candidates: {total_candidates}", file=sys.stderr)


if __name__ == "__main__":
    main()
