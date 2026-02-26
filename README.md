# KitsapSearchEngine

A local semantic search engine for archival corpora. No cloud, no training data, no lock-in.

Distills any text dataset into a searchable index using universal subject classification (CEC) and ontological concept expansion (SUMO/WordNet). Runs on a laptop.

## The Pitch

> "Solving 90% of a customer's 'find our stuff' problem in a day is a business model you can franchise at scale."

1. Point at a dataset (forum archive, document store, newsletter export)
2. Extract → classify → cache (one day's work)
3. Deploy a local SERP — no Google needed

## How It Works

Three scoring axes for relevance ranking:

| Axis | Weight (external) | What it does |
|------|-------------------|--------------|
| **CEC** | 50% | Cutter Expansive Classification — universal library subject hierarchy, zero-shot |
| **SUMO semantic** | 30% | WordNet 3.0 → SUMO concept expansion — bridges informal ↔ formal vocabulary |
| **Keyword Jaccard** | 20% | Surface token overlap — fast, precise for exact terms |

**CEC** classifies documents to library subjects (A=Philosophy, B=Theology, C=History, F=Society, H=Politics…) using keyword frequency. No training.

**SUMO expansion** maps keywords to upper-ontology concepts via WordNet 3.0. "troops" and "military" both resolve to `MilitaryForce`. "priest" and "clergy" both resolve to `Cleric`. Bridges vocabulary across corpora without any training data.

## Demo Dataset: Original Dissent Forum (2001–2005)

- **10,561 threads**, **108,898 posts**
- CEC coverage: 90.8% classified on first run
- Subject distribution: F=20.6%, H=10.0%, E=9.9%, C=9.8%
- Used by: [od-archivist](https://github.com/promptneurons/od-archivist) PN Desktop plugin

## Quick Start

```bash
git clone https://github.com/promptneurons/KitsapSearchEngine
cd KitsapSearchEngine

# Install deps (pyyaml only — everything else is stdlib)
pip3 install pyyaml

# Run existing OD-2006 demo (requires od-cache.jsonl — see below)
python3 scripts/gln-ranker.py \
  --cache data/od-cache.jsonl \
  --profile external_sumo \
  --pool 200 --top 10 \
  --seed 42
```

## Building Your Own Index

### Step 1: Extract threads to YAML

Each document becomes one YAML file with metadata + 80-line content excerpt:

```bash
# Original Dissent SQLite → YAML (requires posts_markdown.db)
python3 scripts/od-extract-threads.py

# Salo archive TTL → YAML (requires salo-threads.ttl)  
python3 scripts/salo-extract-ttl.py

# Result: data/{corpus}-threads/thread-NNNNNN.yaml
#         data/{corpus}-thread-index.yaml
```

### Step 2: Precompute semantic vectors

```bash
python3 scripts/gln-precompute.py \
  --index data/od-thread-index.yaml \
  --output data/od-cache.jsonl

# Adds per-document: CEC class, keywords, SUMO concepts
# Runtime: ~90s for 10k threads on modest hardware
```

### Step 3: Search

```bash
# Interactive ranker (document-to-document similarity)
python3 scripts/gln-ranker.py \
  --cache data/od-cache.jsonl \
  --profile external_sumo \
  --pool 200 --top 10

# Free-text search (via od-archivist integration)
python3 /path/to/od-archivist/scripts/search.py \
  "paleoconservatism Buchanan foreign policy" \
  --cache data/od-cache.jsonl
```

## Scoring Profiles

```python
PROFILES = {
    # N200 private documents — org-specific GLN tree is meaningful
    "internal":      {"gln": 0.40, "fgid": 0.25, "cec": 0.20, "candidates": 0.15, "keywords": 0.00, "sumo": 0.00},
    # Third-party data — CEC + surface keywords, no SUMO load overhead
    "external":      {"gln": 0.00, "fgid": 0.00, "cec": 0.70, "candidates": 0.00, "keywords": 0.30, "sumo": 0.00},
    # Third-party data + semantic expansion — best for cross-vocabulary corpora
    "external_sumo": {"gln": 0.00, "fgid": 0.00, "cec": 0.50, "candidates": 0.00, "keywords": 0.20, "sumo": 0.30},
}
```

## Data Requirements

| File | Purpose | Source |
|------|---------|--------|
| `data/cec-hierarchy.ttl` | CEC subject hierarchy | Included |
| `data/wordnet-mappings/` | WordNet 3.0 + SUMO mappings | `/home/john/sumo/WordNetMappings/` (symlink) |
| `data/od-thread-index.yaml` | Thread index (committed) | Included |
| `data/od-cache.jsonl` | Precomputed vectors (gitignored) | Build with gln-precompute.py |
| `data/od-threads/` | Per-thread YAMLs (gitignored) | Build with od-extract-threads.py |

## Scripts

| Script | Purpose |
|--------|---------|
| `gln-resolver.py` | Classify a single document → CEC + FGID + GLN |
| `gln-precompute.py` | Build JSONL cache for entire index |
| `gln-ranker.py` | Rank documents by relevance to a query |
| `sumo_wordnet.py` | WordNet → SUMO concept bridge |
| `od-extract-threads.py` | SQLite → per-thread YAML (OD-2006) |
| `salo-extract-ttl.py` | TTL + HTML → per-thread YAML (Salo archive) |
| `salo-extract-threads.py` | HTML-only Salo extractor (superseded) |

## Extending to New Corpora

The pipeline is a pattern, not a product. Adapt it to any text dataset:

1. Write an extractor that produces one YAML per document:
   ```yaml
   threadid: "12345"
   title: "Document title"
   source: "your-corpus"
   op: "First 80 lines of content..."
   ```
2. Build an index YAML (see `data/od-thread-index.yaml` for schema)
3. Run `gln-precompute.py --index your-index.yaml --output your-cache.jsonl`
4. Search with `gln-ranker.py --cache your-cache.jsonl --profile external_sumo`

The CEC classifier and SUMO expansion work on any English text with no configuration.

## Related Projects

- [od-archivist](https://github.com/promptneurons/od-archivist) — PN Desktop plugin using this engine for OD-2006 search
- [PN Desktop](https://github.com/promptneurons/pn-desktop) — host application

## License

MIT
