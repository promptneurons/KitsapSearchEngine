# KitsapSearchEngine

**RAG on the cheap.** A local document search engine that classifies and ranks private datasets using CEC subject classification — no cloud, no Google, no training required.

## What It Does

Given any document corpus (forum threads, SMB files, archive collections), this toolchain:
1. **Extracts** documents into structured YAML (one file per document, up to 80 lines of content)
2. **Classifies** each document using the Cutter Expansive Classification (CEC) subject hierarchy
3. **Ranks** documents by relevance using a 4-axis metric space
4. **Outputs** SERP-style markdown with score breakdowns and snippets

## Architecture

```
Source data (SQLite / files)
        ↓
od-extract-threads.py       ← adapter: SQLite → per-thread YAML + index
        ↓
gln-precompute.py           ← classify all docs, build JSONL cache
        ↓
gln-ranker.py               ← rank docs against a query, output SERP markdown
```

## Classification Axes

| Axis | Weight (internal) | Weight (external) | Signal |
|------|-------------------|-------------------|--------|
| GLN proximity | 40% | 0% | Organizational tree distance (private) |
| FGID Jaccard | 25% | 0% | Department code overlap (private) |
| CEC match | 20% | 70% | Subject classification match |
| Candidate overlap | 15% | 30% | Shared classification candidates |

**Internal profile:** for private `clawd`-structured document sets.  
**External profile:** for customer/third-party datasets — CEC does the work.

## Quick Start

```bash
# 1. Extract forum threads (vBulletin SQLite → YAML + index)
python3 scripts/od-extract-threads.py --db /path/to/posts_markdown.db --limit 500

# 2. Precompute classification cache
python3 scripts/gln-precompute.py --index data/od-thread-index.yaml --output data/od-cache.jsonl

# 3. Rank — pick 11 random docs, rank 10 against 1
python3 scripts/gln-ranker.py --cache data/od-cache.jsonl

# 4. Rank against a specific query
python3 scripts/gln-ranker.py --cache data/od-cache.jsonl --query data/od-threads/thread-020872.yaml
```

## Data Dependencies

- `data/cec-hierarchy.ttl` — CEC subject class hierarchy (included)
- `data/jdn-ontology.ttl` — JDN ↔ CEC crosswalk (optional, enhances CEC scoring)
- `llms-N200-index.yaml` — private document index (not included, internal use only)

## OD-2006 Demo Dataset

The `data/od-thread-index.yaml` indexes 10,561 threads from the Original Dissent forum archive (2002–2006). CEC distribution:

| CEC | Subject | Threads |
|-----|---------|---------|
| F | History | 20.6% |
| H | Society/Demotics | 10.0% |
| E | Biography/Literature | 9.9% |
| C | Christianity/Judaism | 9.8% |
| J | Social Science | 7.4% |
| K | Law | 6.8% |

## Product Vision

SMB deployment pattern:
1. Customer provides their data (SharePoint export, forum archive, file share)
2. Distill → classify → index (this toolchain)
3. Deliver a box that knows their data — searchable, navigable, no external dependencies

Feeds into **OpenPlanter** for packaging and deployment.

## Files

```
scripts/
  gln-resolver.py        Triple-axis GLN/CEC classifier (935 lines)
  gln-precompute.py      Build JSONL cache from document index
  gln-ranker.py          4-axis relevance ranker, SERP output
  od-extract-threads.py  Adapter: vBulletin SQLite → per-thread YAML
data/
  cec-hierarchy.ttl      CEC subject class hierarchy
  od-thread-index.yaml   OD-2006 thread index (10,561 entries)
skills/
  SKILL.md               document-classifier skill v0.3
```
