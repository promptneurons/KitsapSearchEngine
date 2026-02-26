#!/usr/bin/env python3
"""
extract-mec-candidates.py - Bootstrap MEC hierarchy from corpus

Introspects combined JSONL caches to surface:
  1. Keyword frequency per CEC class (leaf-level MEC candidates)
  2. Wikilink in-degree centrality (hub nodes = candidate MEC category headers)
  3. Cross-CEC links (nodes linked from multiple classes = candidate cross-cuts)

Output: mec-candidates.yaml — ready for Reference Librarian to curate into
5-level MEC hierarchy.

Usage:
  python3 scripts/extract-mec-candidates.py \
    --caches /home/john/openplanter/data/obsidian-local-cache.jsonl \
             /home/john/KitsapSearchEngine/data/daynotes-cache.jsonl \
    --output /home/john/openplanter/mec-candidates.yaml
"""

import argparse
import json
import yaml
from collections import Counter, defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT   = SCRIPT_DIR.parent



# YAML artifact stopwords — field names, URL fragments, platform tokens
_STOPWORDS = {
    'path','null','title','source','tags','frontmatter','dir','sprint','quarter',
    'lang','links','region','leaf','gln','cec','jdn','fgid','snippet','keywords',
    'candidates','confidence','archetype','prefix','detected','node','sumo',
    'https','http','com','org','wiki','www','nhttps','net','edu','gov','io',
    'daynotes','obsidian','fleeting','azure','wikis','data','index','local',
    'jegoodwin','install','build','test','pdf','dev','new','use','notes','overview',
    # DTG month fragments (tAUG, tJUL, uJAN etc.)
    'taug','tjul','tjan','tfeb','tmar','tapr','tmay','tjun',
    'tsep','toct','tnov','tdec','uaug','ujul','ujan','ufeb',
    'umar','uapr','umay','ujun','usep','uoct','unov','udec',
    'the','and','for','with','this','that','from','are','was','has','have',
    'but','not','all','one','can','also','more','its','into','than','then',
}

def _is_subject_keyword(kw):
    kw_lower = kw.lower()
    return (
        kw_lower not in _STOPWORDS
        and len(kw) >= 4
        and not kw.startswith('$')
        and not kw.startswith('`')
        and not kw[0].isdigit()
        and not any(c in kw for c in ['/', '\\', '=', '?', '#', '@', '!'])
    )


def load_jsonl(path):
    entries = []
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


def main():
    p = argparse.ArgumentParser(description='Extract MEC candidates from corpus caches')
    p.add_argument('--caches', nargs='+', required=True, help='Paths to *-cache.jsonl files')
    p.add_argument('--output', '-o', required=True, help='Output mec-candidates.yaml path')
    p.add_argument('--top', type=int, default=30, help='Top N candidates per CEC class')
    args = p.parse_args()

    # ── Load all caches ─────────────────────────────────────────────────────
    all_entries = []
    corpus_names = []
    for cache_path in args.caches:
        entries = load_jsonl(cache_path)
        name = Path(cache_path).stem.replace('-cache', '')
        corpus_names.append(f'{name} ({len(entries)} notes)')
        for e in entries:
            e['_corpus'] = name
        all_entries.extend(entries)
        print(f'  Loaded {len(entries)} entries from {cache_path}', flush=True)

    print(f'  Total: {len(all_entries)} entries across {len(args.caches)} corpora')

    # ── Per-CEC accumulators ─────────────────────────────────────────────────
    cec_labels   = {}          # cec -> label string
    cec_counts   = Counter()   # cec -> note count
    cec_keywords = defaultdict(Counter)   # cec -> keyword -> freq
    cec_links_out = defaultdict(Counter)  # cec -> link_target -> freq (from notes in this cec)
    link_to_cec  = defaultdict(set)       # link_target -> set of cec classes that link to it

    # Global link in-degree (across all entries that have 'links')
    global_indegree = Counter()           # title/target -> in-degree
    title_to_cec    = {}                  # title -> cec (for context)

    for e in all_entries:
        cec = e.get('cec') or 'UNKNOWN'
        label = e.get('cec_label') or ''
        cec_labels[cec] = label
        cec_counts[cec] += 1

        # Keywords
        for kw in (e.get('keywords') or []):
            if _is_subject_keyword(kw):
                cec_keywords[cec][kw] += 1

        # Wikilinks (only entries that have the 'links' field)
        title = e.get('title', '')
        if title:
            title_to_cec[title] = cec
        for target in (e.get('links') or []):
            if not target or len(target) < 3 or target[0].isdigit() or any(c in target for c in ['$','`','=','\n']): continue
            global_indegree[target] += 1
            cec_links_out[cec][target] += 1
            link_to_cec[target].add(cec)

    # ── Cross-CEC hub nodes ──────────────────────────────────────────────────
    # Nodes linked from 2+ different CEC classes = candidate cross-cut concepts
    cross_cec_hubs = {
        t: (cnt, sorted(link_to_cec[t]))
        for t, cnt in global_indegree.items()
        if len(link_to_cec[t]) >= 2 and cnt >= 3
    }

    # ── Build output structure ───────────────────────────────────────────────
    cec_section = {}
    for cec in sorted(cec_counts.keys()):
        top_keywords = [
            {'term': kw, 'freq': cnt}
            for kw, cnt in cec_keywords[cec].most_common(args.top)
        ]
        # Hub nodes linked TO from this CEC class
        hub_nodes = [
            {
                'target': target,
                'link_freq': cnt,
                'in_degree_global': global_indegree[target],
                'linked_from_cecs': sorted(link_to_cec[target]),
            }
            for target, cnt in cec_links_out[cec].most_common(args.top)
            if cnt >= 2
        ]

        cec_section[cec] = {
            'label': cec_labels.get(cec, ''),
            'note_count': cec_counts[cec],
            'pct': round(cec_counts[cec] * 100 / len(all_entries), 1),
            'top_keywords': top_keywords,
            'hub_nodes': hub_nodes,
        }

    # Cross-CEC hubs sorted by in-degree
    cross_hubs_sorted = sorted(
        [{'target': t, 'in_degree': cnt, 'cec_classes': classes}
         for t, (cnt, classes) in cross_cec_hubs.items()],
        key=lambda x: -x['in_degree']
    )[:50]

    output = {
        'meta': {
            'corpora': corpus_names,
            'total_notes': len(all_entries),
            'cec_classes': len(cec_counts),
            'has_wikilinks': sum(1 for e in all_entries if e.get('links')),
            'purpose': 'MEC candidate extraction for Reference Librarian curation',
            'next_step': 'Curate into 5-level MEC hierarchy; assign sub-classes to candidates',
        },
        'cross_cec_hubs': cross_hubs_sorted,
        'by_cec': cec_section,
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        yaml.dump(output, f, allow_unicode=True, sort_keys=False, default_flow_style=False)

    print(f'\nOutput: {args.output}')
    print(f'  {len(cec_counts)} CEC classes, {len(cross_cec_hubs)} cross-CEC hubs')


if __name__ == '__main__':
    main()
