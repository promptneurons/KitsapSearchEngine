#!/usr/bin/env python3
"""
sumo_wordnet.py - WordNet 3.0 ↔ SUMO concept bridge

Provides lightweight word → SUMO concept expansion using the Sigma
WordNet-SUMO mapping files (no NLTK, no network, stdlib only).

Usage (as module):
    from sumo_wordnet import load_sumo_db, words_to_sumo
    db = load_sumo_db("/path/to/WordNetMappings/")
    concepts = words_to_sumo(["war", "troops", "priest"], db)
    # → frozenset({'WarfareSituation', 'MilitaryForce', 'ReligiousRole', ...})

The mapping covers ~117k synsets across noun/verb/adj/adv POS.
Relationship types:
    =   equivalent        (most specific match)
    +   subsumed by       (word is a kind of SUMO concept)
    @   instance of       (word is an instance of SUMO concept)
    :   complement of equivalent
    [   complement of subsumed
"""

import os
import re
import sys
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

_SUMO_PATTERN = re.compile(r'&%(\w+)[=+@:\[\]]')
_SYNSET_LINE  = re.compile(r'^(\d{8})\s')


def load_index_sense(path):
    """
    Parse index.sense → dict: word_lemma → set of synset_offsets (str).

    index.sense line format:
        word%pos:lexname:lexid:: synset_offset sense_num tag_cnt
    We normalise the word lemma (lowercase, underscores→spaces stripped).
    """
    index = {}
    path = Path(path)
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith(";"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            sense_key = parts[0]          # e.g. war%1:04:00::
            offset    = parts[1]          # 8-digit synset offset
            word = sense_key.split("%")[0].replace("_", " ").lower()
            if word not in index:
                index[word] = set()
            index[word].add(offset)
    return index


def load_sumo_mappings(*paths):
    """
    Parse WordNetMappings30-{noun,verb,adj,adv}.txt
    → dict: synset_offset (str) → SUMO concept name (str).

    Each data line begins with an 8-digit synset offset and ends with &%Concept=|+|@
    Lines starting with ;; are comments.
    """
    mappings = {}
    for path in paths:
        path = Path(path)
        if not path.exists():
            continue
        with path.open(encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.startswith(";"):
                    continue
                m_offset = _SYNSET_LINE.match(line)
                if not m_offset:
                    continue
                offset = m_offset.group(1)
                m_sumo = _SUMO_PATTERN.search(line)
                if m_sumo:
                    mappings[offset] = m_sumo.group(1)
    return mappings


def load_sumo_db(wordnet_dir):
    """
    Convenience loader: given the WordNetMappings directory, return a
    (index_sense, sumo_mappings) tuple ready for words_to_sumo().

    Loads noun + verb + adj mappings (skips adv — rarely informative).
    """
    d = Path(wordnet_dir)
    index = load_index_sense(d / "index.sense")
    mappings = load_sumo_mappings(
        d / "WordNetMappings30-noun.txt",
        d / "WordNetMappings30-verb.txt",
        d / "WordNetMappings30-adj.txt",
    )
    return index, mappings


# ─────────────────────────────────────────────────────────────────────────────
# Concept expansion
# ─────────────────────────────────────────────────────────────────────────────

def words_to_sumo(words, index_sense, sumo_mappings):
    """
    Map a list of word strings to a frozenset of SUMO concept names.

    Words that have no WordNet entry or no SUMO mapping are silently
    skipped.  Multi-word phrases are ignored (keywords are single tokens).
    """
    concepts = set()
    for word in words:
        word_lc = word.lower()
        offsets = index_sense.get(word_lc)
        if not offsets:
            continue
        for offset in offsets:
            concept = sumo_mappings.get(offset)
            if concept:
                concepts.add(concept)
    return frozenset(concepts)


def sumo_jaccard(concepts_a, concepts_b):
    """
    Jaccard similarity between two SUMO concept sets.
    Returns 0.0 when either set is empty (no signal, not neutral).
    """
    if not concepts_a or not concepts_b:
        return 0.0
    set_a = frozenset(concepts_a)
    set_b = frozenset(concepts_b)
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union) if union else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# CLI smoke-test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse, time

    parser = argparse.ArgumentParser(description="SUMO WordNet smoke-test")
    parser.add_argument("--wordnet", default=str(Path(__file__).resolve().parent.parent / "data" / "wordnet-mappings"),
                        help="Path to WordNetMappings directory")
    parser.add_argument("words", nargs="*", default=["war", "troops", "priest", "marriage", "taxation", "computer"])
    args = parser.parse_args()

    print(f"Loading SUMO db from {args.wordnet}...", file=sys.stderr)
    t0 = time.time()
    idx, maps = load_sumo_db(args.wordnet)
    elapsed = time.time() - t0
    print(f"  index.sense: {len(idx):,} lemmas | mappings: {len(maps):,} synsets  ({elapsed:.1f}s)", file=sys.stderr)

    for w in args.words:
        concepts = words_to_sumo([w], idx, maps)
        print(f"  {w!r:20s} → {sorted(concepts)}")

    # Pair comparison
    a = list(words_to_sumo(["war",  "troops",   "battle"], idx, maps))
    b = list(words_to_sumo(["army", "military",  "fight"],  idx, maps))
    c = list(words_to_sumo(["tax",  "economics", "trade"],  idx, maps))
    print(f"\nwar/troops/battle  vs  army/military/fight  → jaccard={sumo_jaccard(a, b):.3f}")
    print(f"war/troops/battle  vs  tax/economics/trade  → jaccard={sumo_jaccard(a, c):.3f}")
