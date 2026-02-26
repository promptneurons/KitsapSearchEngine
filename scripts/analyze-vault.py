#!/usr/bin/env python3
"""
analyze-vault.py - Comprehensive analysis of a KSE-indexed vault

Analyzes timeline, CEC subject distribution, threads of thought,
folder structure, and optionally cross-references with a clawd beads catalog.

Usage:
  python3 scripts/analyze-vault.py --cache data/obsidian-local-cache.jsonl
  python3 scripts/analyze-vault.py --cache data/obsidian-local-cache.jsonl \
      --beads /home/john/clawd/.beads/fleeting-r100.jsonl \
      --output /home/john/openplanter/obsidian-local-analysis.md
"""

import argparse
import json
import re
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent


def parse_dtg(dtg_str):
    if not dtg_str:
        return None
    match = re.match(r'^\d{6}[A-Z]?([A-Z]{3})(\d{2})$', dtg_str)
    if match:
        month_str, year_str = match.groups()
        month_map = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
                     'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
        month = month_map.get(month_str)
        if month:
            return (2000 + int(year_str), month)
    return None


def parse_sprint_code(sprint_str):
    if not sprint_str or len(sprint_str) != 5:
        return None
    try:
        yy = int(sprint_str[:2])
        mm = int(sprint_str[2:4])
        if 1 <= mm <= 12:
            return (2000 + yy, mm)
    except ValueError:
        pass
    return None


def parse_filename_date(filename):
    dtg_match = re.search(r'\b(\d{6}[A-Z]?[A-Z]{3}\d{2})\b', filename)
    if dtg_match:
        result = parse_dtg(dtg_match.group(1))
        if result:
            return result
    sprint_match = re.search(r'/(\d{5})[/.]', filename)
    if sprint_match:
        result = parse_sprint_code(sprint_match.group(1))
        if result:
            return result
    date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
    if date_match:
        year, month = int(date_match.group(1)), int(date_match.group(2))
        if 2015 <= year <= 2035 and 1 <= month <= 12:
            return (year, month)
    return None


def load_jsonl(path):
    entries = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                entries.append(json.loads(line))
    return entries


def analyze_timeline(entries):
    timeline = Counter()
    dated = []
    for e in entries:
        date = parse_filename_date(e.get('path','')) or parse_filename_date(e.get('title',''))
        if date:
            timeline[date] += 1
            dated.append(e)
    return timeline, dated


def analyze_cec(entries):
    counts = Counter()
    labels = {}
    for e in entries:
        cec = e.get('cec')
        if cec:
            counts[cec] += 1
            labels.setdefault(cec, e.get('cec_label',''))
    return counts, labels


def analyze_threads(entries):
    threads = defaultdict(list)
    for e in entries:
        kw = e.get('keywords', [])
        cec = e.get('cec', '?')
        if kw:
            key = f"{cec}:{','.join(sorted(kw[:3]))}"
            threads[key].append(e)
    return {k: v for k, v in threads.items() if len(v) >= 3}


def analyze_folders(entries):
    folders = Counter()
    for e in entries:
        parts = e.get('path','').split('/')
        folder = parts[2] if len(parts) > 2 else 'root'
        folders[folder] += 1
    return folders


def cross_reference(cache, beads):
    by_title = {b.get('title','').strip().lower(): b for b in beads}
    cataloged, missing = [], []
    for e in cache:
        (cataloged if e.get('title','').strip().lower() in by_title else missing).append(e)
    return cataloged, missing


def generate_report(timeline, cec_counts, cec_labels, threads, folders,
                    cataloged, missing, total, total_beads, corpus_name):
    lines = [f"# Vault Analysis: {corpus_name}", "",
             f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "",
             "## Overview", ""]
    lines += [f"- **Total notes:** {total}",
              f"- **Beads catalog entries:** {total_beads}" if total_beads else "",
              f"- **Notes in catalog:** {len(cataloged)} ({len(cataloged)*100//total}%)" if cataloged or missing else "",
              f"- **Not yet cataloged:** {len(missing)} ({len(missing)*100//total}%)" if cataloged or missing else "",
              ""]
    lines = [l for l in lines if l is not None]

    lines += ["## 1. Timeline", ""]
    dated = sum(timeline.values())
    lines.append(f"**Date-parseable:** {dated} of {total}")
    lines += ["", "| Year-Month | Count |", "|------------|-------|"]
    for ym in sorted(timeline):
        lines.append(f"| {ym[0]}-{ym[1]:02d} | {timeline[ym]} |")
    lines.append("")

    lines += ["## 2. CEC Subject Distribution", ""]
    with_cec = sum(cec_counts.values())
    lines.append(f"**Classified:** {with_cec} of {total} ({with_cec*100//total}%)")
    lines += ["", "| CEC | Subject | Count | % |", "|-----|---------|-------|---|"]
    for cec, count in cec_counts.most_common():
        lines.append(f"| {cec} | {cec_labels.get(cec,'')} | {count} | {count*100/with_cec:.1f}% |")
    lines.append("")

    lines += ["## 3. Threads of Thought", ""]
    lines.append(f"**Significant threads (3+ notes):** {len(threads)}")
    lines.append("")
    for key, entries in sorted(threads.items(), key=lambda x: -len(x[1]))[:20]:
        cec, kw = (key.split(':',1) + [''])[:2]
        lines += [f"### {cec}: {kw}", f"**{len(entries)} notes**", ""]
        for e in entries[:5]:
            lines.append(f"- {e.get('title','Untitled')}")
        lines.append("")

    lines += ["## 4. Folder Structure", "", "| Folder | Count | % |", "|--------|-------|---|"]
    for folder, count in folders.most_common():
        lines.append(f"| {folder} | {count} | {count*100/total:.1f}% |")
    lines.append("")

    if missing:
        lines += ["## 5. Uncataloged Notes (sample)", ""]
        for e in missing[:20]:
            lines.append(f"- **{e.get('title','Untitled')}** (CEC: {e.get('cec','?')})")
        lines.append("")

    return '\n'.join(lines)


def main():
    p = argparse.ArgumentParser(description="Analyze a KSE-indexed vault")
    p.add_argument('--cache', '-c', required=True, help="Path to *-cache.jsonl")
    p.add_argument('--beads', '-b', default=None, help="Path to clawd .beads JSONL for cross-reference")
    p.add_argument('--output', '-o', default=None, help="Output .md path (default: stdout)")
    p.add_argument('--name', '-n', default=None, help="Corpus name for report title")
    args = p.parse_args()

    cache_path = Path(args.cache)
    corpus_name = args.name or cache_path.stem.replace('-cache','')

    print(f"Loading {cache_path}...", file=sys.stderr)
    cache = load_jsonl(cache_path)
    print(f"  {len(cache)} entries", file=sys.stderr)

    beads = []
    if args.beads and Path(args.beads).exists():
        print(f"Loading beads from {args.beads}...", file=sys.stderr)
        beads = load_jsonl(args.beads)
        print(f"  {len(beads)} beads", file=sys.stderr)

    timeline, _ = analyze_timeline(cache)
    cec_counts, cec_labels = analyze_cec(cache)
    threads = analyze_threads(cache)
    folders = analyze_folders(cache)
    cataloged, missing = cross_reference(cache, beads) if beads else ([], [])

    report = generate_report(timeline, cec_counts, cec_labels, threads, folders,
                             cataloged, missing, len(cache), len(beads), corpus_name)

    if args.output:
        Path(args.output).write_text(report, encoding='utf-8')
        print(f"Report: {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == '__main__':
    main()
