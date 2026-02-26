#!/usr/bin/env python3
"""
md-extract.py - Extract markdown file corpus to per-doc YAML + index

Handles Obsidian-style markdown with YAML frontmatter, wikilinks, tags.
Works on any flat/nested directory of .md files.

Usage:
  python3 scripts/md-extract.py --source /path/to/vault --name obsidian
  python3 scripts/md-extract.py --source /path/to/Daynotes.wiki --name daynotes

Per-doc YAML fields:
  title, path, source, tags, frontmatter, dir_path, sprint, quarter,
  links (wikilink targets -- thought thread graph), op (first 80 lines)
"""
import argparse, re, sys
from pathlib import Path

# DMOZ path → lang/region mapping
_DMOZ_LANG = {
    'german': 'de', 'french': 'fr', 'spanish': 'es', 'portuguese': 'pt',
    'italian': 'it', 'dutch': 'nl', 'russian': 'ru', 'japanese': 'ja',
    'chinese': 'zh', 'korean': 'ko', 'arabic': 'ar', 'polish': 'pl',
    'swedish': 'sv', 'danish': 'da', 'norwegian': 'no', 'finnish': 'fi',
    'czech': 'cs', 'hungarian': 'hu', 'turkish': 'tr', 'hebrew': 'he',
}
_DMOZ_REGION = {
    'uk': 'GB', 'united_kingdom': 'GB', 'germany': 'DE', 'france': 'FR',
    'spain': 'ES', 'italy': 'IT', 'japan': 'JP', 'china': 'CN',
    'russia': 'RU', 'brazil': 'BR', 'canada': 'CA', 'australia': 'AU',
    'netherlands': 'NL', 'sweden': 'SE', 'norway': 'NO', 'denmark': 'DK',
    'poland': 'PL', 'czech_republic': 'CZ', 'switzerland': 'CH',
}

def parse_lang_region(dir_path: str):
    """Extract lang/region from DMOZ-style directory paths."""
    parts = [p.lower().replace('-', '_') for p in Path(dir_path).parts]
    lang, region = None, None
    for i, part in enumerate(parts):
        if part == 'world' and i + 1 < len(parts):
            candidate = parts[i + 1]
            if candidate in _DMOZ_LANG:
                lang = _DMOZ_LANG[candidate]
            continue
        if region is None and part in _DMOZ_REGION:
            region = _DMOZ_REGION[part]
    return lang, region

try:
    import yaml
except ImportError:
    print('ERROR: pyyaml required.', file=sys.stderr); sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parent.parent
OP_MAX_LINES = 80

_SPRINT_RE = re.compile(r'\b(\d{4,5})\b')
_QUARTER_RE = re.compile(r'(FY\d{4}Q\d)', re.IGNORECASE)

def extract_wikilinks(text):
    """Return deduplicated list of [[link targets]] before stripping."""
    targets = re.findall(r'\[\[([^\]|#]+?)(?:\|[^\]]*)?\]\]', text)
    return list(dict.fromkeys(t.strip() for t in targets if t.strip()))

def strip_wikilinks(text):
    """Replace [[Target|Alias]] with Alias, [[Target]] with Target."""
    return re.sub(r'\[\[([^\]|]+)(?:\|([^\]]+))?\]\]',
                  lambda m: m.group(2) or m.group(1), text)

def extract_frontmatter(text):
    if not text.startswith('---'): return {}, text
    end = text.find('\n---', 3)
    if end == -1: return {}, text
    try: fm = yaml.safe_load(text[3:end]) or {}
    except Exception: fm = {}
    return fm, text[end+4:]

def extract_tags(text, fm):
    tags = list(fm.get('tags', fm.get('tag', [])) or [])
    inline = re.findall(r'(?<!\S)#([A-Za-z][A-Za-z0-9_/-]+)', text)
    return list(dict.fromkeys(tags + inline))

def parse_dir_meta(dir_path):
    """Extract sprint code and FY quarter from PARA directory path."""
    parts = Path(dir_path).parts
    sprint = None
    quarter = None
    for part in parts:
        if not quarter:
            m = _QUARTER_RE.search(part)
            if m: quarter = m.group(1).upper()
        if not sprint:
            m = _SPRINT_RE.search(part)
            if m and len(m.group(1)) in (4, 5):
                sprint = m.group(1)
    return sprint, quarter

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--source', required=True)
    p.add_argument('--name', required=True)
    p.add_argument('--out-dir', default=None)
    p.add_argument('--index', default=None)
    p.add_argument('--limit', '-n', type=int, default=0)
    args = p.parse_args()

    source = Path(args.source)
    out_dir = Path(args.out_dir) if args.out_dir else REPO_ROOT/'data'/args.name
    index_out = Path(args.index) if args.index else REPO_ROOT/'data'/f'{args.name}-index.yaml'
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(source.rglob('*.md'))
    if args.limit: files = files[:args.limit]
    print(f'{len(files)} .md files found in {source}', file=sys.stderr)

    entries = []
    for i, f in enumerate(files):
        try:
            text = f.read_text(encoding='utf-8', errors='replace')
            fm, body = extract_frontmatter(text)
            links = extract_wikilinks(body)
            body_clean = strip_wikilinks(body)
            tags = extract_tags(body_clean, fm)
            title = fm.get('title') or fm.get('Title') or f.stem
            op_lines = [l for l in body_clean.splitlines() if l.strip()][:OP_MAX_LINES]
            rel = f.relative_to(source)
            dir_path = str(rel.parent)
            sprint, quarter = parse_dir_meta(dir_path)
            lang, region = parse_lang_region(dir_path)
            # Daynotes: sprint code may be the filename itself (e.g. 10082.md)
            if not sprint:
                stem_m = _SPRINT_RE.search(f.stem)
                if stem_m and len(stem_m.group(1)) == 5:
                    sprint = stem_m.group(1)
            slug = str(rel).replace('/', '_').replace(' ', '-')[:80]
            out_path = out_dir / f'{slug}.yaml'
            rel_out = f'data/{args.name}/{slug}.yaml'
            doc = {
                'title': title,
                'path': str(rel),
                'source': args.name,
                'tags': tags,
                'frontmatter': fm,
                'dir_path': dir_path,
                'sprint': sprint,
                'quarter': quarter,
                'lang': lang,
                'region': region,
                'links': links,
                'op': '\n'.join(op_lines),
            }
            with open(out_path, 'w', encoding='utf-8') as fh:
                yaml.dump(doc, fh, allow_unicode=True, default_flow_style=False, sort_keys=False)
            entries.append({'path': rel_out, 'title': title})
            if (i+1) % 500 == 0: print(f'  {i+1}/{len(files)}', file=sys.stderr)
        except Exception as e:
            print(f'  WARN {f}: {e}', file=sys.stderr)

    from collections import defaultdict
    by_dir = defaultdict(list)
    for e in entries:
        parts = Path(e['path']).parts
        key = parts[2] if len(parts) > 3 else 'root'
        by_dir[key].append({'path': e['path'], 'title': e['title']})
    sections = [{'name': f'{args.name}-{k}', 'gln': '', 'fgid': [],
                 'files': v, 'children': []} for k, v in sorted(by_dir.items())]
    index = {'name': args.name, 'source': args.name, 'sections': sections}
    with open(index_out, 'w') as fh:
        yaml.dump(index, fh, allow_unicode=True, default_flow_style=False)
    print(f'Done: {len(entries)} docs. Index: {index_out}', file=sys.stderr)

if __name__ == '__main__': main()
