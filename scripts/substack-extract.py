#!/usr/bin/env python3
"""
substack-extract.py - Extract Substack posts to per-post YAML + index

Fetches RSS to discover posts, then scrapes each public post page
for full content. Outputs same YAML + index format as od-extract-threads.py.

Usage:
  python3 scripts/substack-extract.py --url https://macrobius.substack.com
  python3 scripts/substack-extract.py --url https://macrobius.substack.com --limit 10
"""

import argparse
import re
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required. pip3 install pyyaml", file=sys.stderr)
    sys.exit(1)

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
OP_MAX_LINES = 80
FETCH_DELAY = 1.0   # seconds between page fetches


def fetch_url(url, timeout=15):
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (compatible; KitsapSearch/1.0)'
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode('utf-8', errors='replace')


def fetch_rss(base_url):
    """Fetch RSS and return list of (title, url, pubdate) tuples."""
    feed_url = base_url.rstrip('/') + '/feed'
    print(f"Fetching RSS: {feed_url}", file=sys.stderr)
    xml_text = fetch_url(feed_url)
    root = ET.fromstring(xml_text)
    posts = []
    for item in root.findall('.//item'):
        title_el = item.find('title')
        link_el  = item.find('link')
        date_el  = item.find('pubDate')
        title   = title_el.text.strip() if title_el is not None else ''
        link    = link_el.text.strip()  if link_el  is not None else ''
        pubdate = date_el.text.strip()  if date_el  is not None else ''
        if title and link:
            posts.append({'title': title, 'url': link, 'pubdate': pubdate})
    return posts


def clean_html(html_text):
    """Strip HTML tags and normalize whitespace."""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>',  '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&amp;',  '&',  text)
    text = re.sub(r'&lt;',   '<',  text)
    text = re.sub(r'&gt;',   '>',  text)
    text = re.sub(r'&nbsp;', ' ',  text)
    text = re.sub(r'&#\d+;', '',   text)
    text = re.sub(r'[ \t]+', ' ',  text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_post_content(html, max_lines=OP_MAX_LINES):
    """Extract article body paragraphs from a Substack post page."""
    # Pull <p> tags — Substack renders content in paragraphs
    paras = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL)
    lines = []
    for p in paras:
        text = clean_html(p).strip()
        # Skip nav/subscribe noise
        if not text or len(text) < 20:
            continue
        if any(x in text.lower() for x in [
            'subscribe', 'sign in', 'share this post', 'leave a comment',
            'read more', 'thanks for reading', '@context', 'https://'
        ]):
            continue
        lines.append(text)
        if len(lines) >= max_lines:
            break
    return lines


def parse_pubdate(pubdate_str):
    """Parse RSS pubDate to ISO date string."""
    for fmt in ['%a, %d %b %Y %H:%M:%S %z', '%a, %d %b %Y %H:%M:%S %Z']:
        try:
            return datetime.strptime(pubdate_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return pubdate_str[:10] if pubdate_str else ''


def slug_from_url(url):
    """Extract slug from Substack URL for filename."""
    m = re.search(r'/p/([^/?#]+)', url)
    return m.group(1) if m else re.sub(r'[^a-z0-9-]', '-', url.lower())[-40:]


def write_post_yaml(post, content_lines, output_dir):
    slug = slug_from_url(post['url'])
    fname = f"post-{slug[:60]}.yaml"
    fpath = output_dir / fname

    doc = {
        'title':   post['title'],
        'url':     post['url'],
        'date':    parse_pubdate(post['pubdate']),
        'source':  'substack',
        'op':      '\n'.join(content_lines),
    }
    with open(fpath, 'w', encoding='utf-8') as f:
        yaml.dump(doc, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
    return fname


def write_index(posts_meta, output_dir, index_path):
    from collections import defaultdict
    by_year = defaultdict(list)
    for m in posts_meta:
        year = m['date'][:4] if m.get('date') else 'unknown'
        by_year[year].append(m)

    sections = []
    for year in sorted(by_year.keys()):
        files = [{'path': str(Path('data') / output_dir.name / m['fname']),
                  'title': m['title']} for m in by_year[year]]
        sections.append({
            'name': f"{output_dir.name}-{year}",
            'gln': '', 'fgid': [],
            'files': files, 'children': [],
        })

    index = {
        'name': output_dir.name,
        'source': 'substack',
        'sections': sections,
    }
    with open(index_path, 'w', encoding='utf-8') as f:
        yaml.dump(index, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
    print(f"Index: {index_path} ({len(posts_meta)} posts)", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Extract Substack posts to YAML")
    parser.add_argument('--url', required=True, help='Substack base URL')
    parser.add_argument('--output', '-o', help='Output directory (default: data/<name>)')
    parser.add_argument('--index',  '-i', help='Index YAML path')
    parser.add_argument('--limit',  '-n', type=int, default=0)
    args = parser.parse_args()

    # Derive name from URL
    name = re.sub(r'https?://', '', args.url).split('.')[0].replace('/', '-')
    output_dir  = Path(args.output) if args.output else REPO_ROOT / 'data' / name
    index_path  = Path(args.index)  if args.index  else REPO_ROOT / 'data' / f'{name}-index.yaml'
    output_dir.mkdir(parents=True, exist_ok=True)

    posts = fetch_rss(args.url)
    if args.limit:
        posts = posts[:args.limit]
    print(f"{len(posts)} posts to process", file=sys.stderr)

    posts_meta = []
    for i, post in enumerate(posts):
        print(f"  [{i+1}/{len(posts)}] {post['title'][:60]}", file=sys.stderr)
        try:
            html = fetch_url(post['url'])
            content_lines = extract_post_content(html)
        except Exception as e:
            print(f"    WARN: fetch failed: {e}", file=sys.stderr)
            content_lines = []

        fname = write_post_yaml(post, content_lines, output_dir)
        posts_meta.append({
            'title': post['title'],
            'date':  parse_pubdate(post['pubdate']),
            'fname': fname,
        })
        if i < len(posts) - 1:
            time.sleep(FETCH_DELAY)

    write_index(posts_meta, output_dir, index_path)
    print(f"Done. {len(posts_meta)} post YAMLs.", file=sys.stderr)


if __name__ == '__main__':
    main()
