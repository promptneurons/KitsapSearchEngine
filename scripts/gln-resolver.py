#!/usr/bin/env python3
"""
gln-resolver.py - Triple-Axis GLN Classifier

Predicts the closest Generalized Luhmann Number (GLN) for a document
using three scoring axes:
  1. Archetype  (50%) - filename prefix → candidate GLN subtrees
  2. FGID       (30%) - content keywords → department codes → node affinity
  3. CEC        (20%) - content → Cutter Expansive Classification → subject affinity

Data sources (loaded at startup, no network):
  - llms-N200-index.yaml  — the GLN tree
  - data/cec-hierarchy.ttl — CEC main classes + divisions
  - data/jdn-ontology.ttl  — JDN classes + CEC↔JDN crosswalk

Usage:
  python3 scripts/gln-resolver.py projects/BUSINESS-PLAN.md
  python3 scripts/gln-resolver.py --title "MQL-015-evolanzoltan" --content "lead scoring for substack author"
"""

import argparse
import os
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required. Install with: pip3 install pyyaml", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
INDEX_PATH = REPO_ROOT / "llms-N200-index.yaml"
CEC_TTL_PATH = REPO_ROOT / "data" / "cec-hierarchy.ttl"
JDN_TTL_PATH = REPO_ROOT / "data" / "jdn-ontology.ttl"

AXIS_WEIGHTS = {"archetype": 0.50, "fgid": 0.30, "cec": 0.20}
CONTENT_READ_LINES = 80  # lines of content to read for classification
TOP_CANDIDATES = 5

# ---------------------------------------------------------------------------
# Axis 1: Archetype — prefix → candidate GLN subtrees
# ---------------------------------------------------------------------------

# Maps filename prefix patterns to GLN subtree prefixes they're most likely in.
# Built from inspecting the llms-N200-index.yaml tree.
PREFIX_TO_GLNS = {
    "PLAY-":   ["a9c9g9p", "alc9g9b4", "alc9g9b5"],
    "SPEC-":   ["a9c9g9z"],
    "INC-":    ["a1c9d", "a9c9g9d", "a1c9e1"],
    "MQL-":    ["a1c9g5", "a1c9g5d"],
    "STRAT-":  ["a1c2c"],
    "MKT-":    ["a1c4a"],
    "TRAIN-":  ["a1c9c1", "alc9g9b9"],
    "TTP-":    ["a9c9g9z8"],
    "ADR-":    ["a1c9g2"],
    "AUDIT-":  ["a1c2", "a1c2a", "alc9g9b6"],
    "POL-":    ["alc9g9b3"],
    "SOP-":    ["a1c9b", "alc9g9b5"],
    "PROC-":   ["a1c9b", "alc9g9b5"],
    "DEPT-":   ["a1c9g5c"],
    "VND-":    ["a1c9g5f"],
    "ORG-":    ["a1c9g5"],
    "EPIC-":   ["a1c9g8"],
    "BLOG-":   ["a1c3", "a1c6a", "a1c9g3"],
    "LAB-":    ["a1c9c1", "alc9g9b9"],
    "GUIDE-":  ["a1c9g9c"],
    "BUG-":    ["a1c9g4"],
    "DEMO-":   ["a1c9g7"],
    "COMP-":   ["a1c9g5", "a1c9g6"],
    "TOP-":    ["a1c9g5e"],
    "OPORD-":  ["a9c9g9m"],
    "PIR-":    ["a9c9g9n"],
    "CAMP-":   ["a1c9g5a"],
    "COUNTRY-":["a1c9g5b"],
    "CASEFILE-":["a1c9g5"],
    "RUNBOOK-": ["a1c9a"],
    "FLT-":    ["a1c9b"],
    "SALES-":  ["a1c2b"],
    "SKILL-":  ["a1c9j", "alc9j1"],
    "DQ-":     ["a1c7o1"],
    "SR-":     ["a1c2a1"],
    "ARCH-":   ["a1c9g2"],
    "SQL-":    ["a1c9g5"],
    "USER-":   ["a1c9g5"],
    "PERSON-": ["a1c9g5"],
    "F100-":   ["a1c9g5"],
    "N100-":   ["a1c9g5"],
    "HISTORY-":["a1c9g5"],
    "FY":      ["a1c9g5"],
    "TY":      ["a1c9g5"],
    "VARIANCE-":["a9c9g9z9a"],
    "QAP-":    ["alc9g9b2"],
    "FINDING-":["alc9g9b7"],
    "CAPA-":   ["alc9g9b7"],
    "CHANGE-": ["alc9g9b7"],
    "TEST-":   ["a9c9g9z7"],
    "UAT-":    ["a9c9g9z9"],
}

# Directory name → GLN mappings for path-based scoring
DIR_TO_GLNS = {
    "audits":           ["a1c2"],
    "blog":             ["a1c3"],
    "marketing":        ["a1c4a", "a9c9g9g"],
    "sales":            ["a1c2b"],
    "strategy":         ["a1c2c"],
    "config":           ["a1c5"],
    "data":             ["a1c7"],
    "finance":          ["a1c7o", "a1c9g9a"],
    "librarian-context":["a1c7q"],
    "opfor-dataset":    ["a1c7r"],
    "profiles":         ["a1c7s"],
    "salo":             ["a1c7t", "a1c7ts", "a1c7u"],
    "dlc":              ["a1c8"],
    "docs":             ["a1c9"],
    "field-tech":       ["a1c9a"],
    "procedures":       ["a1c9b"],
    "drafts":           ["a1c9a"],
    "flights":          ["a1c9b"],
    "governance-review":["a1c9c", "a1c9g9b"],
    "incidents":        ["a1c9d", "a9c9g9d"],
    "memory":           ["a1c9e"],
    "packages":         ["a1c9f"],
    "projects":         ["a1c9g"],
    "casefiles":        ["a1c9g5"],
    "leads":            ["a1c9g5d"],
    "departments":      ["a1c9g5c"],
    "topics":           ["a1c9g5e"],
    "vendors":          ["a1c9g5f"],
    "epics":            ["a1c9g8"],
    "playbooks":        ["a9c9g9p"],
    "specs":            ["a9c9g9z"],
    "ttps":             ["a9c9g9z8"],
    "scripts":          ["a1c9i"],
    "skills":           ["a1c9j"],
    "templates":        ["a1c9m"],
    "tools":            ["a1c9n"],
    "repos":            ["a1c9h"],
    "ontology":         ["a9c9g9k"],
    "architecture":     ["a1c9g2"],
    "analysis":         ["a1c9g1"],
    "bugs":             ["a1c9g4"],
    "demos":            ["a1c9g7"],
    "extensions":       ["a1c9g9"],
    "migration":        ["a9c9g9i"],
    "onboarding":       ["a9c9g9j"],
    "roadmaps":         ["a9c9g9v"],
    "soc2":             ["a9c9g9y"],
    "sprints":          ["a9c9g9z2"],
    "tests":            ["a9c9g9z7"],
    "uat":              ["a9c9g9z9"],
    "wireframes":       ["a9c9g9z9c"],
    "plans":            ["a9c9g9o"],
    "roles":            ["a9c9g9w"],
    "requirements":     ["a9c9g9t"],
    "campaigns":        ["a1c9g5a"],
    "countries":        ["a1c9g5b"],
}

# ---------------------------------------------------------------------------
# Axis 2: FGID — keyword → department signals
# ---------------------------------------------------------------------------

FGID_KEYWORDS = {
    "LD": [
        "strategy", "strategic", "policy", "decision", "governance", "budget",
        "leadership", "vision", "objective", "goal", "direction", "executive",
        "approve", "approval", "authority", "oversight", "kpi", "oplan",
    ],
    "MG": [
        "quality", "sprint", "capa", "operations", "operational", "process",
        "audit", "incident", "finding", "variance", "compliance", "gate",
        "review", "checklist", "sop", "procedure", "standard", "nqa",
        "heartbeat", "sync", "status", "phase", "iteration",
    ],
    "SF": [
        "azure", "ssh", "pipeline", "infra", "infrastructure", "deploy",
        "cicd", "ci/cd", "node", "container", "docker", "kubernetes",
        "linode", "virtuoso", "sparql", "server", "vm", "disk", "backup",
        "restore", "dns", "ssl", "certificate", "firewall", "nginx",
        "bicep", "terraform", "ansible", "devops", "github actions",
    ],
    "MK": [
        "campaign", "content", "social", "marketing", "outreach", "blog",
        "influencer", "brand", "messaging", "audience", "subscriber",
        "newsletter", "mailchimp", "substack", "seo", "engagement",
        "pitch", "deck", "icp", "ideal customer",
    ],
    "LG": [
        "lead", "prospect", "mql", "pipeline", "qualification", "legal",
        "compliance", "contract", "agreement", "sales cycle", "discovery",
        "outbound", "inbound", "funnel", "conversion",
    ],
    "CF": [
        "product", "ux", "demo", "skill", "craft", "roadmap", "feature",
        "capability", "interface", "design", "prototype", "wireframe",
        "plugin", "extension", "frankentui", "viewer", "dashboard",
        "onboarding", "customer", "fulfillment",
    ],
    "FN": [
        "finance", "financial", "tax", "accounting", "budget", "invoice",
        "bank", "gnucash", "ledger", "receivable", "payable", "expense",
        "revenue", "bookkeeping", "1040", "irs", "w-2", "benefits",
        "insurance", "mortgage", "escrow",
    ],
}

# Security domain FGID codes (not scored by keywords, matched by document context)
SECURITY_FGIDS = {"N100", "N200", "P100", "F100", "R100", "R200", "R300", "Z100", "LN"}

# ---------------------------------------------------------------------------
# Axis 3: CEC — content → subject classification
# ---------------------------------------------------------------------------

# CEC main class labels and associated keywords for content matching.
# Derived from cec-hierarchy.ttl labels.
CEC_CLASSES = {
    "A": {
        "label": "Works of Reference and General",
        "keywords": ["reference", "general", "index", "catalog", "bibliography",
                     "encyclopedia", "dictionary", "meta", "registry"],
    },
    "B": {
        "label": "Philosophy, Paedeia, Logic, Religion",
        "keywords": ["philosophy", "metaphysics", "ethics", "logic", "ontology",
                     "epistemology", "phenomenology", "existentialism", "heidegger",
                     "plato", "aristotle", "nietzsche", "socrates", "idealism",
                     "rationalism", "empiricism", "stoicism"],
    },
    "C": {
        "label": "Christianity and Judaism",
        "keywords": ["christianity", "orthodox", "church", "liturgy", "theology",
                     "judaism", "bible", "scripture", "parish", "diocese",
                     "patriarch", "saint", "monastery", "icon"],
    },
    "D": {
        "label": "Ecclesiastical History",
        "keywords": ["ecclesiastical", "church history", "reformation", "council",
                     "papal", "crusade", "schism"],
    },
    "E": {
        "label": "Biography",
        "keywords": ["biography", "career", "life", "profile", "memoir",
                     "autobiography", "person"],
    },
    "F": {
        "label": "History",
        "keywords": ["history", "historical", "ancient", "medieval", "modern",
                     "civilization", "empire", "dynasty", "revolution", "war",
                     "colonial", "independence"],
    },
    "G": {
        "label": "Geography",
        "keywords": ["geography", "region", "country", "territory", "map",
                     "border", "continent", "climate", "landscape"],
    },
    "H": {
        "label": "Demotics, Society, Social Science",
        "keywords": ["society", "social", "sociology", "demographics", "immigration",
                     "race", "gender", "culture", "community", "population",
                     "urbanization", "inequality", "class"],
    },
    "J": {
        "label": "Politics, Governance",
        "keywords": ["politics", "political", "governance", "government", "democracy",
                     "sovereignty", "geopolitics", "foreign policy", "diplomacy",
                     "election", "parliament", "legislation", "constitution"],
    },
    "K": {
        "label": "Legislation, Law, Women, Societies",
        "keywords": ["law", "legal", "legislation", "regulation", "court",
                     "justice", "statute", "contract", "rights", "corporation",
                     "organization", "association"],
    },
    "L": {
        "label": "Science and Arts",
        "keywords": ["science", "technology", "engineering", "mathematics", "physics",
                     "computer", "algorithm", "machine learning", "artificial intelligence",
                     "data", "software", "programming", "automation", "research",
                     "experiment", "laboratory", "formal methods", "semantic web"],
    },
    "M": {
        "label": "Natural History, Biology",
        "keywords": ["biology", "natural history", "evolution", "ecology",
                     "genetics", "organism", "species", "biodiversity"],
    },
    "N": {
        "label": "Botany",
        "keywords": ["botany", "plant", "flora", "horticulture", "garden",
                     "agriculture", "crop"],
    },
    "O": {
        "label": "Zoology, Anthropology, Ethnology",
        "keywords": ["zoology", "anthropology", "ethnology", "animal",
                     "fauna", "primatology", "ethnography"],
    },
    "Q": {
        "label": "Medicine, Health",
        "keywords": ["medicine", "health", "medical", "clinical", "disease",
                     "therapy", "diagnosis", "hospital", "pharmaceutical"],
    },
    "R": {
        "label": "Useful Arts, Technics",
        "keywords": ["technical", "engineering", "infrastructure", "devops",
                     "deployment", "pipeline", "manufacturing", "industry",
                     "craft", "trade", "mechanic", "utility", "tool"],
    },
    "U": {
        "label": "Art of War",
        "keywords": ["military", "warfare", "strategy", "tactics", "defense",
                     "army", "navy", "intelligence", "opfor", "ujtl",
                     "capability", "operations", "command"],
    },
    "V": {
        "label": "Recreative, Sports, Games",
        "keywords": ["sport", "game", "recreation", "leisure", "fitness",
                     "competition", "tournament"],
    },
    "W": {
        "label": "Fine Arts",
        "keywords": ["art", "painting", "sculpture", "architecture", "music",
                     "aesthetic", "gallery", "museum"],
    },
    "X": {
        "label": "English Language and Literature",
        "keywords": ["english", "literature", "novel", "poetry", "prose",
                     "essay", "fiction", "drama", "literary criticism"],
    },
    "Y": {
        "label": "Language, Philology (non-English)",
        "keywords": ["language", "philology", "linguistics", "grammar",
                     "translation", "foreign language", "classical language"],
    },
    "Z": {
        "label": "Literature (non-English)",
        "keywords": ["foreign literature", "translation", "comparative literature",
                     "world literature"],
    },
}

# CEC → JDN crosswalk (from jdn-ontology.ttl)
CEC_TO_JDN = {
    "A": (45, "General Reference"),
    "B": (27, "Metaphysics"),
    "B3": (24, "Ethics"),
    "B4": (25, "Logic"),
    "C": (14, "Religion"),
    "D": (5, "Ecclesiastical History"),
    "F": (1, "Ancient History"),
    "F1": (2, "Modern History - Foreign"),
    "F2": (2, "Modern History - Foreign"),
    "H": (16, "Politics"),
    "J": (16, "Politics"),
    "K": (12, "Law"),
    "L": (6, "Natural Philosophy"),
    "Y": (40, "Languages"),
}


# ---------------------------------------------------------------------------
# TTL Parser — lightweight regex-based
# ---------------------------------------------------------------------------

def parse_cec_ttl(path):
    """Parse cec-hierarchy.ttl for concept labels and broader relations."""
    concepts = {}
    if not path.exists():
        return concepts

    text = path.read_text(encoding="utf-8")

    # Match concept blocks: cec:XX a skos:Concept ; skos:prefLabel "..."@en ;
    for m in re.finditer(
        r'cec:(\S+)\s+a\s+skos:Concept\s*;'
        r'.*?skos:prefLabel\s+"([^"]+)"',
        text, re.DOTALL
    ):
        code, label = m.group(1), m.group(2)
        concepts[code] = {"label": label, "broader": None}

    # Match broader relations
    for m in re.finditer(r'cec:(\S+)\s+.*?skos:broader\s+cec:(\S+)', text, re.DOTALL):
        child, parent = m.group(1), m.group(2)
        if child in concepts:
            concepts[child]["broader"] = parent

    return concepts


def parse_jdn_ttl(path):
    """Parse jdn-ontology.ttl for JDN labels and CEC crosswalk."""
    jdn_classes = {}
    crosswalk = {}
    if not path.exists():
        return jdn_classes, crosswalk

    text = path.read_text(encoding="utf-8")

    # JDN classes
    for m in re.finditer(
        r'jdn:(\d+)\s+a\s+skos:Concept\s*;\s*rdfs:label\s+"([^"]+)"',
        text
    ):
        jdn_classes[int(m.group(1))] = m.group(2)

    # CEC → JDN crosswalk
    for m in re.finditer(
        r'cec:(\S+)\s+skos:closeMatch\s+jdn:(\d+)',
        text
    ):
        crosswalk[m.group(1)] = int(m.group(2))

    return jdn_classes, crosswalk


# ---------------------------------------------------------------------------
# GLN Tree Loader
# ---------------------------------------------------------------------------

class GLNNode:
    """A node in the GLN tree."""
    __slots__ = ("gln", "name", "level", "fgid", "file_titles", "file_paths",
                 "children", "parent_path")

    def __init__(self, gln, name, level, fgid, file_titles, file_paths, parent_path=""):
        self.gln = gln
        self.name = name
        self.level = level
        self.fgid = fgid
        self.file_titles = file_titles
        self.file_paths = file_paths
        self.children = []
        self.parent_path = parent_path

    @property
    def full_path(self):
        if self.parent_path:
            return f"{self.parent_path}/{self.name}"
        return self.name

    def __repr__(self):
        return f"GLNNode({self.gln}, {self.name}, L{self.level})"


def load_gln_tree(yaml_path):
    """Load llms-N200-index.yaml into a flat list of GLNNode objects."""
    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    nodes = []

    def walk(sections, parent_path=""):
        for s in sections:
            fgid = s.get("fgid", [])
            if isinstance(fgid, str):
                fgid = [fgid]
            if not isinstance(fgid, list):
                fgid = []

            files = s.get("files", [])
            titles = [f.get("title", "") for f in files]
            paths = [f.get("path", "") for f in files]

            node = GLNNode(
                gln=s.get("gln", ""),
                name=s.get("name", ""),
                level=s.get("level", 0),
                fgid=fgid,
                file_titles=titles,
                file_paths=paths,
                parent_path=parent_path,
            )
            nodes.append(node)

            for child in s.get("children", []):
                child_sections = [child] if isinstance(child, dict) else []
                walk(child_sections, node.full_path)

            # Also walk the children list directly
            walk(s.get("children", []), node.full_path)

    # The top-level is data["sections"]
    walk(data.get("sections", []))

    # Deduplicate (walk visits children twice in the naive approach above)
    seen = set()
    deduped = []
    for n in nodes:
        key = (n.gln, n.name, n.level)
        if key not in seen:
            seen.add(key)
            deduped.append(n)

    return deduped


# ---------------------------------------------------------------------------
# Scoring Functions
# ---------------------------------------------------------------------------

def score_archetype(title, file_path, nodes):
    """Axis 1: Score each GLN node by filename prefix and path component match."""
    scores = {}
    title_upper = title.upper()

    # Find matching prefix
    matched_prefix = None
    matched_glns = set()
    for prefix, glns in PREFIX_TO_GLNS.items():
        if title_upper.startswith(prefix.upper()):
            matched_prefix = prefix
            matched_glns = set(glns)
            break

    # Find matching directory components
    path_parts = set()
    if file_path:
        path_parts = set(Path(file_path).parts)
    matched_dirs = set()
    dir_glns = set()
    for dirname, glns in DIR_TO_GLNS.items():
        if dirname in path_parts:
            matched_dirs.add(dirname)
            dir_glns.update(glns)

    for node in nodes:
        score = 0.0

        # Prefix match: does this node's GLN match any of the mapped GLN prefixes?
        if matched_glns:
            for mg in matched_glns:
                if node.gln == mg:
                    # Exact match — strongest signal
                    score = max(score, 1.0)
                elif node.gln.startswith(mg) and len(node.gln) <= len(mg) + 2:
                    # Direct child of mapped node (e.g., mapped=a1c9g5, node=a1c9g5d)
                    score = max(score, 0.85)
                elif mg.startswith(node.gln):
                    # Node is an ancestor of mapped target — moderate signal
                    score = max(score, 0.4)
            # Partial prefix overlap for siblings
            if score == 0.0:
                for mg in matched_glns:
                    common = os.path.commonprefix([node.gln, mg])
                    if len(common) >= 4:
                        score = max(score, 0.2 * len(common) / max(len(node.gln), len(mg)))

        # Directory match — prefer exact over ancestor/descendant
        if dir_glns:
            for dg in dir_glns:
                if node.gln == dg:
                    score = max(score, 0.8)
                elif node.gln.startswith(dg) and len(node.gln) <= len(dg) + 2:
                    score = max(score, 0.7)
                elif dg.startswith(node.gln) and len(dg) <= len(node.gln) + 2:
                    score = max(score, 0.5)
                # Don't match distant ancestors/descendants

        # Name match: does the node name appear in the path?
        if node.name and node.name in path_parts:
            score = max(score, 0.6)

        # File title similarity: does this node already contain files with similar titles?
        if title:
            for ft in node.file_titles:
                if ft and title_upper.startswith(ft[:4].upper()):
                    score = max(score, 0.5)
                    break

        scores[node.gln] = score

    return scores, matched_prefix, matched_glns, matched_dirs


def score_fgid(content, nodes):
    """Axis 2: Score each GLN node by FGID keyword affinity."""
    content_lower = content.lower()

    # Detect department signals in content
    dept_scores = {}
    for dept, keywords in FGID_KEYWORDS.items():
        hits = sum(1 for kw in keywords if kw in content_lower)
        if hits > 0:
            dept_scores[dept] = min(hits / 3.0, 1.0)  # normalize: 3+ hits = 1.0

    detected_fgids = sorted(dept_scores.keys(), key=lambda d: dept_scores[d], reverse=True)

    # Score each node by overlap between detected departments and node's FGID
    scores = {}
    for node in nodes:
        node_depts = set(node.fgid) - SECURITY_FGIDS  # only compare dept codes
        if not node_depts and not dept_scores:
            scores[node.gln] = 0.3  # neutral — no dept tags, no dept signals
            continue
        if not node_depts:
            # Node has no department tags (e.g., P100-only nodes like blog/)
            # Give a small baseline so they aren't completely outscored
            scores[node.gln] = 0.15
            continue
        if not dept_scores:
            scores[node.gln] = 0.1
            continue

        # Weighted Jaccard: intersection weighted by detection strength
        overlap = 0.0
        for d in node_depts:
            if d in dept_scores:
                overlap += dept_scores[d]

        score = overlap / len(node_depts)
        scores[node.gln] = min(score, 1.0)

    return scores, detected_fgids


def score_cec(content, title):
    """Axis 3: Score content against CEC main classes, return class scores."""
    content_lower = (content + " " + title).lower()

    class_scores = {}
    for code, info in CEC_CLASSES.items():
        hits = sum(1 for kw in info["keywords"] if kw in content_lower)
        if hits > 0:
            class_scores[code] = min(hits / 2.0, 1.0)  # normalize: 2+ hits = 1.0

    if not class_scores:
        return {}, None, None, None

    best_cec = max(class_scores, key=class_scores.get)
    best_label = CEC_CLASSES[best_cec]["label"]

    # JDN crosswalk
    jdn_num, jdn_label = CEC_TO_JDN.get(best_cec, (None, None))

    return class_scores, best_cec, best_label, (jdn_num, jdn_label)


def score_cec_affinity(cec_class_scores, nodes):
    """Map CEC class scores to per-node affinity based on node name/content heuristics."""
    # Build a rough mapping: which CEC classes are associated with which node names
    NAME_CEC_AFFINITY = {
        "audits":     ["A", "J"],
        "blog":       ["X", "A"],
        "marketing":  ["K", "H"],
        "sales":      ["K"],
        "strategy":   ["J", "U"],
        "data":       ["A", "L"],
        "finance":    ["K"],
        "librarian-context": ["A", "L"],
        "opfor-dataset": ["U"],
        "profiles":   ["E"],
        "salo":       ["H", "F", "B"],
        "docs":       ["A", "R"],
        "governance-review": ["J", "K"],
        "incidents":  ["R", "L"],
        "memory":     ["A"],
        "projects":   ["R", "L"],
        "casefiles":  ["E", "K", "H"],
        "leads":      ["E", "K"],
        "departments":["K"],
        "topics":     ["L", "R"],
        "vendors":    ["K", "R"],
        "epics":      ["R"],
        "playbooks":  ["R", "J"],
        "specs":      ["R", "L"],
        "ttps":       ["R", "U"],
        "skills":     ["L", "R"],
        "ontology":   ["B", "L"],
        "architecture":["R", "L"],
        "analysis":   ["L"],
        "bugs":       ["R", "L"],
        "demos":      ["R"],
        "migration":  ["R"],
        "soc2":       ["J", "K"],
        "tests":      ["R", "L"],
        "requirements":["R"],
        "config":     ["R", "A"],
        "orthodox":   ["C", "D"],
        "church":     ["C", "D"],
    }

    scores = {}
    for node in nodes:
        affinities = NAME_CEC_AFFINITY.get(node.name, [])
        if not affinities or not cec_class_scores:
            scores[node.gln] = 0.0
            continue

        score = 0.0
        for cec_code in affinities:
            if cec_code in cec_class_scores:
                score = max(score, cec_class_scores[cec_code])

        scores[node.gln] = score

    return scores


# ---------------------------------------------------------------------------
# Composite Resolver
# ---------------------------------------------------------------------------

def resolve_gln(title, content, file_path, nodes):
    """Run triple-axis scoring and return ranked candidates."""

    # Axis 1: Archetype
    arch_scores, matched_prefix, matched_glns, matched_dirs = score_archetype(
        title, file_path, nodes
    )

    # Axis 2: FGID
    fgid_scores, detected_fgids = score_fgid(content, nodes)

    # Axis 3: CEC
    cec_class_scores, best_cec, best_cec_label, jdn_info = score_cec(content, title)
    cec_node_scores = score_cec_affinity(cec_class_scores, nodes)

    # Build path containment map: if the file path starts with a node's directory,
    # that's a direct structural match (strongest possible signal)
    path_containment = {}
    if file_path:
        fp_normalized = file_path.replace("\\", "/")
        for node in nodes:
            # Check if any of the node's existing files share the same parent directory
            for np in node.file_paths:
                np_dir = str(Path(np).parent)
                if fp_normalized.startswith(np_dir + "/") or fp_normalized == np:
                    path_containment[node.gln] = 1.0
                    break
            # Also check if file_path starts with node's full_path as directory prefix
            node_dir = node.full_path.replace("\\", "/")
            if fp_normalized.startswith(node_dir + "/") or node_dir.endswith("/" + Path(fp_normalized).parent.name):
                if node.gln not in path_containment:
                    path_containment[node.gln] = 0.85

    # Composite
    candidates = []
    for node in nodes:
        a = arch_scores.get(node.gln, 0.0)
        f = fgid_scores.get(node.gln, 0.0)
        c = cec_node_scores.get(node.gln, 0.0)

        # Path containment bonus — if the file is literally in this node's directory,
        # that's the ground truth signal and should dominate
        containment = path_containment.get(node.gln, 0.0)
        if containment > 0:
            a = max(a, containment)

        composite = (
            AXIS_WEIGHTS["archetype"] * a
            + AXIS_WEIGHTS["fgid"] * f
            + AXIS_WEIGHTS["cec"] * c
        )

        # Depth bonus: prefer more specific nodes (deeper = better) on ties
        depth_bonus = node.level * 0.01
        composite += depth_bonus

        candidates.append({
            "gln": node.gln,
            "name": node.name,
            "level": node.level,
            "path": node.full_path,
            "score": round(composite, 4),
            "archetype_score": round(a, 4),
            "fgid_score": round(f, 4),
            "cec_score": round(c, 4),
        })

    # Sort by composite score descending, then by level descending (deeper preferred)
    candidates.sort(key=lambda c: (c["score"], c["level"]), reverse=True)

    # Build result
    top = candidates[0] if candidates else None

    result = {
        "prediction": {
            "gln": top["gln"] if top else None,
            "name": top["name"] if top else None,
            "path": top["path"] if top else None,
            "confidence": top["score"] if top else 0.0,
        },
        "axes": {
            "archetype": {
                "prefix": matched_prefix,
                "matched_subtrees": sorted(matched_glns) if matched_glns else [],
                "matched_dirs": sorted(matched_dirs) if matched_dirs else [],
                "score": top["archetype_score"] if top else 0.0,
            },
            "fgid": {
                "detected": detected_fgids[:4],
                "node_fgid": None,  # filled below
                "score": top["fgid_score"] if top else 0.0,
            },
            "cec": {
                "class": best_cec,
                "label": best_cec_label,
                "jdn": jdn_info[0] if jdn_info else None,
                "jdn_label": jdn_info[1] if jdn_info else None,
                "score": top["cec_score"] if top else 0.0,
            },
        },
        "candidates": [],
    }

    # Fill in node FGID for winner
    if top:
        for node in nodes:
            if node.gln == top["gln"]:
                result["axes"]["fgid"]["node_fgid"] = node.fgid
                break

    # Top N candidates
    for c in candidates[:TOP_CANDIDATES]:
        result["candidates"].append({
            "gln": c["gln"],
            "name": c["name"],
            "path": c["path"],
            "score": c["score"],
        })

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def read_file_content(path, max_lines=CONTENT_READ_LINES):
    """Read the first N lines of a file for classification."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = []
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                lines.append(line)
            return "".join(lines)
    except (OSError, IOError) as e:
        print(f"WARNING: Could not read {path}: {e}", file=sys.stderr)
        return ""


def title_from_path(path):
    """Extract a title from a file path (stem, no extension)."""
    return Path(path).stem


def main():
    parser = argparse.ArgumentParser(
        description="GLN Resolver - Triple-Axis Document Classifier",
        epilog="Predicts the closest GLN for a document using archetype, FGID, and CEC axes.",
    )
    parser.add_argument(
        "file", nargs="?",
        help="Path to the document to classify (relative to repo root)",
    )
    parser.add_argument(
        "--title", "-t",
        help="Document title (overrides filename-derived title)",
    )
    parser.add_argument(
        "--content", "-c",
        help="Content snippet (overrides file reading)",
    )
    parser.add_argument(
        "--index", "-i",
        default=str(INDEX_PATH),
        help=f"Path to llms-N200-index.yaml (default: {INDEX_PATH})",
    )
    parser.add_argument(
        "--top", "-n",
        type=int, default=TOP_CANDIDATES,
        help=f"Number of top candidates to show (default: {TOP_CANDIDATES})",
    )

    args = parser.parse_args()

    if not args.file and not args.title:
        parser.error("Either a file path or --title is required")

    # Resolve file path
    file_path = ""
    if args.file:
        # Try as-is first, then relative to repo root
        p = Path(args.file)
        if not p.exists():
            p = REPO_ROOT / args.file
        if p.exists():
            file_path = str(p.relative_to(REPO_ROOT)) if str(p).startswith(str(REPO_ROOT)) else args.file
        else:
            file_path = args.file  # use as-is for path scoring even if file doesn't exist

    # Determine title
    title = args.title or (title_from_path(args.file) if args.file else "")

    # Determine content
    if args.content:
        content = args.content
    elif args.file:
        p = Path(args.file)
        if not p.exists():
            p = REPO_ROOT / args.file
        content = read_file_content(str(p)) if p.exists() else ""
    else:
        content = ""

    # Load data
    top_n = args.top

    index_path = Path(args.index)
    if not index_path.exists():
        print(f"ERROR: Index file not found: {index_path}", file=sys.stderr)
        sys.exit(1)

    nodes = load_gln_tree(str(index_path))

    # Resolve
    result = resolve_gln(title, content, file_path, nodes)

    # Update candidate count
    result["candidates"] = result["candidates"][:top_n]

    # Output as YAML
    print(yaml.dump(result, default_flow_style=False, sort_keys=False, allow_unicode=True))


if __name__ == "__main__":
    main()
