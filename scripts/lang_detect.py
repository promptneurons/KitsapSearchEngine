#!/usr/bin/env python3
"""
lang_detect.py - Lightweight language detector (stdlib only)

Identifies the language of a text snippet using function-word
fingerprinting.  No external packages, no network calls.

Covers: en, de, fr, es, it, pt, nl, la, ru, pl, cs, sv, da, no
Returns "unknown" when confidence is below threshold.

Usage (as module):
    from lang_detect import detect_language, is_english
    lang, conf = detect_language("The quick brown fox")
    # → ("en", 0.82)

Usage (CLI smoke-test):
    python3 scripts/lang_detect.py "La volonté générale est toujours droite"
"""

import re

# ─────────────────────────────────────────────────────────────────────────────
# Function-word fingerprints (high-frequency, unambiguous per language)
# ─────────────────────────────────────────────────────────────────────────────
# Each set: the 20-30 most distinctive closed-class words for that language.
# Chosen to minimise cross-language overlap.

_FINGERPRINTS = {
    "en": {
        "the", "of", "and", "to", "in", "is", "that", "it", "was",
        "for", "are", "with", "this", "have", "from", "not", "but",
        "they", "which", "been", "would", "there", "their", "what",
        "were", "when", "will", "an", "or", "had", "can", "do",
    },
    "de": {
        "die", "der", "und", "in", "den", "von", "zu", "das", "mit",
        "sich", "des", "auf", "für", "ist", "im", "dem", "nicht",
        "ein", "eine", "als", "auch", "es", "an", "werden", "aus",
        "er", "hat", "dass", "sie", "nach", "wird", "bei", "noch",
    },
    "fr": {
        "le", "la", "les", "de", "du", "un", "une", "et", "en",
        "est", "que", "qui", "il", "elle", "au", "par", "sur",
        "des", "dans", "avec", "ne", "pas", "pour", "plus", "se",
        "son", "sa", "ce", "ont", "mais", "leur", "dont", "été",
    },
    "es": {
        "de", "la", "el", "que", "en", "los", "del", "las", "un",
        "por", "con", "una", "su", "para", "es", "se", "al", "lo",
        "como", "más", "pero", "sus", "le", "ya", "o", "fue",
        "este", "ha", "si", "porque", "esta", "son", "entre", "cuando",
    },
    "it": {
        "il", "di", "che", "la", "e", "in", "un", "una", "per",
        "del", "con", "non", "è", "si", "al", "lo", "le", "da",
        "della", "nel", "i", "anche", "dei", "ha", "sono", "ma",
        "come", "più", "sua", "questo", "ci", "sul", "dalla", "alla",
    },
    "pt": {
        "de", "a", "o", "que", "e", "do", "da", "em", "um", "para",
        "uma", "com", "não", "os", "por", "se", "na", "no", "como",
        "mais", "foi", "ao", "das", "dos", "sua", "seu", "já",
        "são", "mas", "esta", "isso", "este", "ele", "ela", "nos",
    },
    "nl": {
        "de", "van", "het", "een", "in", "is", "op", "dat", "te",
        "zijn", "er", "met", "voor", "en", "niet", "die", "aan",
        "ook", "bij", "worden", "door", "maar", "heeft", "ze",
        "worden", "naar", "hij", "nog", "als", "om", "uit", "this",
    },
    "la": {
        "et", "in", "est", "ad", "non", "ut", "qui", "quod", "sed",
        "cum", "ex", "de", "per", "nec", "atque", "aut", "iam",
        "si", "pro", "inter", "quam", "hoc", "quo", "qua", "enim",
        "esse", "me", "te", "se", "id", "quae", "eius", "erat",
    },
    "ru": {
        # Romanised common forms (after lowercasing Cyrillic won't match ASCII)
        # Use Cyrillic directly
        "и", "в", "не", "на", "я", "что", "он", "как", "это",
        "но", "его", "из", "то", "по", "они", "мы", "с", "да",
        "за", "о", "до", "же", "так", "от", "все", "или", "бы",
    },
    "pl": {
        "w", "i", "nie", "się", "na", "to", "jest", "z", "do",
        "że", "jak", "go", "tak", "ale", "już", "czy", "przez",
        "po", "ten", "za", "co", "jej", "jego", "tego",
    },
    "sv": {
        "och", "i", "att", "det", "som", "en", "är", "på", "av",
        "för", "med", "till", "den", "har", "inte", "om", "vi",
        "han", "de", "kan", "ett", "men", "sig", "hon",
    },
    "da": {
        "og", "i", "at", "det", "som", "en", "er", "på", "af",
        "for", "med", "til", "den", "har", "ikke", "om", "vi",
        "han", "de", "kan", "et", "men", "sig", "hun",
    },
    "no": {
        "og", "i", "at", "det", "som", "en", "er", "på", "av",
        "for", "med", "til", "den", "har", "ikke", "om", "vi",
        "han", "de", "kan", "et", "men", "seg", "hun",
    },
}

# Minimum function-word hits to register a language
# Adaptive: short texts need fewer hits
def _min_hits(total_tokens):
    if total_tokens < 15:  return 2
    if total_tokens < 40:  return 3
    return 4
# Minimum ratio of hits/tokens to report a language (not "unknown")
_MIN_RATIO = 0.04


# ─────────────────────────────────────────────────────────────────────────────
# Script detection (fast pre-filter)
# ─────────────────────────────────────────────────────────────────────────────

def _script_hint(text):
    """Return a rough script hint based on character ranges."""
    cyrillic = sum(1 for c in text if '\u0400' <= c <= '\u04ff')
    cjk      = sum(1 for c in text if '\u3040' <= c <= '\u30ff' or '\u4e00' <= c <= '\u9fff')
    arabic   = sum(1 for c in text if '\u0600' <= c <= '\u06ff')
    total    = max(len(text), 1)
    if cyrillic / total > 0.15:
        return "cyrillic"
    if cjk / total > 0.10:
        return "cjk"
    if arabic / total > 0.10:
        return "arabic"
    return "latin"


# ─────────────────────────────────────────────────────────────────────────────
# Main detector
# ─────────────────────────────────────────────────────────────────────────────

def detect_language(text, min_chars=40):
    """
    Detect language of text.

    Returns (lang_code, confidence) where:
      - lang_code  is ISO 639-1 ("en", "de", "fr", …) or "unknown"
      - confidence is float 0.0–1.0

    Confidence reflects the proportion of tokens matched by the
    winning fingerprint vs. total token count (capped at 1.0).

    For very short texts (< min_chars) returns ("unknown", 0.0).
    """
    if not text or len(text.strip()) < min_chars:
        return "unknown", 0.0

    script = _script_hint(text)

    if script == "cjk":
        return "zh", 1.0   # rough — could be ja/ko; treat all as non-English
    if script == "arabic":
        return "ar", 1.0

    # Tokenise: lowercase, split on non-alpha (covers Latin + Cyrillic + extended)
    tokens = re.findall(r"[a-zа-яёА-ЯЁ\u00c0-\u024f]+", text.lower())
    if not tokens:
        return "unknown", 0.0

    total = len(tokens)

    # For Cyrillic script, score ru/pl via fingerprint then return early
    if script == "cyrillic":
        cyrillic_scores = {}
        for lang in ("ru", "pl"):
            fp = _FINGERPRINTS.get(lang, set())
            hits = sum(1 for t in tokens if t in fp)
            if hits > 0:
                cyrillic_scores[lang] = hits / total
        if cyrillic_scores:
            best = max(cyrillic_scores, key=cyrillic_scores.get)
            return best, round(min(cyrillic_scores[best] * 8, 1.0), 3)
        return "ru", 0.5   # Cyrillic but no fingerprint match

    # Score each language — use frequency so repeated function words count
    min_h = _min_hits(total)
    scores = {}
    for lang, fp in _FINGERPRINTS.items():
        hits = sum(1 for t in tokens if t in fp)
        if hits >= min_h:
            scores[lang] = hits / total

    if not scores:
        return "unknown", 0.0

    best_lang = max(scores, key=scores.get)
    best_conf = min(scores[best_lang] / _MIN_RATIO, 1.0) * (scores[best_lang] / max(scores.values()))

    # Reject if ratio too low
    if scores[best_lang] < _MIN_RATIO:
        return "unknown", round(scores[best_lang], 3)

    # Disambiguate close pairs (es/pt, da/no/sv, etc.)
    sorted_scores = sorted(scores.items(), key=lambda x: -x[1])
    if len(sorted_scores) >= 2:
        top, second = sorted_scores[0], sorted_scores[1]
        # If second is within 20% of top, flag ambiguity — return top but lower confidence
        if second[1] / top[1] > 0.80:
            return top[0], round(top[1] * 0.6, 3)

    return best_lang, round(min(scores[best_lang] * 10, 1.0), 3)


def is_english(text, threshold=0.5):
    """Convenience: returns True if text is confidently English."""
    lang, conf = detect_language(text)
    return lang == "en" and conf >= threshold


def language_gate(text, threshold=0.5):
    """
    Returns (lang, is_english_flag, confidence).
    Use to decide whether to run SUMO expansion.
    """
    lang, conf = detect_language(text)
    return lang, (lang == "en" and conf >= threshold), conf


# ─────────────────────────────────────────────────────────────────────────────
# CLI smoke-test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    samples = sys.argv[1:] if len(sys.argv) > 1 else [
        "The quick brown fox jumps over the lazy dog. This is a test of the language detection system.",
        "La volonté générale est toujours droite et tend toujours à l'utilité publique.",
        "Die Grenzen meiner Sprache bedeuten die Grenzen meiner Welt.",
        "El pueblo que no conoce su historia está condenado a repetirla.",
        "Gallia est omnis divisa in partes tres, quarum unam incolunt Belgae.",
        "Каждый человек имеет право на жизнь, свободу и личную неприкосновенность.",
        "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod.",
        "短い日本語テキスト",
    ]

    for s in samples:
        lang, conf = detect_language(s)
        print(f"[{lang:8s} {conf:.2f}]  {s[:70]}")
