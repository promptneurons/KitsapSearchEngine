#!/bin/bash
# run-all-precompute.sh — Rebuild all KitsapSearchEngine caches
#
# Run after weekly token reset (Thursdays 20:00 UTC / 12:00 PST)
# Safe to re-run; each step is idempotent.
#
# Usage:
#   bash scripts/run-all-precompute.sh              # all corpora
#   bash scripts/run-all-precompute.sh --fast        # skip OD/Salo (already current)
#   bash scripts/run-all-precompute.sh --phora-fetch # fetch live OP from Phora
#
# Log: /tmp/kse-precompute-$(date +%Y%m%d).log

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG="/tmp/kse-precompute-$(date +%Y%m%dT%H%M%S).log"
FAST=0
PHORA_FETCH=""

for arg in "$@"; do
    case "$arg" in
        --fast)        FAST=1 ;;
        --phora-fetch) PHORA_FETCH="--fetch" ;;
    esac
done

log() { echo "[$(date -u +%H:%M:%SZ)] $*" | tee -a "$LOG"; }

log "=== KitsapSearchEngine full precompute ==="
log "REPO_ROOT: $REPO_ROOT"
log "Log: $LOG"
echo ""

cd "$REPO_ROOT"

# ─────────────────────────────────────────────────────────────────────────────
# 1. Macrobius Substack — refresh from RSS
# ─────────────────────────────────────────────────────────────────────────────
log "--- [1/5] Macrobius Substack refresh ---"
python3 scripts/substack-extract.py \
    --url https://macrobius.substack.com \
    --output-dir data/macrobius \
    --index data/macrobius-index.yaml \
    2>&1 | tee -a "$LOG" || log "WARN: macrobius extract failed (continuing)"

log "--- [2/5] Macrobius precompute ---"
python3 scripts/gln-precompute.py \
    --index data/macrobius-index.yaml \
    --output data/macrobius-cache.jsonl \
    2>&1 | tee -a "$LOG"

# ─────────────────────────────────────────────────────────────────────────────
# 2. Phora — extract from TTL, optionally fetch live OP
# ─────────────────────────────────────────────────────────────────────────────
log "--- [3/5] Phora extract ---"

# Sync latest phora TTL from GREEN if available
PHORA_TTL_GREEN="/home/claudius/clawd/data/phora-threads.ttl"
PHORA_TTL_LOCAL="$REPO_ROOT/data/phora-threads.ttl"

if [ -f "$PHORA_TTL_GREEN" ]; then
    log "Copying phora-threads.ttl from GREEN..."
    cp "$PHORA_TTL_GREEN" "$PHORA_TTL_LOCAL"
elif ssh -o ConnectTimeout=5 -o BatchMode=yes green "test -f $PHORA_TTL_GREEN" 2>/dev/null; then
    log "Syncing phora-threads.ttl from GREEN via SSH..."
    scp "green:$PHORA_TTL_GREEN" "$PHORA_TTL_LOCAL" 2>&1 | tee -a "$LOG"
else
    log "WARN: phora-threads.ttl not found locally or on GREEN — skipping Phora"
    PHORA_TTL_LOCAL=""
fi

if [ -n "$PHORA_TTL_LOCAL" ] && [ -f "$PHORA_TTL_LOCAL" ]; then
    python3 scripts/phora-extract-ttl.py \
        --ttl "$PHORA_TTL_LOCAL" \
        $PHORA_FETCH \
        2>&1 | tee -a "$LOG"

    log "--- [4/5] Phora precompute ---"
    python3 scripts/gln-precompute.py \
        --index data/phora-thread-index.yaml \
        --output data/phora-cache.jsonl \
        2>&1 | tee -a "$LOG"
else
    log "--- [4/5] Phora skipped ---"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 3. OD-2006 and Salo — only if --fast not set
# ─────────────────────────────────────────────────────────────────────────────
if [ "$FAST" -eq 0 ]; then
    log "--- [5a/5] OD-2006 precompute (full rebuild) ---"
    python3 scripts/gln-precompute.py \
        --index data/od-thread-index.yaml \
        --output data/od-cache.jsonl \
        2>&1 | tee -a "$LOG"

    log "--- [5b/5] Salo precompute (full rebuild) ---"
    python3 scripts/gln-precompute.py \
        --index data/salo-thread-index.yaml \
        --output data/salo-cache.jsonl \
        2>&1 | tee -a "$LOG"
else
    log "--- [5/5] OD + Salo skipped (--fast) ---"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
log ""
log "=== Summary ==="
for cache in data/macrobius-cache.jsonl data/phora-cache.jsonl \
             data/od-cache.jsonl data/salo-cache.jsonl; do
    if [ -f "$REPO_ROOT/$cache" ]; then
        count=$(wc -l < "$REPO_ROOT/$cache")
        size=$(du -sh "$REPO_ROOT/$cache" | cut -f1)
        log "  $cache: $count entries ($size)"
    else
        log "  $cache: NOT BUILT"
    fi
done
log "=== Done === Log: $LOG"
