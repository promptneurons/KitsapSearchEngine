# Claudius Command Reference

**Version:** 2026-02-11  
**Status:** Living document

---

## Command Map

### Finance (FN)

| Command | Capability | Status |
|---------|------------|--------|
| `/fn status` | Show FQ + cash + burn | 📋 Designed |
| `/fn costs` | Cost breakdown by category | 📋 Designed |
| `/fn revenue` | Revenue sources | 📋 Designed |
| `/fn burn` | Monthly burn rate | 📋 Designed |
| `/fn runway` | Months remaining | 📋 Designed |
| `/fn approve <id>` | Approve FQ item | 📋 Designed |
| `/fn audit <id>` | Show decision trail | 📋 Designed |

### Data / ETL

| Command | Capability | Status |
|---------|------------|--------|
| `/data ingest <path>` | ETL file to S3 | 📋 Designed |
| `/data status` | Inbox + processing queue | 📋 Designed |
| `/data list` | Show inbox contents | 📋 Designed |
| `/data validate <path>` | Check file without loading | 📋 Designed |
| `/classify <path>` | Predict GLN for file (gln-resolver) | ✅ Live |
| `/classify --title <t>` | Predict GLN by title + content | ✅ Live |
| `/rank` | Rank 10 random docs against 1 query | ✅ Live |
| `/rank --query <path>` | Rank against specific document | ✅ Live |
| `/rank --seed <n>` | Reproducible ranking | ✅ Live |

### Decision Queue (DQ)

| Command | Capability | Status |
|---------|------------|--------|
| `/dq list` | Show pending decisions | ✅ Manual |
| `/dq add <desc>` | Propose new decision | ✅ Manual |
| `/dq approve <id>` | Sign off (2PC) | ✅ Manual |
| `/dq status` | Queue summary | ✅ Manual |

### Quality / Ops

| Command | Capability | Status |
|---------|------------|--------|
| `/qg run` | Run quality gate | ✅ Live |
| `/qg status` | Last gate result | ✅ Live |
| `/qg preflight` | Surface assumptions | ✅ Live |
| `/audit log` | Recent audit entries | ✅ Live |

### Campaign (ICS Operations)

| Command | Capability | Status |
|---------|------------|--------|
| `/campaign status` | Current campaign + op period | 🔧 Building |
| `/campaign list` | All campaigns | 📋 Designed |
| `/campaign start <id>` | Activate campaign | 📋 Designed |
| `/campaign objective <id> <status>` | Update objective | 📋 Designed |
| `/campaign ccir <trigger>` | Log CCIR event | 📋 Designed |
| `/campaign close <id>` | Close with lessons | 📋 Designed |

### CAPA (Corrective Actions)

| Command | Capability | Status |
|---------|------------|--------|
| `/capa list` | Show active CAPAs | ✅ Manual |
| `/capa add <source> <desc>` | Log new CAPA | ✅ Manual |
| `/capa status <id>` | CAPA details | ✅ Manual |
| `/capa close <id>` | Close with verification | ✅ Manual |

### Discovery

| Command | Capability | Status |
|---------|------------|--------|
| `/demos` | List available demos | ✅ Live |
| `/help` | Command overview | ✅ Live |
| `/status` | Session status | ✅ Live |

### AOR-Specific (Role Defaults)

| Command | Role | Default View |
|---------|------|--------------|
| `/ld` | Leadership | Org chart, decisions |
| `/mk` | Marketing | Campaigns, costs |
| `/mg` | Management | Projects, status |
| `/cf` | Customer/Finance | FQ, revenue |
| `/lg` | Legal/Compliance | Audit, compliance |
| `/fn` | Finance | T-accounts, costs |
| `/sf` | Staff/IT | Infrastructure, security |

---

## Capability Matrix

| Capability | Commands | Dependencies |
|------------|----------|--------------|
| **Financial Tracking** | /fn, /cf | JSONL data, Stripe |
| **Decision Governance** | /dq, /fn approve | 2PC, Beads |
| **Data Ingestion** | /data | S3, transform scripts |
| **Document Classification** | /classify, /rank | gln-resolver.py, gln-cache.jsonl |
| **Quality Monitoring** | /qg, /audit | quality-gate.sh |
| **Visual Introspection** | /ld, Petri nets | DOT, GREEN SVG |

---

## Implementation Status

| Status | Meaning |
|--------|---------|
| ✅ Live | Working now |
| 📋 Designed | Spec ready, not built |
| 🔧 Building | In progress |
| 💡 Idea | Not yet designed |

---

## Command Parsing

All commands follow pattern:
```
/<domain> <action> [target] [--flags]
```

Examples:
```
/fn status                    # domain=fn, action=status
/fn approve FQ-003            # domain=fn, action=approve, target=FQ-003
/data ingest file.csv --type=financial
/classify projects/BUSINESS-PLAN.md
/rank --query projects/BUSINESS-PLAN.md --pool 20
```

---

## Integration Points

| System | Commands | Protocol |
|--------|----------|----------|
| **Telegram** | All | Chat interface |
| **Agent Mail** | /data (future) | INGEST_REQUEST |
| **S3** | /data | Archivist writer |
| **Beads** | /dq | JSONL state |
| **Virtuoso** | /audit | SPARQL sync |

---

## Future: Control Plane Integration

```yaml
# control-plane/commands/claudius.yaml
agent: claudius
commands:
  - domain: fn
    actions: [status, costs, revenue, approve]
    capabilities: [financial-tracking, decision-governance]
  - domain: data
    actions: [ingest, status, list, validate]
    capabilities: [etl, s3-write]
```

---

*Commands are the interface. Capabilities are the power.*
