import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("Documentation")
st.caption("Technical reference for the AML screening pipeline, scoring model, and dashboard usage.")

st.markdown("<br>", unsafe_allow_html=True)

with st.expander("**1. Pipeline Overview**", expanded=True):
    st.markdown("""
The Argus AML screening pipeline is a **fully automated, event-driven system** built natively in Snowflake. It screens incoming customer and transaction data against global sanctions lists, scores potential matches, and routes results through AI-assisted adjudication.

### Architecture

```
INCOMING_SCREENINGS (table)
        │
        ▼
INCOMING_SCREENINGS_STREAM (append-only stream)
        │
        ▼  ┌─────────────────────────────────┐
SCREEN_NEW_RECORDS_TASK ──►│  SCREEN_BATCH() procedure        │
  (every 5 min)            │  ┌─────────────────────────────┐ │
                           │  │ 1. Cleanse names (unidecode) │ │
                           │  │ 2. Generate phonetic tokens   │ │
                           │  │ 3. Blocking join (phonetic)   │ │
                           │  │ 4. Composite scoring          │ │
                           │  │ 5. Classification (4-way)     │ │
                           │  └─────────────────────────────┘ │
                           └─────────────────────────────────┘
        │
        ▼
SCREENING_RESULTS (table)
        │
        ▼  ┌─────────────────────────────────┐
AI_ADJUDICATOR_TASK ──────►│  RUN_AI_ADJUDICATOR() procedure │
  (chained after screening) │  Cortex LLM: DISMISS / ESCALATE │
                           └─────────────────────────────────┘
        │
        ▼
SCREENING_RESULTS (updated with AI decision)
        │
        ▼
ARGUS DASHBOARD (Streamlit) ──► Human review & final disposition
```

### Key Tables

| Table | Purpose |
|-------|---------|
| `INCOMING_SCREENINGS` | Inbound screening requests (KYC, transaction monitoring, CSV uploads) |
| `SCREENING_RESULTS` | All screening outcomes with scores, matches, and dispositions |
| `SANCTIONS_LIST_SNAPSHOT` | Versioned copy of the sanctions reference data |
| `SANCTIONS_PHONETIC_BLOCKS` | Pre-computed phonetic tokens for efficient blocking joins |
| `AUDIT_LOG` | Immutable event trail for compliance |
| `PIPELINE_SETTINGS` | Configurable thresholds and parameters |
""")

with st.expander("**2. Sanctions Data Source**"):
    st.markdown("""
### Source

The pipeline ingests sanctions data from the **Snowflake Marketplace**:

```
GLOBAL_SANCTIONS_AND_PEP_LISTS.GLOBAL_SANCTIONS_AND_PEP_LISTS_SAMPLE_DATA.PEP_SAMPLE_DATA
```

### Snapshot Refresh Logic

The `REFRESH_SANCTIONS_SNAPSHOT` procedure runs daily at **2:00 AM ET** and:

1. **Computes an MD5 hash** of the source data (`ENTITY_NAME || ENTITY_ALIASES || LISTING_COUNTRY`)
2. **Compares** against the previous snapshot hash
3. **Skips** if unchanged (logs `SANCTIONS_SNAPSHOT_SKIPPED` to audit)
4. **Loads** new snapshot if changed:
   - Inserts all entities with `CLEANSE_NAME()` applied
   - Builds phonetic blocks from **primary names**
   - Builds phonetic blocks from **all aliases** (split by `;`)
5. **Logs** `SANCTIONS_SNAPSHOT_REFRESHED` to audit

### Alias-Based Phonetic Blocking

Each sanctions entry can have multiple aliases (e.g., `"Vladimir Putin;V.V. Putin;Владимир Путин"`). The pipeline generates phonetic tokens for **every alias**, not just the primary name. This enables cross-script matching where the input is in Cyrillic/Arabic/CJK but the primary name is in Latin.
""")

with st.expander("**3. Screening Process**"):
    st.markdown("""
When `SCREEN_BATCH()` runs, it processes all new records from the stream in a single atomic transaction:

### Step 1: Name Cleansing (`CLEANSE_NAME`)

```
Input:  "Dr. Владимир Путин LLC"
    ↓   unidecode() → romanize all scripts
        "Dr. Vladimir Putin LLC"
    ↓   Remove honorifics (Dr, Prof, Sheikh, etc.)
        "Vladimir Putin LLC"
    ↓   Remove entity suffixes (LLC, Ltd, Corp, etc.)
        "Vladimir Putin"
    ↓   Remove punctuation, normalize whitespace
Output: "VLADIMIR PUTIN"
```

**Supported scripts**: Latin, Cyrillic, Arabic, Chinese (Hanzi), Japanese (Kanji/Kana), Korean (Hangul), Devanagari, Thai, and all scripts supported by the `unidecode` library.

### Step 2: Phonetic Tokenization (`PHONETIC_TOKENS`)

Each word in the cleansed name generates **two phonetic keys**:

| Algorithm | Purpose | Example |
|-----------|---------|---------|
| **Metaphone** | English phonetic encoding | "PUTIN" → `PTN` |
| **NYSIIS** | Name-optimized phonetic encoding | "PUTIN" → `PATAN` |

### Step 3: Blocking Join

The screened entity's phonetic tokens are joined against `SANCTIONS_PHONETIC_BLOCKS` to find candidate matches. This is a **recall-optimized** step — it casts a wide net to avoid false negatives.

A match occurs when **any** phonetic token from the screened name matches **any** token from a sanctions entry (including alias-derived tokens).

### Step 4: Composite Scoring (`COMPOSITE_SCORE_WITH_ALIASES`)

Each blocked pair is scored. If the sanctions entry has aliases, the screened name is scored against **each alias** independently, and the **highest score** is kept.
""")

with st.expander("**4. Scoring Model**"):
    st.markdown("""
The composite score is a weighted combination of four signals:

### Weight Allocation (adaptive based on data availability)

| Data Available | Name | DOB | POB | Country |
|----------------|------|-----|-----|---------|
| DOB + POB present | 65% | 18% | 10% | 7% |
| DOB only | **70%** | **20%** | 0% | **10%** |
| POB only | 80% | 0% | 8% | 12% |
| Neither | 85% | 0% | 0% | 15% |

### Name Similarity (3 sub-components)

| Component | Weight | Method |
|-----------|--------|--------|
| **Token Alignment** | 60% | Greedy bipartite matching of name tokens using Jaro-Winkler similarity |
| **Full String Jaro-Winkler** | 25% | Character-level similarity of the complete romanized name |
| **Phonetic Resonance** | 15% | Jaccard overlap of Metaphone/NYSIIS key sets |

**Token penalty**: If the worst token-pair similarity is below 0.80 and both names have the same token count, a penalty is applied to prevent "Mohammed Ali" from matching "Mohammed Petrov".

### DOB Fuzzy Matching

| Condition | Score | Type |
|-----------|-------|------|
| Exact match | 1.00 | `exact` |
| Day/month swapped (both ≤12) | 0.95 | `format_ambiguity_possible` |
| Day/month swapped (one >12) | 0.85 | `day_month_swapped` |
| Same month/day, year ±1 | 0.75 | `year_plus_minus_1` |
| Same year/month, day ±2 | 0.70 | `day_close` |
| Same year only | 0.40 | `year_only` |
| No match | 0.00 | `no_match` |

### Place of Birth Matching

Uses Jaro-Winkler similarity on romanized POB strings:
- **≥ 0.85** → `fuzzy_high`
- **≥ 0.70** → `fuzzy_partial`
- **< 0.70** → `no_match`

### Country Matching

Binary: **1.0** if `UPPER(TRIM(screened_country)) == UPPER(TRIM(sanction_country))`, else **0.0**.
""")

with st.expander("**5. Disposition Routing**"):
    settings_df = session.sql("""
        SELECT SETTING_KEY, SETTING_VALUE, DESCRIPTION
        FROM AML_SCREENING.PIPELINE.PIPELINE_SETTINGS
        WHERE SETTING_KEY LIKE '%THRESHOLD%' OR SETTING_KEY LIKE '%NAME_SIM%' OR SETTING_KEY = 'DOB_YEAR_GAP_MAX'
        ORDER BY SETTING_KEY
    """).to_pandas()

    st.markdown("""
### Classification Logic (`CLASSIFY_MATCH`)

```
                          ┌──────────────────────────┐
                          │  Logical Exclusion?       │
                          │  (DOB gap > 15 years)     │
                          └────────┬─────────────────┘
                                   │ Yes → NO_MATCH
                                   │ No ↓
                          ┌──────────────────────────┐
                          │  Name Sim < 0.70?         │
                          └────────┬─────────────────┘
                                   │ Yes → NO_MATCH
                                   │ No ↓
                          ┌──────────────────────────┐
                          │  Name Sim ≥ 0.85?         │
                          │  AND (DOB ≥ 0.85           │
                          │   OR Country ≥ 0.85        │
                          │   OR High-risk country)?   │
                          └────────┬─────────────────┘
                                   │ Yes → CRITICAL_MATCH
                                   │ No ↓
                          ┌──────────────────────────┐
                          │  Name Sim ≥ 0.85?         │
                          │  OR High-risk country?    │
                          └────────┬─────────────────┘
                                   │ Yes → PENDING_HUMAN_REVIEW
                                   │ No ↓
                              PENDING_AI_ADJUDICATION
```

**High-risk countries** (auto-escalated): Iran (IR), North Korea (KP), Syria (SY), Cuba (CU), Myanmar (MM)

### Current Thresholds
""")
    st.dataframe(settings_df, use_container_width=True, hide_index=True)

with st.expander("**6. AI Adjudicator**"):
    st.markdown("""
### Overview

Records classified as `PENDING_AI_ADJUDICATION` (name similarity between 0.70–0.85, no high-risk country flag) are evaluated by a **Snowflake Cortex LLM**.

### Model & Parameters

| Setting | Value |
|---------|-------|
| Model | `llama3.1-70b` (via `SNOWFLAKE.CORTEX.COMPLETE`) |
| Temperature | 0 (deterministic) |
| Max Tokens | 1024 |

### Decision Framework

The LLM receives the full match data (screened name, sanctions name, all scores, DOB/country/POB comparisons) and applies structured rules:

**DISMISS criteria:**
- Only a single common first name token matches (e.g., "Mohammed" alone)
- All corroborating data (DOB, country, POB) contradicts
- Name similarity is driven by common tokens, not meaningful overlap

**ESCALATE criteria:**
- Multiple name tokens match with high individual similarity
- Any corroborating data supports the match
- Match involves aliases or transliterated names

### Safety Net: One-Way Gate

If `name_similarity ≥ 0.85` (configurable via `NAME_SIM_ONE_WAY_GATE`), the AI **cannot auto-dismiss** even if it decides DISMISS. Instead, the result is set to `DISMISS_OVERRIDDEN` with disposition `PENDING_HUMAN_REVIEW` and the gate is logged in `AI_ERROR`.

### Output

The AI returns structured JSON:
```json
{
    "name_analysis": "Token-by-token comparison...",
    "data_corroboration": "DOB/country/POB analysis...",
    "applicable_rule": "Which dismissal/escalation rule applies...",
    "decision": "DISMISS or ESCALATE",
    "reasoning": "One-sentence summary."
}
```
""")

with st.expander("**7. Cross-Script Support**"):
    st.markdown("""
### How It Works

All name processing uses the `unidecode` library to **transliterate any script to ASCII Latin** before phonetic encoding and comparison.

### Examples

| Input | Script | Romanized | Phonetic Keys |
|-------|--------|-----------|---------------|
| Владимир Путин | Cyrillic | VLADIMIR PUTIN | `FLTMR`, `PTN`, `VLADANAR`, `PATAN` |
| 김정은 | Korean (Hangul) | GIMJEONGEUN | `JMJNJN`, `GANJANGAN` |
| بشار الأسد | Arabic | BSHAR AL'SD | `BXAR`, `ALST` |
| 李伟 | Chinese (Hanzi) | LI WEI | `L`, `W` |
| दाऊद इब्राहिम | Devanagari | DA'UD IBRAHIMA | `TTT`, `ABRHM` |

### How Cross-Script Matching Works

1. **Screened input** in native script → `CLEANSE_NAME()` → romanized + uppercased
2. **Phonetic tokens** generated from romanized text
3. **Blocking join** matches against sanctions phonetic blocks (which include tokens from **all aliases**, including native-script aliases)
4. **Scoring** compares romanized screened name against romanized sanctions name/aliases

### Limitation

`unidecode` transliteration is **lossy and best-guess**. Korean "김" → "Gim" (not "Kim"), so the primary name "Kim Jong Un" won't produce matching tokens for "김정은". This is solved by including the Korean alias `김정은` in the sanctions entry, which generates its own phonetic blocks that can be matched.
""")

with st.expander("**8. Dashboard Guide**"):
    st.markdown("""
### Pages

#### Dashboard
The main overview page showing:
- **AI Noise Reduction chart** — Daily count of `AUTO_DISMISSED` screenings, showing how many false positives the AI removes
- **Pending Review card** — Count of `CRITICAL_MATCH` + `PENDING_HUMAN_REVIEW` cases awaiting officer action

#### Cases
Two views:
- **Case List** — Filterable table of all screening results. Filter by disposition status. Click any row to view details.
- **Case Detail** — Full screening analysis including:
  - Entity information (name, DOB, country, type)
  - Risk score gauge and name similarity breakdown
  - **Match Comparison table** — Side-by-side of screened vs. matched data with MATCH/MISMATCH/PARTIAL indicators
  - **AI Analysis** — The LLM's reasoning (if applicable)
  - **Score Breakdown** — Token alignment, Jaro-Winkler, phonetic resonance visualization
  - **Evidence & Files** — Upload/download supporting documents
  - **Review Decision** — Submit Clear/Escalate with rationale
  - **PDF Export** — Generate a compliance case report

#### Integrations
Two input methods:
- **Manual Entry** — Single record form with first/last/middle name, DOB, POB, gender, country
- **Bulk CSV Upload** — Upload a CSV file with columns: `FIRST_NAME, LAST_NAME, MIDDLE_NAME, DATE_OF_BIRTH, PLACE_OF_BIRTH, GENDER, COUNTRY, SOURCE_SYSTEM`

Both methods insert into `INCOMING_SCREENINGS`. Records are automatically screened within 5 minutes by the task scheduler, or manually via `CALL AML_SCREENING.PIPELINE.SCREEN_BATCH()`.

#### Reports
Pipeline analytics:
- Total screenings, pending review, dismissed, AI accuracy, average risk score
- Screening volume by disposition (stacked bar chart over time)
- Disposition donut chart
- Downloadable CSV export of all results

#### DB Admin
- **Pipeline Settings** — Edit thresholds (name similarity, DOB gap, AI parameters)
- **Sanctions Snapshot** — Browse current sanctions entries and metadata
- **Audit Log** — Filterable event history
- **Table Browser** — Read-only inspection of any table in the database

#### Debugger
Placeholder for system diagnostics and AI model tracing (future development).
""")

with st.expander("**9. Task Automation**"):
    st.markdown("""
### Scheduled Tasks

| Task | Schedule | Trigger | Action |
|------|----------|---------|--------|
| `SCREEN_NEW_RECORDS_TASK` | Every 5 minutes | `SYSTEM$STREAM_HAS_DATA()` | Calls `SCREEN_BATCH()` |
| `AI_ADJUDICATOR_TASK` | Chained (after screening) | Predecessor completes | Calls `RUN_AI_ADJUDICATOR()` |
| `REFRESH_SANCTIONS_TASK` | Daily at 2:00 AM ET | CRON schedule | Calls `REFRESH_SANCTIONS_SNAPSHOT()` |

### Task Graph

```
SCREEN_NEW_RECORDS_TASK (root, 5-min schedule)
        │
        └──► AI_ADJUDICATOR_TASK (child, runs after parent)

REFRESH_SANCTIONS_TASK (independent, daily CRON)
```

### Manual Execution

```sql
CALL AML_SCREENING.PIPELINE.SCREEN_BATCH();
CALL AML_SCREENING.PIPELINE.RUN_AI_ADJUDICATOR();
CALL AML_SCREENING.PIPELINE.REFRESH_SANCTIONS_SNAPSHOT();
```

### Task Management

```sql
-- Suspend (child first, then parent)
ALTER TASK AML_SCREENING.PIPELINE.SCREEN_NEW_RECORDS_TASK SUSPEND;
ALTER TASK AML_SCREENING.PIPELINE.AI_ADJUDICATOR_TASK SUSPEND;

-- Resume (child first, then parent)
ALTER TASK AML_SCREENING.PIPELINE.AI_ADJUDICATOR_TASK RESUME;
ALTER TASK AML_SCREENING.PIPELINE.SCREEN_NEW_RECORDS_TASK RESUME;
```
""")

with st.expander("**10. Configuration Reference**"):
    all_settings = session.sql("""
        SELECT SETTING_KEY, SETTING_VALUE, DESCRIPTION, UPDATED_AT, UPDATED_BY
        FROM AML_SCREENING.PIPELINE.PIPELINE_SETTINGS
        ORDER BY SETTING_KEY
    """).to_pandas()

    st.markdown("All pipeline behavior is controlled via `PIPELINE_SETTINGS`. Changes take effect on the next screening batch run.")
    st.dataframe(all_settings, use_container_width=True, hide_index=True)

    st.markdown("""
### UDF Reference

| Function | Type | Purpose |
|----------|------|---------|
| `CLEANSE_NAME(VARCHAR)` | Python UDF | Romanize + normalize names |
| `PHONETIC_TOKENS(VARCHAR)` | Python UDTF | Generate Metaphone/NYSIIS tokens |
| `GET_PHONETIC_KEY(VARCHAR)` | Python UDF | Combined phonetic key string |
| `COMPOSITE_SCORE(...)` | Python UDF | Score a single name pair |
| `COMPOSITE_SCORE_WITH_ALIASES(...)` | Python UDF | Score against all aliases, return best |
| `CLASSIFY_MATCH(...)` | SQL UDF | 4-way disposition routing |
| `NORMALIZE_COUNTRY(VARCHAR)` | SQL UDF | Uppercase/trim country codes |
| `GET_SETTING(VARCHAR)` | SQL UDF | Read from PIPELINE_SETTINGS |
""")
