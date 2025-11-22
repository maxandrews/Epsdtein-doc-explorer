# Analysis Pipeline Guide

This guide covers running the document analysis pipeline to extract relationships from new documents.

> **Note:** The analysis pipeline is only needed if you want to process new documents. The repository includes a pre-built database with analyzed documents.

## Prerequisites

### Required

- Node.js 18+
- Anthropic API key ([Get one here](https://console.anthropic.com/))
- Source documents (PDFs)

### Optional

- Python 3.8+ (for PDF extraction)
- SQLite CLI (for database inspection)

## Overview

The analysis pipeline consists of several stages:

```
PDF Documents
     │
     ▼
┌─────────────────┐
│ 1. Extract Text │ ── extract_data.py
└────────┬────────┘
         ▼
┌─────────────────┐
│ 2. AI Analysis  │ ── analyze_documents.ts
└────────┬────────┘
         ▼
┌─────────────────┐
│ 3. Deduplicate  │ ── dedupe_with_llm.ts
└────────┬────────┘
         ▼
┌─────────────────┐
│ 4. Cluster Tags │ ── cluster_tags.ts
└────────┬────────┘
         ▼
┌─────────────────┐
│ 5. Update Index │ ── update_top_clusters.ts
└────────┬────────┘
         ▼
   Ready to Use
```

---

## Environment Setup

### Set API Key

```bash
# Option 1: Export in terminal
export ANTHROPIC_API_KEY=sk-ant-your-key-here

# Option 2: Create .env file
echo "ANTHROPIC_API_KEY=sk-ant-your-key-here" > .env
```

### Verify Setup

```bash
# Check API key is set
echo $ANTHROPIC_API_KEY

# Test API connection
curl -s https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{"model":"claude-3-haiku-20240307","max_tokens":10,"messages":[{"role":"user","content":"Hi"}]}' \
  | head -c 100
```

---

## Pipeline Stages

### Stage 1: Extract Text from PDFs

**Script:** `analysis_pipeline/extract_data.py`

**Purpose:** Extract raw text from PDF documents.

**Prerequisites:**
```bash
# Install Python dependencies
pip install PyPDF2 pdfminer.six
```

**Usage:**
```bash
cd analysis_pipeline
python extract_data.py --input ../data/new_documents/ --output ./extracted/
```

**Input:** Directory of PDF files
**Output:** JSON files with extracted text

**Output format:**
```json
{
  "doc_id": "DOC-001-0045",
  "file_path": "data/new_documents/document.pdf",
  "text": "Full extracted text content...",
  "page_count": 15,
  "extraction_timestamp": "2024-01-15T12:00:00Z"
}
```

---

### Stage 2: AI-Powered Analysis

**Script:** `analysis_pipeline/analyze_documents.ts`

**Purpose:** Use Claude AI to extract entities, relationships, and metadata.

**Usage:**
```bash
# Analyze all unprocessed documents
npm run analyze

# Or run directly
npx tsx analysis_pipeline/analyze_documents.ts
```

**What it extracts:**
- **Document summary** (one sentence + paragraph)
- **Date range** (earliest and latest dates mentioned)
- **RDF triples** (actor-action-target relationships)
- **Semantic tags** for each relationship
- **Locations** where events occurred
- **Actor types** (person, organization, etc.)

**Model used:** `claude-haiku-4-5-20250929` (fast and cost-effective)

**Cost estimation:**
- ~$0.001-0.005 per document (varies by length)
- Full corpus (~1000 docs): ~$2-5

**Configuration options** (in the script):
```typescript
const MODEL = 'claude-haiku-4-5-20250929';
const MAX_TOKENS = 4096;
const BATCH_SIZE = 10;  // Documents per batch
```

**Database tables populated:**
- `documents` - Document metadata and summaries
- `rdf_triples` - Extracted relationships

---

### Stage 3: Entity Deduplication

**Script:** `analysis_pipeline/dedupe_with_llm.ts`

**Purpose:** Merge duplicate entity names (e.g., "Jeff Epstein" → "Jeffrey Epstein").

**Usage:**
```bash
npm run dedupe-llm

# Or run directly
npx tsx analysis_pipeline/dedupe_with_llm.ts
```

**How it works:**
1. Finds similar names using fuzzy string matching
2. Groups potential duplicates
3. Uses Claude to determine if they're the same entity
4. Creates alias mappings in `entity_aliases` table

**Example aliases created:**
```
"Jeff Epstein" → "Jeffrey Epstein"
"J. Epstein" → "Jeffrey Epstein"
"Ghislaine" → "Ghislaine Maxwell"
```

**The API server automatically resolves aliases** at query time, so you don't need to reprocess the triples.

---

### Stage 4: Tag Clustering

**Script:** `analysis_pipeline/cluster_tags.ts`

**Purpose:** Group 28,000+ tags into 30 semantic clusters for filtering.

**Usage:**
```bash
npx tsx analysis_pipeline/cluster_tags.ts
```

**How it works:**
1. Collects all unique tags from `rdf_triples.triple_tags`
2. Generates embeddings using Qwen3-Embedding-0.6B-ONNX
3. Runs K-means clustering (K=30)
4. Generates human-readable cluster names from exemplar tags

**Output:** `tag_clusters.json`

```json
[
  {
    "id": 0,
    "name": "Legal Proceedings",
    "exemplars": ["lawsuit", "deposition", "court filing", "testimony"],
    "tags": ["lawsuit", "legal action", "court case", ...]
  },
  ...
]
```

**Configuration:**
```typescript
const NUM_CLUSTERS = 30;
const EMBEDDING_MODEL = 'Qwen/Qwen3-Embedding-0.6B-ONNX';
```

**Note:** This can take 5-10 minutes due to embedding generation.

---

### Stage 5: Update Top Clusters

**Script:** `analysis_pipeline/update_top_clusters.ts`

**Purpose:** Pre-compute the top 3 clusters for each relationship for fast filtering.

**Usage:**
```bash
npx tsx analysis_pipeline/update_top_clusters.ts
```

**How it works:**
1. For each RDF triple, counts tag matches per cluster
2. Stores top 3 cluster IDs in `top_cluster_ids` column
3. Creates index for fast filtering

**This is a migration script** - run it once after clustering, or after updating clusters.

**Performance impact:** Improves query filtering by 10x+.

---

## Complete Pipeline Run

To process new documents from scratch:

```bash
# 1. Set API key
export ANTHROPIC_API_KEY=sk-ant-your-key-here

# 2. Extract text from PDFs (if needed)
cd analysis_pipeline
python extract_data.py --input ../data/new_docs/ --output ./extracted/
cd ..

# 3. Run AI analysis
npm run analyze

# 4. Deduplicate entities
npm run dedupe-llm

# 5. Generate tag clusters
npx tsx analysis_pipeline/cluster_tags.ts

# 6. Update cluster indexes
npx tsx analysis_pipeline/update_top_clusters.ts

# 7. Verify results
sqlite3 document_analysis.db "SELECT COUNT(*) FROM documents;"
sqlite3 document_analysis.db "SELECT COUNT(*) FROM rdf_triples;"
```

---

## Incremental Processing

To add new documents without reprocessing everything:

```bash
# 1. Identify new documents
npx tsx analysis_pipeline/identify_new_documents.ts

# 2. Analyze only new documents
npm run analyze
# The script skips already-processed doc_ids

# 3. Run deduplication (will find new aliases)
npm run dedupe-llm

# 4. Optionally re-cluster if many new tags
npx tsx analysis_pipeline/cluster_tags.ts
npx tsx analysis_pipeline/update_top_clusters.ts
```

---

## Utility Scripts

### `analysis_pipeline/split_large_docs.ts`

Split large documents into smaller chunks for better analysis:

```bash
npx tsx analysis_pipeline/split_large_docs.ts
```

### `analysis_pipeline/analyze_tags.ts`

Analyze tag distribution and find outliers:

```bash
npx tsx analysis_pipeline/analyze_tags.ts
```

### `analysis_pipeline/fix_unknown_actors.ts`

Resolve "Unknown Person" entries with more context:

```bash
npx tsx analysis_pipeline/fix_unknown_actors.ts
```

### `analysis_pipeline/name_clusters_with_claude.ts`

Generate better cluster names using Claude:

```bash
npx tsx analysis_pipeline/name_clusters_with_claude.ts
```

---

## Script Reference

| Script | Purpose | When to Run |
|--------|---------|-------------|
| `extract_data.py` | PDF text extraction | New PDF documents |
| `analyze_documents.ts` | AI entity extraction | New documents |
| `dedupe_with_llm.ts` | Entity deduplication | After analysis |
| `cluster_tags.ts` | Tag clustering | After major tag changes |
| `update_top_clusters.ts` | Index optimization | After clustering |
| `split_large_docs.ts` | Document splitting | Large documents |
| `identify_new_documents.ts` | Find unprocessed docs | Before analysis |
| `process_csv_tranches.ts` | Batch CSV processing | Bulk imports |

---

## Cost Management

### Estimating Costs

```bash
# Check how many documents need processing
sqlite3 document_analysis.db "
  SELECT COUNT(*) as total,
         SUM(CASE WHEN error IS NULL THEN 1 ELSE 0 END) as processed
  FROM documents;
"

# Check average tokens per document
sqlite3 document_analysis.db "
  SELECT AVG(input_tokens) as avg_input,
         AVG(output_tokens) as avg_output,
         SUM(cost_usd) as total_cost
  FROM documents;
"
```

### Cost-Saving Tips

1. **Use Haiku model** (default) - 10x cheaper than Sonnet
2. **Batch processing** - Reduces API overhead
3. **Skip processed documents** - Script checks `doc_id` before processing
4. **Set token limits** - Prevent runaway costs on large documents

---

## Troubleshooting

### API Rate Limits

**Error:** `429 Too Many Requests`

**Solution:** The script has built-in retry logic with exponential backoff. If persists, reduce batch size:
```typescript
const BATCH_SIZE = 5;  // Reduce from 10
```

### Out of Memory

**Error:** Node.js heap out of memory during clustering.

**Solution:**
```bash
# Increase Node.js memory limit
NODE_OPTIONS="--max-old-space-size=4096" npx tsx analysis_pipeline/cluster_tags.ts
```

### Database Locked

**Error:** `SQLITE_BUSY`

**Solution:** Ensure no other process is using the database:
```bash
fuser document_analysis.db
# Kill any listed processes
```

### Invalid JSON in Response

**Error:** JSON parse error from Claude response.

**Solution:** The script includes JSON repair logic. If issues persist, check:
```bash
# Run test on a single document
npx tsx analysis_pipeline/test_json_repair.ts
```

---

## Database Schema Reference

### documents

```sql
CREATE TABLE documents (
  id INTEGER PRIMARY KEY,
  doc_id TEXT UNIQUE NOT NULL,
  file_path TEXT NOT NULL,
  one_sentence_summary TEXT,
  paragraph_summary TEXT,
  date_range_earliest TEXT,
  date_range_latest TEXT,
  category TEXT,
  content_tags TEXT,      -- JSON array
  full_text TEXT,
  analysis_timestamp TEXT,
  input_tokens INTEGER,
  output_tokens INTEGER,
  cost_usd REAL,
  error TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### rdf_triples

```sql
CREATE TABLE rdf_triples (
  id INTEGER PRIMARY KEY,
  doc_id TEXT NOT NULL,
  timestamp TEXT,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  target TEXT NOT NULL,
  location TEXT,
  actor_likely_type TEXT,
  triple_tags TEXT,       -- JSON array
  explicit_topic TEXT,
  implicit_topic TEXT,
  sequence_order INTEGER,
  top_cluster_ids TEXT,   -- JSON array (materialized)
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
);
```

### entity_aliases

```sql
CREATE TABLE entity_aliases (
  original_name TEXT PRIMARY KEY,
  canonical_name TEXT NOT NULL,
  reasoning TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  created_by TEXT DEFAULT 'llm_dedupe'
);
```
