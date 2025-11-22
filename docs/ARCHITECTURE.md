# Architecture

This document describes the system architecture of the Epstein Document Network Explorer.

## System Overview

The application consists of three main components:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SYSTEM ARCHITECTURE                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐     ┌──────────────────┐     ┌─────────────────┐  │
│  │  Analysis        │     │   API Server     │     │   Frontend      │  │
│  │  Pipeline        │────▶│   (Express)      │◀────│   (React)       │  │
│  │  (TypeScript)    │     │                  │     │                 │  │
│  └────────┬─────────┘     └────────┬─────────┘     └─────────────────┘  │
│           │                        │                                     │
│           ▼                        ▼                                     │
│  ┌──────────────────────────────────────────┐                           │
│  │           SQLite Database                 │                           │
│  │  ┌────────────┐ ┌────────────┐ ┌───────┐ │                           │
│  │  │ documents  │ │ rdf_triples│ │aliases│ │                           │
│  │  └────────────┘ └────────────┘ └───────┘ │                           │
│  └──────────────────────────────────────────┘                           │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. Analysis Pipeline

**Purpose:** Extract structured data from raw documents using AI

**Location:** `analysis_pipeline/`

**Technology Stack:**
- TypeScript
- Claude AI (Anthropic) for extraction
- HuggingFace Transformers for embeddings
- better-sqlite3 for database access

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ANALYSIS PIPELINE FLOW                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  PDF Documents                                                           │
│       │                                                                  │
│       ▼                                                                  │
│  ┌─────────────────┐                                                     │
│  │ extract_data.py │ ── Extract text from PDFs                          │
│  └────────┬────────┘                                                     │
│           ▼                                                              │
│  ┌──────────────────────┐                                                │
│  │ analyze_documents.ts │ ── Claude AI extracts entities & relationships │
│  └────────┬─────────────┘                                                │
│           ▼                                                              │
│  ┌─────────────────────┐                                                 │
│  │ dedupe_with_llm.ts  │ ── Merge duplicate entities                     │
│  └────────┬────────────┘                                                 │
│           ▼                                                              │
│  ┌─────────────────┐                                                     │
│  │ cluster_tags.ts │ ── K-means clustering with embeddings               │
│  └────────┬────────┘                                                     │
│           ▼                                                              │
│  ┌──────────────────────┐                                                │
│  │ update_top_clusters  │ ── Materialize cluster IDs for performance     │
│  └────────┬─────────────┘                                                │
│           ▼                                                              │
│     SQLite Database                                                      │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key Scripts:**

| Script | Purpose |
|--------|---------|
| `extract_data.py` | PDF text extraction |
| `analyze_documents.ts` | AI-powered entity/relationship extraction |
| `dedupe_with_llm.ts` | Entity deduplication using Claude |
| `cluster_tags.ts` | K-means tag clustering with embeddings |
| `update_top_clusters.ts` | Pre-compute top clusters per relationship |

---

### 2. API Server

**Purpose:** Serve data to frontend and handle queries

**Location:** `api_server.ts`

**Technology Stack:**
- Express.js 5.x
- better-sqlite3 (WAL mode)
- CORS with whitelist
- Rate limiting

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         API SERVER ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Incoming Request                                                        │
│       │                                                                  │
│       ▼                                                                  │
│  ┌─────────────┐                                                         │
│  │ Rate Limit  │ ── 1000 requests / 15 min per IP                       │
│  └──────┬──────┘                                                         │
│         ▼                                                                │
│  ┌─────────────┐                                                         │
│  │   CORS      │ ── Whitelist validation                                │
│  └──────┬──────┘                                                         │
│         ▼                                                                │
│  ┌─────────────┐     ┌────────────────────────────────────┐             │
│  │   Router    │────▶│  /api/relationships                │             │
│  │             │     │  /api/actor/:name/relationships    │             │
│  │             │     │  /api/stats                        │             │
│  │             │     │  /api/search                       │             │
│  │             │     │  /api/document/:id                 │             │
│  │             │     │  /api/tag-clusters                 │             │
│  │             │     │  /health                           │             │
│  └──────┬──────┘     └────────────────────────────────────┘             │
│         ▼                                                                │
│  ┌─────────────┐                                                         │
│  │  Database   │ ── SQLite with WAL mode                                │
│  │   Layer     │ ── Alias resolution via JOINs                          │
│  │             │ ── Cluster filtering via materialized column           │
│  └─────────────┘                                                         │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key Features:**
- Entity alias resolution (automatic deduplication)
- Density-based network pruning
- BM25 keyword search
- Hop distance filtering (BFS from Jeffrey Epstein)
- Materialized cluster IDs for fast filtering

---

### 3. Frontend (React)

**Purpose:** Interactive network visualization and exploration

**Location:** `network-ui/`

**Technology Stack:**
- React 19
- TypeScript
- Vite (build tool)
- TailwindCSS (styling)
- react-force-graph-2d (visualization)
- D3.js (force simulation)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       FRONTEND COMPONENT TREE                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  App.tsx                                                                 │
│  ├── State Management (useState hooks)                                   │
│  │   ├── relationships, selectedActor, filters                          │
│  │   └── tagClusters, stats, limit                                      │
│  │                                                                       │
│  ├── Desktop Layout (>1024px)                                            │
│  │   ├── Sidebar.tsx (left)                                              │
│  │   │   ├── Statistics display                                          │
│  │   │   ├── Actor search                                                │
│  │   │   ├── Limit slider                                                │
│  │   │   ├── Hop distance filter                                         │
│  │   │   └── Cluster filter buttons                                      │
│  │   │                                                                   │
│  │   ├── NetworkGraph.tsx (center)                                       │
│  │   │   ├── Force-directed graph                                        │
│  │   │   ├── Node sizing by degree                                       │
│  │   │   └── Click to select actor                                       │
│  │   │                                                                   │
│  │   └── RightSidebar.tsx                                                │
│  │       ├── Selected actor info                                         │
│  │       ├── Relationship timeline                                       │
│  │       └── Document links                                              │
│  │                                                                       │
│  └── Mobile Layout (<1024px)                                             │
│      ├── NetworkGraph.tsx (full screen)                                  │
│      └── MobileBottomNav.tsx                                             │
│          ├── Search tab                                                  │
│          ├── Timeline tab                                                │
│          └── Filters tab                                                 │
│                                                                          │
│  Shared Components:                                                      │
│  ├── DocumentModal.tsx (full-text viewer)                                │
│  └── WelcomeModal.tsx (first-time introduction)                          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Database Schema

### Entity Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATABASE SCHEMA                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────┐         ┌─────────────────────────────────┐    │
│  │      documents      │         │          rdf_triples            │    │
│  ├─────────────────────┤         ├─────────────────────────────────┤    │
│  │ id (PK)             │         │ id (PK)                         │    │
│  │ doc_id (UNIQUE)     │◀────────│ doc_id (FK)                     │    │
│  │ file_path           │    1:N  │ timestamp                       │    │
│  │ one_sentence_summary│         │ actor ─────────────────────┐    │    │
│  │ paragraph_summary   │         │ action                     │    │    │
│  │ date_range_earliest │         │ target                     │    │    │
│  │ date_range_latest   │         │ location                   │    │    │
│  │ category            │         │ actor_likely_type          │    │    │
│  │ content_tags (JSON) │         │ triple_tags (JSON)         │    │    │
│  │ full_text           │         │ top_cluster_ids (JSON)     │    │    │
│  │ analysis_timestamp  │         │ sequence_order             │    │    │
│  │ input_tokens        │         │ created_at                 │    │    │
│  │ output_tokens       │         └───────────────┬─────────────┘    │    │
│  │ cost_usd            │                         │                  │    │
│  │ created_at          │                         │                  │    │
│  └─────────────────────┘                         │                  │    │
│                                                  │                  │    │
│  ┌─────────────────────────────────┐             │                  │    │
│  │       entity_aliases            │             │                  │    │
│  ├─────────────────────────────────┤             │                  │    │
│  │ original_name (PK)              │◀────────────┘                  │    │
│  │ canonical_name                  │  Resolves actor/target names   │    │
│  │ reasoning                       │                                │    │
│  │ created_at                      │                                │    │
│  │ created_by                      │                                │    │
│  └─────────────────────────────────┘                                │    │
│                                                                          │
│  Indexes:                                                                │
│  ├── idx_documents_doc_id ON documents(doc_id)                          │
│  ├── idx_documents_category ON documents(category)                      │
│  ├── idx_rdf_triples_doc_id ON rdf_triples(doc_id)                      │
│  ├── idx_rdf_triples_actor ON rdf_triples(actor)                        │
│  ├── idx_rdf_triples_timestamp ON rdf_triples(timestamp)                │
│  └── idx_top_cluster_ids ON rdf_triples(top_cluster_ids)                │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Table Descriptions

**`documents`** - Source document metadata
- Stores AI-generated summaries
- Contains full text for search
- Tracks API usage and costs

**`rdf_triples`** - Extracted relationships
- RDF-style triples (actor-action-target)
- Semantic tags and cluster assignments
- Temporal and location data

**`entity_aliases`** - Name deduplication
- Maps variant names to canonical forms
- Stores LLM reasoning for transparency
- Applied at query time via JOIN

---

## Data Flow

### 1. Document Analysis Flow

```
PDF File → extract_data.py → JSON → analyze_documents.ts → Claude AI
                                                              │
                                                              ▼
                                                    ┌─────────────────┐
                                                    │ Extracted Data: │
                                                    │ - Summary       │
                                                    │ - RDF Triples   │
                                                    │ - Tags          │
                                                    │ - Dates         │
                                                    └────────┬────────┘
                                                             │
                                                             ▼
                                                      SQLite Database
```

### 2. Query Flow (API)

```
Frontend Request
       │
       ▼
┌──────────────────┐
│ Parse Parameters │ ── limit, clusters, maxHops, keywords
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Build SQL Query  │ ── Dynamic WHERE clauses
│                  │ ── Alias resolution JOINs
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Execute Query    │ ── Fetch from SQLite
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Post-Processing  │ ── Cluster filtering (in-memory)
│                  │ ── BM25 keyword matching
│                  │ ── Density-based pruning
└────────┬─────────┘
         │
         ▼
    JSON Response
```

### 3. Visualization Flow (Frontend)

```
API Response (relationships)
       │
       ▼
┌──────────────────┐
│ Build Node Map   │ ── Unique actors from relationships
│                  │ ── Calculate degree for each node
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Build Link Array │ ── Source/target references
│                  │ ── Deduplicate edges
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Force Simulation │ ── D3.js force layout
│                  │ ── Node collision avoidance
│                  │ ── Link distance by weight
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Canvas Rendering │ ── react-force-graph-2d
│                  │ ── Node sizing by degree
│                  │ ── Zoom/pan controls
└──────────────────┘
```

---

## Performance Optimizations

### Database Level

| Optimization | Description |
|--------------|-------------|
| **WAL Mode** | Concurrent reads during writes |
| **Indexes** | Fast lookups on `doc_id`, `actor`, `timestamp`, `top_cluster_ids` |
| **Materialized Clusters** | Pre-computed top 3 clusters per triple |
| **Row Limits** | Max 100k rows per query to prevent OOM |

### API Level

| Optimization | Description |
|--------------|-------------|
| **Density Pruning** | Show highest-degree nodes first |
| **Edge Deduplication** | Count unique edges, not total relationships |
| **Alias Resolution** | Single JOIN instead of multiple lookups |
| **Rate Limiting** | Prevent abuse (1000 req/15min) |

### Frontend Level

| Optimization | Description |
|--------------|-------------|
| **Adaptive Limits** | 3k mobile, 9.6k desktop default |
| **Canvas Rendering** | 2D canvas instead of SVG DOM |
| **Memoization** | Prevent unnecessary re-renders |
| **Lazy Loading** | Document text fetched on demand |

---

## Security Considerations

### Input Validation
- All query parameters validated and sanitized
- SQL parameterization (no string interpolation)
- Actor names limited to 200 characters
- Document IDs validated against path traversal

### Rate Limiting
- 1000 requests per 15 minutes per IP
- Prevents DoS and abuse

### CORS
- Whitelist of allowed origins
- No wildcard (`*`) in production
- Credentials supported for auth (future)

### Data Safety
- Read-only API (no mutation endpoints)
- Source database never modified by user input
- Graceful shutdown to prevent corruption

---

## Future Architecture Considerations

### Planned: Community Edits System

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     COMMUNITY EDITS ARCHITECTURE                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌───────────────────┐        ┌───────────────────┐                     │
│  │ document_analysis │        │  community_edits  │                     │
│  │     .db           │        │      .db          │                     │
│  │ (read-only)       │        │ (user content)    │                     │
│  │                   │        │                   │                     │
│  │ - documents       │        │ - edit_proposals  │                     │
│  │ - rdf_triples     │◀──────▶│ - edit_votes      │                     │
│  │ - entity_aliases  │  JOIN  │ - comments        │                     │
│  └───────────────────┘        │ - moderation_flags│                     │
│                               └───────────────────┘                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

See [COMMUNITY_EDITS_DESIGN.md](../COMMUNITY_EDITS_DESIGN.md) for details.

### Scaling Considerations

For larger deployments:
- **PostgreSQL** - Replace SQLite for multi-server
- **Redis** - Caching layer for frequent queries
- **CDN** - Static asset caching
- **Read Replicas** - Distribute query load
