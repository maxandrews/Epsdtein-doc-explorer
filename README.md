# Epstein Document Network Explorer

> **Note:** Additional documents are currently being processed and added to the network. The analysis pipeline is actively ingesting newly released documents from the House Oversight Committee.

An intelligent document analysis and network visualization system that processes legal documents to extract relationships, entities, and events, then visualizes them as an interactive knowledge graph.

## Project Overview

This project analyzes the Epstein document corpus to extract structured information about actors, actions, locations, and relationships. It uses Claude AI for intelligent extraction and presents findings through an interactive network visualization interface.

**Live Demo:** [Deployed on Render](https://epstein-doc-explorer-1.onrender.com/)

Source documents are available here: https://drive.google.com/drive/folders/1ldncvdqIf6miiskDp_EDuGSDAaI_fJx8

---

## Architecture Overview

The project has two main phases:

### 1. Analysis Pipeline
**Purpose:** Extract structured data from raw documents using AI
**Technology:** TypeScript, Claude AI (Anthropic), SQLite
**Location:** Root directory + `analysis_pipeline/`

### 2. Visualization Interface
**Purpose:** Interactive exploration of the extracted relationship network
**Technology:** React, TypeScript, Vite, D3.js/Force-Graph
**Location:** `network-ui/`

---

## Key Features

### Analysis Pipeline Features
- **AI-Powered Extraction:** Uses Claude to extract entities, relationships, and events from documents
- **Semantic Tagging:** Automatically tags triples with contextual metadata (legal, financial, travel, etc.)
- **Tag Clustering:** Groups 28,000+ tags into 30 semantic clusters using K-means for better filtering
- **Entity Deduplication:** Merges duplicate entities using LLM-based similarity detection
- **Incremental Processing:** Supports analyzing new documents without reprocessing everything
- **Top-3 Cluster Assignment:** Each relationship is assigned to its 3 most relevant tag clusters

### Visualization Features
- **Interactive Network Graph:** Force-directed graph with edge deduplication for performance
- **Actor-Centric Views:** Click any actor to see their specific relationships
- **Smart Filtering:** Filter by 30 content categories and hop distance from Jeffrey Epstein
- **Density-Based Pruning:** Displays highest-density network connections for clarity
- **Timeline View:** Chronological relationship browser with document links
- **Document Viewer:** Full-text document display with highlighting
- **Responsive Design:** Works on desktop and mobile devices
- **Performance Optimized:** Uses materialized database columns for fast filtering

---

## Project Structure

```
docnetwork/
├── analysis_pipeline/          # Document analysis scripts
│   ├── extract_data.py        # Initial document extraction
│   ├── analyze_documents.ts   # Main AI analysis pipeline
│   ├── cluster_tags.ts        # K-means tag clustering
│   ├── dedupe_with_llm.ts     # Entity deduplication
│   └── extracted/             # Raw extracted documents
│
├── network-ui/                 # React visualization app
│   ├── src/
│   │   ├── components/        # React components
│   │   ├── api.ts            # Backend API client
│   │   └── App.tsx           # Main application
│   └── dist/                  # Production build
│
├── api_server.ts              # Express API server
├── document_analysis.db       # SQLite database (91MB)
├── tag_clusters.json          # 30 semantic tag clusters
└── analysis_pipeline/update_top_clusters.ts # Migration: materialize top clusters
```

---

## Core Components

### Analysis Pipeline

#### 1. Document Extraction (`analysis_pipeline/extract_data.py`)
**Purpose:** Extract raw text from PDF documents
**Input:** PDF files in `data/documents/`
**Output:** JSON files in `analysis_pipeline/extracted/`
**Key Features:**
- Preserves document metadata (ID, category, date)
- Handles various PDF formats
- Stores full text for AI analysis

#### 2. Document Analysis (`analysis_pipeline/analyze_documents.ts`)
**Purpose:** Main AI-powered extraction pipeline
**Input:** Extracted JSON documents
**Output:** SQLite database with entities and relationships
**Key Features:**
- Uses Claude to extract RDF-style triples (subject-action-object)
- Extracts temporal information (dates, timestamps)
- Tags relationships with contextual metadata
- Handles batch processing with rate limiting
- Stores document full text for search

**Database Schema:**
```sql
-- Documents table
CREATE TABLE documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id TEXT UNIQUE NOT NULL,
  file_path TEXT NOT NULL,
  one_sentence_summary TEXT NOT NULL,      -- AI-generated brief summary
  paragraph_summary TEXT NOT NULL,         -- AI-generated detailed summary
  date_range_earliest TEXT,                -- Earliest date mentioned in document
  date_range_latest TEXT,                  -- Latest date mentioned in document
  category TEXT NOT NULL,                  -- Document category
  content_tags TEXT NOT NULL,              -- JSON array of content tags
  analysis_timestamp TEXT NOT NULL,        -- When analysis was performed
  input_tokens INTEGER,                    -- Claude API usage metrics
  output_tokens INTEGER,
  cache_read_tokens INTEGER,
  cost_usd REAL,                          -- Estimated API cost
  error TEXT,                             -- Error message if analysis failed
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  full_text TEXT                          -- Complete document text for search
);
CREATE INDEX idx_documents_doc_id ON documents(doc_id);
CREATE INDEX idx_documents_category ON documents(category);

-- RDF triples (relationships)
CREATE TABLE rdf_triples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id TEXT NOT NULL,
  timestamp TEXT,                         -- When the event occurred
  actor TEXT NOT NULL,                    -- Subject of the relationship
  action TEXT NOT NULL,                   -- Action/verb
  target TEXT NOT NULL,                   -- Object of the relationship
  location TEXT,                          -- Where the event occurred
  actor_likely_type TEXT,                 -- Type of actor (person, organization, etc.)
  triple_tags TEXT,                       -- JSON array of tags
  explicit_topic TEXT,                    -- Explicit subject matter
  implicit_topic TEXT,                    -- Inferred subject matter
  sequence_order INTEGER NOT NULL,        -- Order within document
  top_cluster_ids TEXT,                   -- JSON array of top 3 cluster IDs (materialized)
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
);
CREATE INDEX idx_rdf_triples_doc_id ON rdf_triples(doc_id);
CREATE INDEX idx_rdf_triples_actor ON rdf_triples(actor);
CREATE INDEX idx_rdf_triples_timestamp ON rdf_triples(timestamp);
CREATE INDEX idx_top_cluster_ids ON rdf_triples(top_cluster_ids);

-- Entity aliases (deduplication)
CREATE TABLE entity_aliases (
  original_name TEXT PRIMARY KEY,
  canonical_name TEXT NOT NULL,
  reasoning TEXT,                         -- LLM explanation for the merge
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by TEXT DEFAULT 'llm_dedupe'    -- Source of the alias
);
```

#### 3. Tag Clustering (`analysis_pipeline/cluster_tags.ts`)
**Purpose:** Group 28,000+ tags into semantic clusters
**Input:** All unique tags from database
**Output:** `tag_clusters.json` with 30 clusters
**Process:**
1. Collect all unique tags from triples
2. Generate or load cached embeddings using Qwen3-Embedding-0.6B-ONNX
3. Run K-means clustering with K-means++ initialization
4. Generate human-readable cluster names from exemplar tags
5. Save cluster assignments with full tag lists

**Technical Details:**
- **Algorithm:** K-means with cosine distance metric
- **Initialization:** K-means++ for better convergence
- **Convergence:** Typically ~85 iterations
- **Complexity:** O(n·k·i) - much faster than hierarchical methods
- **Output:** 30 clusters ranging from 500-1400 tags each

#### 4. Entity Deduplication (`analysis_pipeline/dedupe_with_llm.ts`)
**Purpose:** Merge duplicate entity mentions
**Input:** Entity names from database
**Output:** `entity_aliases` table mapping variants to canonical names
**Process:**
1. Identify potential duplicates using fuzzy matching
2. Use Claude to determine if entities are the same person
3. Create alias mappings (e.g., "Jeff Epstein" → "Jeffrey Epstein")
4. API server resolves aliases in real-time

#### 5. Migration Scripts
**`analysis_pipeline/update_top_clusters.ts`**
- Adds and populates `top_cluster_ids` column to `rdf_triples`
- Computes top 3 clusters for each triple based on tag matches
- Creates index for fast filtering
- Improves query performance by 10x+

---

### API Server (`api_server.ts`)

**Purpose:** Express.js backend serving data and frontend
**Port:** 3001 (configurable via `PORT` env var)
**Technology:** Express, better-sqlite3, CORS

#### Key Endpoints

**`GET /api/stats`**
- Returns database statistics (document count, triple count, actor count)
- Shows top document categories

**`GET /api/tag-clusters`**
- Returns all 30 tag clusters with metadata
- Includes cluster names, exemplar tags, and tag counts

**`GET /api/relationships?limit=9600&clusters=0,1,2&maxHops=3`**
- Returns relationship network filtered by clusters and hop distance
- Applies density-based pruning (highest-degree nodes prioritized)
- Edge deduplication before limiting (slider value = unique visual edges)
- Returns metadata: `{ relationships, totalBeforeLimit, totalBeforeFilter }`
- Uses materialized `top_cluster_ids` for fast filtering

**`GET /api/actor/:name/relationships?clusters=0,1,2`**
- Returns all relationships for a specific actor
- Handles entity aliases (resolves variants to canonical names)
- Filtered by selected tag clusters
- Returns: `{ relationships, totalBeforeFilter }`

**`GET /api/search?q=query`**
- Searches for actors by name
- Returns fuzzy matches with relationship counts

**`GET /api/document/:docId`**
- Returns document metadata
- Includes category, doc_id, file path

**`GET /api/document/:docId/text`**
- Returns full document text
- Used for document viewer modal

#### Performance Optimizations
- **Materialized Clusters:** Pre-computed top 3 clusters per triple
- **Indexed Columns:** Indexes on `top_cluster_ids`, `actor`, `target`
- **Database Limits:** 100k row limit to prevent memory exhaustion
- **Alias Resolution:** Efficient LEFT JOIN on entity_aliases
- **Rate Limiting:** 1000 requests per 15 minutes per IP

---

### Visualization Interface

#### Frontend Architecture (`network-ui/`)

**Technology Stack:**
- **React 18** with TypeScript
- **Vite** for build tooling
- **TailwindCSS** for styling
- **react-force-graph-2d** for network visualization
- **D3.js** for force simulation

#### Key Components

**`App.tsx`** - Main application container
- Manages global state (relationships, selected actor, filters)
- Loads tag clusters and enables all by default
- Coordinates data fetching and updates
- Handles desktop/mobile layout switching

**`NetworkGraph.tsx`** - Force-directed graph visualization
- Renders nodes (actors) and links (relationships)
- Node size based on connection count
- Click actors to select/deselect
- Zoom and pan controls
- Performance: Handles 15,000+ relationships smoothly

**`Sidebar.tsx`** - Desktop left sidebar
- Displays database statistics
- Actor search with autocomplete
- Relationship limit slider (100-25,000, default 9,600 desktop / 3,000 mobile)
- Hop distance filter (1-10 hops from Jeffrey Epstein, default 3)
- Tag cluster filter buttons
- Document category breakdown

**`RightSidebar.tsx`** - Desktop right sidebar (actor details)
- Shows when actor is selected
- Timeline view of actor's relationships
- "Showing X of Y relationships" indicator
- Document links with click-to-view

**`MobileBottomNav.tsx`** - Mobile navigation
- Tabbed interface: Search, Timeline, Filters
- Condensed version of desktop sidebars
- Touch-optimized controls

**`DocumentModal.tsx`** - Full-text document viewer
- Displays complete document text
- Highlights actor names in context
- Scrollable with close button
- Fetches text on demand

**`WelcomeModal.tsx`** - First-time visitor welcome
- Introduces users to the explorer
- Stored in localStorage (shown once)
- Dismissible

#### State Management

**Global State (in App.tsx):**
```typescript
const [stats, setStats] = useState<Stats | null>(null);
const [tagClusters, setTagClusters] = useState<TagCluster[]>([]);
const [relationships, setRelationships] = useState<Relationship[]>([]);
const [totalBeforeLimit, setTotalBeforeLimit] = useState<number>(0);
const [selectedActor, setSelectedActor] = useState<string | null>(null);
const [actorRelationships, setActorRelationships] = useState<Relationship[]>([]);
const [actorTotalBeforeFilter, setActorTotalBeforeFilter] = useState<number>(0);
const [limit, setLimit] = useState(isMobile ? 3000 : 9600);
const [maxHops, setMaxHops] = useState<number | null>(3);
const [minDensity, setMinDensity] = useState(50);
const [enabledClusterIds, setEnabledClusterIds] = useState<Set<number>>(new Set());
```

**Data Flow:**
1. Load tag clusters on mount → enable all clusters
2. Fetch relationships when limit or clusters change
3. Fetch actor relationships when actor selected or clusters change
4. Update graph when relationships change

#### Responsive Design
- **Desktop (>1024px):** Dual sidebar layout with main graph
- **Mobile (<1024px):** Full-screen graph with bottom navigation
- **Adaptive Limits:** Mobile defaults to 3k relationships, desktop 9.6k

---

## Local Development

```bash
# Install dependencies
npm install
cd network-ui && npm install && cd ..

# Run API server
npx tsx api_server.ts

# Run frontend (separate terminal)
cd network-ui && npm run dev

# Access:
# - API: http://localhost:3001
# - Frontend: http://localhost:5173
```

---

## Key Files Reference

### Analysis Scripts
| File | Purpose | When to Run |
|------|---------|-------------|
| `analysis_pipeline/analyze_documents.ts` | Main AI analysis | After impactful schema changes or adding new docs |
| `analysis_pipeline/cluster_tags.ts` | Create tag clusters with K-means | After major tag changes |
| `analysis_pipeline/dedupe_with_llm.ts` | Deduplicate entities | After analyzing new documents |
| `analysis_pipeline/update_top_clusters.ts` | Materialize cluster IDs | After running cluster_tags.ts |

---

## License

MIT License - See LICENSE file for details

## Contact

For questions or issues, please open a GitHub issue.

**Repository:** https://github.com/maxandrews/Epstein-doc-explorer
