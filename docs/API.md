# API Reference

The Epstein Document Network Explorer API provides endpoints for querying the document analysis database and retrieving relationship network data.

## Base URL

- **Local Development:** `http://localhost:3001`
- **Production:** `https://epstein-doc-explorer-1.onrender.com`

## Rate Limiting

All endpoints are rate-limited to **1,000 requests per 15 minutes** per IP address.

**Rate Limit Exceeded Response:**
```json
{
  "error": "Too many requests, please try again later"
}
```
**Status Code:** `429 Too Many Requests`

---

## Endpoints

### Health Check

#### `GET /health`

Check if the API server is running.

**Response:**
```json
{
  "status": "ok",
  "uptime": 3600.123,
  "timestamp": "2024-01-15T12:00:00.000Z"
}
```

---

### Statistics

#### `GET /api/stats`

Get database statistics including document counts, relationship counts, and category breakdown.

**Response:**
```json
{
  "totalDocuments": { "count": 1250 },
  "totalTriples": { "count": 45000 },
  "totalActors": { "count": 3200 },
  "categories": [
    { "category": "Legal", "count": 450 },
    { "category": "Financial", "count": 320 },
    { "category": "Travel", "count": 180 }
  ]
}
```

---

### Tag Clusters

#### `GET /api/tag-clusters`

Get all semantic tag clusters used for filtering relationships.

**Response:**
```json
[
  {
    "id": 0,
    "name": "Legal Proceedings",
    "exemplars": ["lawsuit", "deposition", "court filing"],
    "tagCount": 1250
  },
  {
    "id": 1,
    "name": "Financial Transactions",
    "exemplars": ["wire transfer", "payment", "donation"],
    "tagCount": 890
  }
]
```

**Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `id` | number | Unique cluster identifier (used in filtering) |
| `name` | string | Human-readable cluster name |
| `exemplars` | string[] | Sample tags in this cluster |
| `tagCount` | number | Total tags in this cluster |

---

### Relationships

#### `GET /api/relationships`

Get the relationship network with filtering and density-based pruning.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | number | 500 | Max unique edges to return (1-20,000) |
| `clusters` | string | (all) | Comma-separated cluster IDs to include (e.g., `0,1,5`) |
| `categories` | string | (all) | Comma-separated document categories to include |
| `yearMin` | number | - | Minimum year filter (1970-2025) |
| `yearMax` | number | - | Maximum year filter (1970-2025) |
| `includeUndated` | boolean | true | Include relationships without timestamps |
| `keywords` | string | - | Comma-separated keywords for BM25 fuzzy search |
| `maxHops` | number | - | Max hop distance from Jeffrey Epstein (1-10, or "any") |

**Example Request:**
```
GET /api/relationships?limit=5000&clusters=0,1,2&maxHops=3&yearMin=1990&yearMax=2010
```

**Response:**
```json
{
  "relationships": [
    {
      "id": 12345,
      "doc_id": "DOC-001-0045",
      "timestamp": "1998-03-15",
      "actor": "Jeffrey Epstein",
      "action": "met with",
      "target": "John Doe",
      "location": "New York, NY",
      "tags": ["meeting", "social"]
    }
  ],
  "totalBeforeLimit": 15000,
  "totalBeforeFilter": 45000
}
```

**Response Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `relationships` | array | Array of relationship objects |
| `totalBeforeLimit` | number | Unique edges before density pruning |
| `totalBeforeFilter` | number | Total relationships before cluster/category filtering |

**Relationship Object:**
| Field | Type | Description |
|-------|------|-------------|
| `id` | number | Unique relationship ID |
| `doc_id` | string | Source document ID |
| `timestamp` | string\|null | Date of event (ISO format) |
| `actor` | string | Subject/source of relationship |
| `action` | string | Action/verb describing relationship |
| `target` | string | Object/destination of relationship |
| `location` | string\|null | Location where event occurred |
| `tags` | string[] | Semantic tags for this relationship |

**Notes:**
- Relationships are pruned using density-based algorithm (highest-degree nodes prioritized)
- The `limit` parameter controls unique visual edges, not total relationship count
- Entity aliases are automatically resolved to canonical names

---

#### `GET /api/actor/:name/relationships`

Get all relationships for a specific actor.

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Actor name (URL-encoded) |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `clusters` | string | (all) | Comma-separated cluster IDs to include |
| `categories` | string | (all) | Comma-separated document categories |
| `yearMin` | number | - | Minimum year filter |
| `yearMax` | number | - | Maximum year filter |
| `includeUndated` | boolean | true | Include undated relationships |
| `keywords` | string | - | Keywords for BM25 search |
| `maxHops` | number | - | Max hop distance filter |

**Example Request:**
```
GET /api/actor/Jeffrey%20Epstein/relationships?clusters=0,1,2
```

**Response:**
```json
{
  "relationships": [
    {
      "id": 12345,
      "doc_id": "DOC-001-0045",
      "timestamp": "1998-03-15",
      "actor": "Jeffrey Epstein",
      "action": "met with",
      "target": "John Doe",
      "location": "New York, NY",
      "tags": ["meeting", "social"]
    }
  ],
  "totalBeforeFilter": 2500
}
```

**Notes:**
- Handles entity aliases (e.g., "Jeff Epstein" resolves to "Jeffrey Epstein")
- Returns all matching relationships (no density pruning)
- `totalBeforeFilter` is the count before any cluster/category filters

---

### Actors

#### `GET /api/actors`

Get top 100 actors by connection count.

**Response:**
```json
[
  { "name": "Jeffrey Epstein", "connection_count": 2500 },
  { "name": "Ghislaine Maxwell", "connection_count": 1200 },
  { "name": "John Doe", "connection_count": 450 }
]
```

---

#### `GET /api/search`

Search for actors by name.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `q` | string | Search query (partial match supported) |

**Example Request:**
```
GET /api/search?q=epstein
```

**Response:**
```json
[
  { "name": "Jeffrey Epstein", "connection_count": 2500 },
  { "name": "Mark Epstein", "connection_count": 85 }
]
```

**Notes:**
- Returns up to 20 results
- Case-insensitive partial matching
- Results sorted by connection count (descending)

---

#### `GET /api/actor-counts`

Get total relationship counts for top actors (unfiltered).

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | number | 300 | Number of top actors to return |

**Response:**
```json
{
  "Jeffrey Epstein": 2500,
  "Ghislaine Maxwell": 1200,
  "John Doe": 450
}
```

**Notes:**
- Returns unfiltered totals (ignores cluster/category filters)
- Used for displaying accurate node sizes in the graph

---

#### `GET /api/actor/:name/count`

Get total relationship count for a specific actor (unfiltered).

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Actor name (URL-encoded) |

**Response:**
```json
{
  "count": 2500
}
```

---

### Documents

#### `GET /api/document/:docId`

Get document metadata by document ID.

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `docId` | string | Document ID |

**Response:**
```json
{
  "doc_id": "DOC-001-0045",
  "file_path": "data/001_split/DOC-001-0045.pdf",
  "one_sentence_summary": "Deposition transcript discussing financial arrangements.",
  "paragraph_summary": "This document contains a deposition transcript...",
  "category": "Legal",
  "date_range_earliest": "1998-01-01",
  "date_range_latest": "1998-12-31"
}
```

**Error Response (404):**
```json
{
  "error": "Document not found"
}
```

---

#### `GET /api/document/:docId/text`

Get full text content of a document.

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `docId` | string | Document ID |

**Response:**
```json
{
  "text": "DEPOSITION OF JOHN DOE\n\nQ: Please state your name for the record..."
}
```

**Error Responses:**
- `400 Bad Request` - Invalid document ID
- `404 Not Found` - Document not found or text not available

---

## Error Handling

All endpoints return consistent error responses:

**Error Response Format:**
```json
{
  "error": "Error message description"
}
```

**Common HTTP Status Codes:**
| Code | Description |
|------|-------------|
| `200` | Success |
| `400` | Bad Request - Invalid parameters |
| `404` | Not Found - Resource doesn't exist |
| `429` | Too Many Requests - Rate limit exceeded |
| `500` | Internal Server Error |

---

## CORS Configuration

The API accepts requests from:
- `http://localhost:5173` (Vite dev server)
- `http://localhost:3000`
- `https://epsteinvisualizer.com`
- `https://www.epsteinvisualizer.com`
- `*.onrender.com` (Render deployments)

Custom origins can be configured via the `ALLOWED_ORIGINS` environment variable.

---

## Database Schema Reference

For understanding the underlying data structure, see the [Architecture documentation](./ARCHITECTURE.md).

### Key Tables

**`documents`** - Source document metadata and full text
**`rdf_triples`** - Extracted relationships (actor-action-target)
**`entity_aliases`** - Canonical name mappings for deduplication
