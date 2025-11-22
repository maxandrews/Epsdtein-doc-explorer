# Setup Guide

This guide covers setting up the Epstein Document Network Explorer for local development.

## Prerequisites

### Required Software

| Software | Version | Purpose |
|----------|---------|---------|
| **Node.js** | 18.x or higher | Runtime environment |
| **npm** | 9.x or higher | Package manager |
| **Python** | 3.8+ | PDF text extraction (optional) |

### Optional

- **Anthropic API Key** - Required only for running the analysis pipeline (document extraction)
- **SQLite CLI** - Useful for database inspection

## Quick Start

```bash
# Clone the repository
git clone https://github.com/jslabxyz/Epstein-doc-explorer.git
cd Epstein-doc-explorer

# Install dependencies
npm install
cd network-ui && npm install && cd ..

# Start the API server
npx tsx api_server.ts

# In a new terminal, start the frontend
cd network-ui && npm run dev
```

**Access the application:**
- Frontend: http://localhost:5173
- API: http://localhost:3001

---

## Detailed Setup

### 1. Clone the Repository

```bash
git clone https://github.com/jslabxyz/Epstein-doc-explorer.git
cd Epstein-doc-explorer
```

### 2. Install Dependencies

**Root dependencies (API server):**
```bash
npm install
```

**Frontend dependencies:**
```bash
cd network-ui
npm install
cd ..
```

Or use the build script:
```bash
chmod +x build.sh
./build.sh
```

### 3. Database Setup

The repository includes a pre-built SQLite database (`document_analysis.db`) with analyzed documents. No additional setup is required for basic usage.

**Verify database:**
```bash
# Check database exists and has data
sqlite3 document_analysis.db "SELECT COUNT(*) FROM documents;"
sqlite3 document_analysis.db "SELECT COUNT(*) FROM rdf_triples;"
```

### 4. Environment Variables

Create a `.env` file in the root directory for custom configuration:

```bash
# .env
PORT=3001                          # API server port
DB_PATH=document_analysis.db       # Path to SQLite database
ALLOWED_ORIGINS=http://localhost:5173,http://localhost:3000
```

**All Environment Variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3001` | Port for the API server |
| `DB_PATH` | `document_analysis.db` | Path to the SQLite database file |
| `ALLOWED_ORIGINS` | See below | Comma-separated list of allowed CORS origins |

**Default CORS Origins:**
- `http://localhost:5173`
- `http://localhost:3000`
- `https://epsteinvisualizer.com`
- `https://www.epsteinvisualizer.com`

### 5. Start the Application

**Option A: Development mode (recommended)**

Terminal 1 - API Server:
```bash
npx tsx api_server.ts
```

Terminal 2 - Frontend (with hot reload):
```bash
cd network-ui
npm run dev
```

**Option B: Production build locally**

```bash
# Build frontend
cd network-ui && npm run build && cd ..

# Start server (serves both API and frontend)
npx tsx api_server.ts
```

Access at http://localhost:3001

---

## Frontend Configuration

The frontend uses Vite and connects to the API server via environment variables.

### Environment Variables

Create `network-ui/.env.local` for frontend configuration:

```bash
# network-ui/.env.local
VITE_API_BASE_URL=http://localhost:3001
```

**Frontend Environment Variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `VITE_API_BASE_URL` | `""` (same origin) | API server URL |

---

## Available Scripts

### Root Directory

| Command | Description |
|---------|-------------|
| `npm run api` | Start the API server |
| `npm run analyze` | Run document analysis pipeline |
| `npm run dedupe-llm` | Run entity deduplication |
| `npm run query` | Interactive database queries |

### Frontend (network-ui/)

| Command | Description |
|---------|-------------|
| `npm run dev` | Start development server with hot reload |
| `npm run build` | Build for production |
| `npm run preview` | Preview production build locally |
| `npm run lint` | Run ESLint |

---

## Project Structure

```
Epstein-doc-explorer/
├── api_server.ts              # Express API server
├── document_analysis.db       # SQLite database (pre-built)
├── tag_clusters.json          # Semantic tag clusters
├── build.sh                   # Build automation script
├── package.json               # Root dependencies
│
├── analysis_pipeline/         # Document analysis scripts
│   ├── analyze_documents.ts   # Main AI analysis
│   ├── cluster_tags.ts        # Tag clustering
│   ├── dedupe_with_llm.ts     # Entity deduplication
│   └── ...
│
├── network-ui/                # React frontend
│   ├── src/
│   │   ├── components/        # React components
│   │   ├── App.tsx           # Main application
│   │   ├── api.ts            # API client
│   │   └── types.ts          # TypeScript types
│   ├── package.json          # Frontend dependencies
│   └── vite.config.ts        # Vite configuration
│
├── data/                      # Source documents (not in git)
│   └── ...
│
└── docs/                      # Documentation
    ├── API.md                 # API reference
    ├── SETUP.md               # This file
    └── ...
```

---

## Running the Analysis Pipeline

> **Note:** The analysis pipeline requires an Anthropic API key and source documents. It's only needed if you want to process new documents.

See [Analysis Pipeline Guide](./ANALYSIS_PIPELINE.md) for detailed instructions.

**Quick overview:**

```bash
# Set API key
export ANTHROPIC_API_KEY=your-key-here

# Run analysis on new documents
npm run analyze

# Deduplicate entities
npm run dedupe-llm

# Update tag clusters
npx tsx analysis_pipeline/cluster_tags.ts
```

---

## Troubleshooting

### Common Issues

**Port already in use:**
```bash
# Find and kill process on port 3001
lsof -ti:3001 | xargs kill -9

# Or use a different port
PORT=3002 npx tsx api_server.ts
```

**Database not found:**
```bash
# Verify database exists
ls -la document_analysis.db

# Check DB_PATH environment variable
echo $DB_PATH
```

**CORS errors:**
```bash
# Add your origin to ALLOWED_ORIGINS
ALLOWED_ORIGINS=http://localhost:5173,http://yourhost:port npx tsx api_server.ts
```

**Frontend can't connect to API:**
1. Verify API server is running on expected port
2. Check `VITE_API_BASE_URL` in `network-ui/.env.local`
3. Check browser console for CORS errors

See [Troubleshooting Guide](./TROUBLESHOOTING.md) for more solutions.

---

## Next Steps

- [API Reference](./API.md) - Detailed API documentation
- [Architecture](./ARCHITECTURE.md) - System design and data flow
- [Deployment](./DEPLOYMENT.md) - Production deployment guide
- [Contributing](../CONTRIBUTING.md) - How to contribute
