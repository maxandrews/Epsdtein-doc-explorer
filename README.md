# Epstein Document Network Explorer

An intelligent document analysis and network visualization system that processes legal documents to extract relationships, entities, and events, then visualizes them as an interactive knowledge graph.

> **Note:** Additional documents are being continuously processed from the House Oversight Committee releases.

**[Live Demo](https://epstein-doc-explorer-1.onrender.com/)** | **[Source Documents](https://drive.google.com/drive/folders/1ldncvdqIf6miiskDp_EDuGSDAaI_fJx8)**

---

## Features

- **AI-Powered Extraction** - Claude AI extracts entities, relationships, and events from documents
- **Interactive Network Graph** - Force-directed visualization with 15,000+ relationships
- **Smart Filtering** - Filter by 30 semantic categories, hop distance, date range, keywords
- **Entity Deduplication** - Automatic merging of duplicate names
- **Timeline View** - Chronological relationship browser with document links
- **Responsive Design** - Works on desktop and mobile

---

## Quick Start

```bash
# Clone and install
git clone https://github.com/jslabxyz/Epstein-doc-explorer.git
cd Epstein-doc-explorer
npm install && cd network-ui && npm install && cd ..

# Run (two terminals)
npx tsx api_server.ts              # API server
cd network-ui && npm run dev       # Frontend
```

**Access:** http://localhost:5173

---

## Documentation

| Document | Description |
|----------|-------------|
| **[Setup Guide](./docs/SETUP.md)** | Local development setup |
| **[API Reference](./docs/API.md)** | REST API endpoints |
| **[Architecture](./docs/ARCHITECTURE.md)** | System design and data flow |
| **[Deployment](./docs/DEPLOYMENT.md)** | Production deployment (Render, Docker, VPS) |
| **[Analysis Pipeline](./docs/ANALYSIS_PIPELINE.md)** | Running document analysis |
| **[Troubleshooting](./docs/TROUBLESHOOTING.md)** | Common issues and solutions |
| **[Contributing](./CONTRIBUTING.md)** | How to contribute |

---

## Architecture

```
┌──────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Analysis        │     │   API Server     │     │   Frontend      │
│  Pipeline        │────▶│   (Express)      │◀────│   (React)       │
│  (Claude AI)     │     │                  │     │                 │
└────────┬─────────┘     └────────┬─────────┘     └─────────────────┘
         │                        │
         └────────────────────────┘
                      │
                      ▼
              SQLite Database
```

**Tech Stack:**
- **Backend:** TypeScript, Express.js, SQLite, Claude AI
- **Frontend:** React 19, Vite, TailwindCSS, D3.js, react-force-graph

---

## Project Structure

```
├── api_server.ts              # Express API server
├── document_analysis.db       # SQLite database
├── tag_clusters.json          # Semantic tag clusters
├── analysis_pipeline/         # Document analysis scripts
├── network-ui/                # React frontend
└── docs/                      # Documentation
```

---

## License

MIT License - See [LICENSE](./LICENSE) for details.

---

## Credits

- Source documents from House Oversight Committee releases
- Special thanks to [u/tensonaut](https://huggingface.co/datasets/tensonaut/EPSTEIN_FILES_20K) for OCR extraction
