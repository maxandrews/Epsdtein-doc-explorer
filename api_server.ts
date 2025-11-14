#!/usr/bin/env node

import express from 'express';
import cors from 'cors';
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const app = express();
const PORT = process.env.PORT || 3001;
const DB_PATH = process.env.DB_PATH || 'document_analysis.db';
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS?.split(',') || ['http://localhost:5173', 'http://localhost:3000'];

// CORS configuration with origin whitelist
app.use(cors({
  origin: (origin, callback) => {
    // Allow requests with no origin (like mobile apps or curl requests)
    if (!origin) return callback(null, true);
    if (ALLOWED_ORIGINS.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
  maxAge: 86400
}));

// Request size limits
app.use(express.json({ limit: '10mb' }));

// Simple rate limiting middleware
const requestCounts = new Map<string, { count: number; resetTime: number }>();
const RATE_LIMIT_WINDOW = 15 * 60 * 1000; // 15 minutes
const RATE_LIMIT_MAX_REQUESTS = 1000; // Max requests per window

app.use((req, res, next) => {
  const ip = req.ip || req.socket.remoteAddress || 'unknown';
  const now = Date.now();
  const userData = requestCounts.get(ip);

  if (!userData || now > userData.resetTime) {
    requestCounts.set(ip, { count: 1, resetTime: now + RATE_LIMIT_WINDOW });
    return next();
  }

  if (userData.count >= RATE_LIMIT_MAX_REQUESTS) {
    return res.status(429).json({ error: 'Too many requests, please try again later' });
  }

  userData.count++;
  next();
});

// Initialize database with error handling
let db: Database.Database;
try {
  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL'); // Enable WAL mode for better concurrency
  console.log(`✓ Database initialized: ${DB_PATH}`);
} catch (error) {
  console.error('Failed to initialize database:', error);
  process.exit(1);
}

// Load tag clusters with error handling
let tagClusters: any[] = [];
try {
  const clustersPath = path.join(process.cwd(), 'tag_clusters.json');
  tagClusters = JSON.parse(fs.readFileSync(clustersPath, 'utf-8'));
  console.log(`✓ Loaded ${tagClusters.length} tag clusters`);
} catch (error) {
  console.error('Failed to load tag clusters:', error);
  tagClusters = [];
}

// Get all actors (nodes) with alias resolution
app.get('/api/actors', (req, res) => {
  try {
    const actors = db.prepare(`
      SELECT DISTINCT
        COALESCE(ea.canonical_name, rt.actor) as name,
        COUNT(*) as connection_count
      FROM rdf_triples rt
      LEFT JOIN entity_aliases ea ON rt.actor = ea.original_name
      GROUP BY COALESCE(ea.canonical_name, rt.actor)
      ORDER BY connection_count DESC
      LIMIT 100
    `).all();
    res.json(actors);
  } catch (error) {
    console.error('Error in /api/actors:', error);
    res.status(500).json({ error: 'An internal error occurred' });
  }
});

// Helper function to validate and sanitize inputs
function validateLimit(limit: any): number {
  const parsed = parseInt(limit);
  if (isNaN(parsed) || parsed < 1) return 500;
  return Math.min(20000, Math.max(1, parsed));
}

function validateClusterIds(clusters: any): number[] {
  if (!clusters) return [];
  return String(clusters)
    .split(',')
    .map(Number)
    .filter(n => !isNaN(n) && n >= 0 && Number.isInteger(n))
    .slice(0, 50); // Limit to 50 clusters max
}

// Get all relationships (edges) with distance-based pruning
app.get('/api/relationships', (req, res) => {
  try {
    const limit = validateLimit(req.query.limit);
    const clusterIds = validateClusterIds(req.query.clusters);
    const EPSTEIN_NAME = 'Jeffrey Epstein';

    // Build set of tags in selected clusters for filtering
    const selectedTags = new Set<string>();
    if (clusterIds.length > 0) {
      clusterIds.forEach(clusterId => {
        const cluster = tagClusters.find((c: any) => c.id === clusterId);
        if (cluster) {
          cluster.tags.forEach((tag: string) => selectedTags.add(tag));
        }
      });
    }

    // Fetch relationships with alias resolution and triple_tags
    // Apply database-level LIMIT to prevent memory exhaustion
    const MAX_DB_LIMIT = 50000; // Maximum rows to fetch from database
    const allRelationships = db.prepare(`
      SELECT
        rt.id,
        rt.doc_id,
        rt.timestamp,
        COALESCE(ea_actor.canonical_name, rt.actor) as actor,
        rt.action,
        COALESCE(ea_target.canonical_name, rt.target) as target,
        rt.location,
        rt.triple_tags
      FROM rdf_triples rt
      LEFT JOIN entity_aliases ea_actor ON rt.actor = ea_actor.original_name
      LEFT JOIN entity_aliases ea_target ON rt.target = ea_target.original_name
      WHERE rt.timestamp IS NULL OR rt.timestamp >= '1970-01-01'
      ORDER BY rt.timestamp
      LIMIT ?
    `).all(MAX_DB_LIMIT) as Array<{
      id: number;
      doc_id: string;
      timestamp: string | null;
      actor: string;
      action: string;
      target: string;
      location: string | null;
      triple_tags: string | null;
    }>;

    // Filter by tag clusters if specified
    const filteredRelationships = allRelationships.filter(rel => {
      if (selectedTags.size === 0) return true; // No filter

      try {
        const tags = rel.triple_tags ? JSON.parse(rel.triple_tags) : [];
        return tags.some((tag: string) => selectedTags.has(tag));
      } catch {
        return false;
      }
    });

    // Build adjacency list for BFS
    const adjacency = new Map<string, Set<string>>();

    filteredRelationships.forEach(rel => {
      if (!adjacency.has(rel.actor)) adjacency.set(rel.actor, new Set());
      if (!adjacency.has(rel.target)) adjacency.set(rel.target, new Set());
      adjacency.get(rel.actor)!.add(rel.target);
      adjacency.get(rel.target)!.add(rel.actor);
    });

    // BFS to calculate distances from Jeffrey Epstein
    const distances = new Map<string, number>();
    const queue: string[] = [];

    if (adjacency.has(EPSTEIN_NAME)) {
      distances.set(EPSTEIN_NAME, 0);
      queue.push(EPSTEIN_NAME);

      while (queue.length > 0) {
        const current = queue.shift()!;
        const currentDistance = distances.get(current)!;

        const neighbors = adjacency.get(current) || new Set();
        neighbors.forEach(neighbor => {
          if (!distances.has(neighbor)) {
            distances.set(neighbor, currentDistance + 1);
            queue.push(neighbor);
          }
        });
      }
    }

    // Assign distance score to each relationship (minimum distance of the two actors)
    const relationshipsWithDistance = filteredRelationships.map(rel => {
      const actorDistance = distances.get(rel.actor) ?? Infinity;
      const targetDistance = distances.get(rel.target) ?? Infinity;
      const minDistance = Math.min(actorDistance, targetDistance);

      return {
        ...rel,
        _distance: minDistance
      };
    });

    // Sort by distance (closest first) and take top limit
    relationshipsWithDistance.sort((a, b) => a._distance - b._distance);
    const prunedRelationships = relationshipsWithDistance.slice(0, limit);

    // Remove the _distance field before sending
    const result = prunedRelationships.map(({ _distance, triple_tags, ...rel }) => ({
      ...rel,
      tags: triple_tags ? JSON.parse(triple_tags) : []
    }));

    res.json(result);
  } catch (error) {
    console.error('Error in /api/relationships:', error);
    res.status(500).json({ error: 'An internal error occurred' });
  }
});

// Get relationships for specific actor with alias resolution and cluster filtering
app.get('/api/actor/:name/relationships', (req, res) => {
  try {
    const { name } = req.params;

    // Validate actor name
    if (!name || name.length > 200) {
      return res.status(400).json({ error: 'Invalid actor name' });
    }

    const clusterIds = validateClusterIds(req.query.clusters);

    // Build set of tags in selected clusters for filtering
    const selectedTags = new Set<string>();
    if (clusterIds.length > 0) {
      clusterIds.forEach(clusterId => {
        const cluster = tagClusters.find((c: any) => c.id === clusterId);
        if (cluster) {
          cluster.tags.forEach((tag: string) => selectedTags.add(tag));
        }
      });
    }

    // Find all aliases for this name (if it's a canonical name)
    // OR find the canonical name if this is an alias
    const aliasQuery = db.prepare(`
      SELECT original_name FROM entity_aliases WHERE canonical_name = ?
      UNION
      SELECT canonical_name FROM entity_aliases WHERE original_name = ?
      UNION
      SELECT ? as name
    `).all(name, name, name);

    const allNames = aliasQuery.map((row: any) => row.original_name || row.canonical_name || row.name);
    const placeholders = allNames.map(() => '?').join(',');

    const allRelationships = db.prepare(`
      SELECT
        rt.id,
        rt.doc_id,
        rt.timestamp,
        COALESCE(ea_actor.canonical_name, rt.actor) as actor,
        rt.action,
        COALESCE(ea_target.canonical_name, rt.target) as target,
        rt.location,
        rt.triple_tags
      FROM rdf_triples rt
      LEFT JOIN entity_aliases ea_actor ON rt.actor = ea_actor.original_name
      LEFT JOIN entity_aliases ea_target ON rt.target = ea_target.original_name
      WHERE (rt.actor IN (${placeholders}) OR rt.target IN (${placeholders}))
        AND (rt.timestamp IS NULL OR rt.timestamp >= '1970-01-01')
      ORDER BY rt.timestamp
    `).all(...allNames, ...allNames) as Array<{
      id: number;
      doc_id: string;
      timestamp: string | null;
      actor: string;
      action: string;
      target: string;
      location: string | null;
      triple_tags: string | null;
    }>;

    // Filter by tag clusters if specified
    const filteredRelationships = allRelationships.filter(rel => {
      if (selectedTags.size === 0) return true; // No filter

      try {
        const tags = rel.triple_tags ? JSON.parse(rel.triple_tags) : [];
        return tags.some((tag: string) => selectedTags.has(tag));
      } catch {
        return false;
      }
    });

    const result = filteredRelationships.map((rel) => ({
      id: rel.id,
      doc_id: rel.doc_id,
      timestamp: rel.timestamp,
      actor: rel.actor,
      action: rel.action,
      target: rel.target,
      location: rel.location,
      tags: rel.triple_tags ? JSON.parse(rel.triple_tags) : []
    }));

    res.json(result);
  } catch (error) {
    console.error('Error in /api/actor/:name/relationships:', error);
    res.status(500).json({ error: 'An internal error occurred' });
  }
});

// Get statistics with alias resolution
app.get('/api/stats', (req, res) => {
  try {
    const stats = {
      totalDocuments: db.prepare('SELECT COUNT(*) as count FROM documents').get(),
      totalTriples: db.prepare('SELECT COUNT(*) as count FROM rdf_triples').get(),
      totalActors: db.prepare(`
        SELECT COUNT(DISTINCT COALESCE(ea.canonical_name, rt.actor)) as count
        FROM rdf_triples rt
        LEFT JOIN entity_aliases ea ON rt.actor = ea.original_name
      `).get(),
      categories: db.prepare(`
        SELECT category, COUNT(*) as count
        FROM documents
        GROUP BY category
        ORDER BY count DESC
      `).all(),
    };
    res.json(stats);
  } catch (error) {
    console.error('Error in /api/stats:', error);
    res.status(500).json({ error: 'An internal error occurred' });
  }
});

// Search actors with alias resolution
app.get('/api/search', (req, res) => {
  try {
    const query = req.query.q as string;
    if (!query) {
      return res.json([]);
    }

    const results = db.prepare(`
      SELECT DISTINCT
        COALESCE(ea.canonical_name, rt.actor) as name,
        COUNT(*) as connection_count
      FROM rdf_triples rt
      LEFT JOIN entity_aliases ea ON rt.actor = ea.original_name
      WHERE COALESCE(ea.canonical_name, rt.actor) LIKE ?
      GROUP BY COALESCE(ea.canonical_name, rt.actor)
      ORDER BY connection_count DESC
      LIMIT 20
    `).all(`%${query}%`);

    res.json(results);
  } catch (error) {
    console.error('Error in /api/search:', error);
    res.status(500).json({ error: 'An internal error occurred' });
  }
});

// Get document by doc_id
app.get('/api/document/:docId', (req, res) => {
  try {
    const { docId } = req.params;
    const doc = db.prepare(`
      SELECT
        doc_id,
        file_path,
        one_sentence_summary,
        paragraph_summary,
        category,
        date_range_earliest,
        date_range_latest
      FROM documents
      WHERE doc_id = ?
    `).get(docId);

    if (!doc) {
      return res.status(404).json({ error: 'Document not found' });
    }

    res.json(doc);
  } catch (error) {
    console.error('Error in /api/document/:docId:', error);
    res.status(500).json({ error: 'An internal error occurred' });
  }
});

// Get document text from database
app.get('/api/document/:docId/text', (req, res) => {
  try {
    const { docId } = req.params;

    // Validate docId
    if (!docId || docId.length > 100 || /[<>:"|?*]/.test(docId)) {
      return res.status(400).json({ error: 'Invalid document ID' });
    }

    const doc = db.prepare('SELECT full_text FROM documents WHERE doc_id = ?').get(docId) as { full_text: string | null } | undefined;

    if (!doc) {
      return res.status(404).json({ error: 'Document not found' });
    }

    if (!doc.full_text) {
      return res.status(404).json({ error: 'Document text not available' });
    }

    res.json({ text: doc.full_text });
  } catch (error) {
    console.error('Error in /api/document/:docId/text:', error);
    res.status(500).json({ error: 'An internal error occurred' });
  }
});

// Get tag clusters
app.get('/api/tag-clusters', (req, res) => {
  try {
    // Return just the cluster metadata (id, name, exemplars) without all tags
    const clusters = tagClusters.map((cluster: any) => ({
      id: cluster.id,
      name: cluster.name,
      exemplars: cluster.exemplars,
      tagCount: cluster.tags.length
    }));
    res.json(clusters);
  } catch (error) {
    console.error('Error in /api/tag-clusters:', error);
    res.status(500).json({ error: 'An internal error occurred' });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

// Serve static frontend files
const frontendPath = path.join(process.cwd(), 'network-ui', 'dist');
if (fs.existsSync(frontendPath)) {
  app.use(express.static(frontendPath));

  // Serve index.html for all non-API routes (SPA support)
  app.use((req, res, next) => {
    // Skip API routes
    if (req.path.startsWith('/api') || req.path.startsWith('/health')) {
      return next();
    }
    // Serve index.html for all other routes (client-side routing)
    res.sendFile(path.join(frontendPath, 'index.html'));
  });

  console.log(`✓ Serving frontend from ${frontendPath}`);
} else {
  console.log(`⚠ Frontend build not found at ${frontendPath}`);
}

const server = app.listen(PORT, () => {
  console.log(`\n🚀 API Server running at http://localhost:${PORT}`);
  console.log(`📊 Network UI will connect to this server\n`);
});

// Graceful shutdown
const gracefulShutdown = (signal: string) => {
  console.log(`\n${signal} received, closing server gracefully...`);
  server.close(() => {
    console.log('HTTP server closed');
    try {
      db.close();
      console.log('Database connection closed');
    } catch (error) {
      console.error('Error closing database:', error);
    }
    process.exit(0);
  });

  // Force shutdown after 10 seconds
  setTimeout(() => {
    console.error('Forced shutdown after timeout');
    process.exit(1);
  }, 10000);
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
