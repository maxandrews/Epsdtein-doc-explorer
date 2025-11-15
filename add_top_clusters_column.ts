#!/usr/bin/env node

import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const DB_PATH = process.env.DB_PATH || 'document_analysis.db';
const db = new Database(DB_PATH);

// Load tag clusters
const clustersPath = path.join(process.cwd(), 'tag_clusters.json');
const tagClusters = JSON.parse(fs.readFileSync(clustersPath, 'utf-8'));

console.log(`Loaded ${tagClusters.length} tag clusters`);

// Helper function to get top N clusters for a given set of tags
function getTopClustersForTags(tags: string[], topN: number = 3): number[] {
  const clusterMatchCounts = new Map<number, number>();

  // Count how many tags from each cluster match
  tagClusters.forEach((cluster: any) => {
    const matchCount = tags.filter(tag => cluster.tags.includes(tag)).length;
    if (matchCount > 0) {
      clusterMatchCounts.set(cluster.id, matchCount);
    }
  });

  // Sort clusters by match count (descending) and take top N
  return Array.from(clusterMatchCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([clusterId]) => clusterId);
}

console.log('Adding top_cluster_ids column to rdf_triples table...');

// Add the new column
try {
  db.exec(`ALTER TABLE rdf_triples ADD COLUMN top_cluster_ids TEXT`);
  console.log('✓ Column added successfully');
} catch (error: any) {
  if (error.message.includes('duplicate column name')) {
    console.log('✓ Column already exists, updating values...');
  } else {
    throw error;
  }
}

// Get all triples with their tags
console.log('Fetching all triples...');
const triples = db.prepare(`
  SELECT id, triple_tags FROM rdf_triples
`).all() as Array<{ id: number; triple_tags: string | null }>;

console.log(`Processing ${triples.length} triples...`);

// Update each triple with its top 3 cluster IDs
const updateStmt = db.prepare(`
  UPDATE rdf_triples
  SET top_cluster_ids = ?
  WHERE id = ?
`);

let processed = 0;
const batchSize = 1000;

db.exec('BEGIN TRANSACTION');

for (const triple of triples) {
  try {
    const tags = triple.triple_tags ? JSON.parse(triple.triple_tags) : [];
    const topClusters = getTopClustersForTags(tags, 3);
    const topClustersJson = JSON.stringify(topClusters);

    updateStmt.run(topClustersJson, triple.id);

    processed++;
    if (processed % batchSize === 0) {
      db.exec('COMMIT');
      console.log(`Processed ${processed}/${triples.length} triples...`);
      db.exec('BEGIN TRANSACTION');
    }
  } catch (error) {
    console.error(`Error processing triple ${triple.id}:`, error);
  }
}

db.exec('COMMIT');

console.log(`✓ Completed! Processed ${processed} triples`);

// Create an index on the new column for faster queries
console.log('Creating index on top_cluster_ids...');
try {
  db.exec(`CREATE INDEX IF NOT EXISTS idx_top_cluster_ids ON rdf_triples(top_cluster_ids)`);
  console.log('✓ Index created');
} catch (error) {
  console.error('Error creating index:', error);
}

db.close();
console.log('✓ Migration complete!');
