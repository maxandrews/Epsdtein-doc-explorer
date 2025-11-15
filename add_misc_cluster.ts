#!/usr/bin/env node

import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const DB_PATH = process.env.DB_PATH || 'document_analysis.db';
const db = new Database(DB_PATH);

// Load tag clusters
const clustersPath = path.join(process.cwd(), 'tag_clusters.json');
let tagClusters = JSON.parse(fs.readFileSync(clustersPath, 'utf-8'));

console.log(`Loaded ${tagClusters.length} existing tag clusters`);

// Check if misc cluster already exists
const miscCluster = tagClusters.find((c: any) => c.name === 'Misc');
let miscClusterId: number;

if (miscCluster) {
  miscClusterId = miscCluster.id;
  console.log(`✓ "Misc" cluster already exists with ID ${miscClusterId}`);
} else {
  // Create new misc cluster with ID 20
  miscClusterId = 20;
  const newMiscCluster = {
    id: miscClusterId,
    name: 'Misc',
    exemplars: ['uncategorized', 'other', 'miscellaneous'],
    tags: []
  };

  tagClusters.push(newMiscCluster);

  // Save updated clusters
  fs.writeFileSync(clustersPath, JSON.stringify(tagClusters, null, 2));
  console.log(`✓ Created "Misc" cluster with ID ${miscClusterId}`);
}

// Get all triples
console.log('Fetching all triples...');
const triples = db.prepare(`
  SELECT id, top_cluster_ids FROM rdf_triples
`).all() as Array<{ id: number; top_cluster_ids: string | null }>;

console.log(`Processing ${triples.length} triples...`);

// Update triples that don't have any cluster assignments
const updateStmt = db.prepare(`
  UPDATE rdf_triples
  SET top_cluster_ids = ?
  WHERE id = ?
`);

let updated = 0;
const batchSize = 1000;

db.exec('BEGIN TRANSACTION');

for (const triple of triples) {
  try {
    let topClusters: number[] = [];

    if (triple.top_cluster_ids) {
      topClusters = JSON.parse(triple.top_cluster_ids);
    }

    // If no clusters assigned, add misc cluster
    if (topClusters.length === 0) {
      topClusters = [miscClusterId];
      updateStmt.run(JSON.stringify(topClusters), triple.id);
      updated++;

      if (updated % batchSize === 0) {
        db.exec('COMMIT');
        console.log(`Updated ${updated} triples...`);
        db.exec('BEGIN TRANSACTION');
      }
    }
  } catch (error) {
    console.error(`Error processing triple ${triple.id}:`, error);
  }
}

db.exec('COMMIT');

console.log(`✓ Completed! Updated ${updated} triples to include "Misc" cluster`);
console.log(`✓ ${triples.length - updated} triples already had cluster assignments`);

db.close();
console.log('✓ Migration complete!');
