import Database from 'better-sqlite3';
import * as fs from 'fs';
import * as path from 'path';

const db = new Database('document_analysis.db');

console.log('Adding full_text column to documents table...');

// Add the column (will fail if it already exists, which is fine)
try {
  db.exec('ALTER TABLE documents ADD COLUMN full_text TEXT');
  console.log('✓ Added full_text column');
} catch (error: any) {
  if (error.message.includes('duplicate column name')) {
    console.log('✓ full_text column already exists');
  } else {
    throw error;
  }
}

// Get all documents
const documents = db.prepare('SELECT id, doc_id, file_path FROM documents').all() as Array<{
  id: number;
  doc_id: string;
  file_path: string;
}>;

console.log(`\nMigrating ${documents.length} documents...`);

const updateStmt = db.prepare('UPDATE documents SET full_text = ? WHERE id = ?');
let successCount = 0;
let errorCount = 0;

for (const doc of documents) {
  try {
    const fullPath = path.join(process.cwd(), doc.file_path);
    const text = fs.readFileSync(fullPath, 'utf-8');
    updateStmt.run(text, doc.id);
    successCount++;
    if (successCount % 100 === 0) {
      console.log(`  Migrated ${successCount}/${documents.length} documents...`);
    }
  } catch (error) {
    console.error(`  ✗ Failed to read ${doc.file_path}:`, error);
    errorCount++;
  }
}

console.log(`\n✓ Migration complete!`);
console.log(`  Success: ${successCount} documents`);
console.log(`  Errors: ${errorCount} documents`);

db.close();
