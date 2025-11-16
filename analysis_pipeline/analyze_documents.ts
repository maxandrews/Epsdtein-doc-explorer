#!/usr/bin/env node

// Please note: this method uses the agents SDK and assume you are already locally authenticated via claude code via a MAX plan.
// Running this with the API rather than the max plan will cost about $50 for the 2000 epstein emails
import { query } from '@anthropic-ai/claude-agent-sdk';
import { query } from '@anthropic-ai/claude-agent-sdk';
import * as fs from 'fs/promises';
import * as path from 'path';
import Database from 'better-sqlite3';

// Model configuration
const ANALYSIS_MODEL = 'claude-haiku-4-5'; // Fast and cost-effective for document analysis

interface RDFTriple {
  timestamp?: string; // ISO format YYYY-MM-DDTHH:MM or date YYYY-MM-DD
  actor: string;
  action: string;
  target: string;
  location?: string; // Physical location where the action occurred
  actor_likely_type?: string; // Type of unknown/redacted actor
  tags: string[]; // Triple-level tags
  explicit_topic: string; // What the interaction directly says
  implicit_topic: string; // What it likely implies
}

interface DocumentAnalysis {
  doc_id: string;
  one_sentence_summary: string;
  paragraph_summary: string;
  date_range_earliest?: string;
  date_range_latest?: string;
  category: string;
  content_tags: string[];
  rdf_triples: RDFTriple[];
}

interface AnalysisResult {
  doc_id: string;
  file_path: string;
  full_text: string;
  analysis: DocumentAnalysis;
  usage: {
    input_tokens: number;
    output_tokens: number;
    cache_read_input_tokens?: number;
  } | null;
  cost_usd: number;
  error?: string;
}

/**
 * Initialize SQLite database with schema for document analysis
 */
function initDatabase(dbPath: string): Database.Database {
  const db = new Database(dbPath);

  // Enable foreign keys
  db.pragma('foreign_keys = ON');

  // Create documents table
  db.exec(`
    CREATE TABLE IF NOT EXISTS documents (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id TEXT UNIQUE NOT NULL,
      file_path TEXT NOT NULL,
      one_sentence_summary TEXT NOT NULL,
      paragraph_summary TEXT NOT NULL,
      date_range_earliest TEXT,
      date_range_latest TEXT,
      category TEXT NOT NULL,
      content_tags TEXT NOT NULL, -- JSON array
      full_text TEXT,
      analysis_timestamp TEXT NOT NULL,
      input_tokens INTEGER,
      output_tokens INTEGER,
      cache_read_tokens INTEGER,
      cost_usd REAL,
      error TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Create RDF triples table
  db.exec(`
    CREATE TABLE IF NOT EXISTS rdf_triples (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id TEXT NOT NULL,
      timestamp TEXT,
      actor TEXT NOT NULL,
      action TEXT NOT NULL,
      target TEXT NOT NULL,
      location TEXT,
      actor_likely_type TEXT,
      triple_tags TEXT,
      explicit_topic TEXT,
      implicit_topic TEXT,
      sequence_order INTEGER NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
    );
  `);

  // Create indexes for efficient querying
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_documents_doc_id ON documents(doc_id);
    CREATE INDEX IF NOT EXISTS idx_documents_category ON documents(category);
    CREATE INDEX IF NOT EXISTS idx_rdf_triples_doc_id ON rdf_triples(doc_id);
    CREATE INDEX IF NOT EXISTS idx_rdf_triples_actor ON rdf_triples(actor);
    CREATE INDEX IF NOT EXISTS idx_rdf_triples_timestamp ON rdf_triples(timestamp);
  `);

  console.log(`✓ Database initialized at: ${dbPath}\n`);
  return db;
}

/**
 * Analyze a single document using a Claude agent
 */
async function analyzeDocument(
  docId: string,
  filePath: string,
  content: string
): Promise<AnalysisResult> {
  const analysisPrompt = `You are analyzing a document from a legal/investigative document collection. The document ID is "${docId}".

IMPORTANT: You have ALL the information you need in the document text below. Do NOT attempt to read files, explore directories, or gather additional context. Analyze ONLY the text provided.

**CRITICAL IDENTIFICATION RULES:**
This document may contain communications involving Jeffrey Epstein. He may appear under these identifiers:
- Email: jeeitunes@gmail.com
- Email: e:jeeitunes@gmail.com
- Name: jee
- Name: Jeffrey Epstein
- Name: Jeffrey
- Name: Epstein

When you see ANY of these identifiers as a sender, participant, or actor, you MUST use "Jeffrey Epstein" as the actor name in your RDF triples. DO NOT use "jee", "unknown person", or any other placeholder.

Here is the document text:
\`\`\`
${content}
\`\`\`

Your task is to analyze this document and extract structured information. Focus on:

1. **Main actors/participants** - People, organizations, entities mentioned or involved
2. **Key events and actions** - What happened, when, between whom
3. **Temporal information** - Dates, times, sequences of events
4. **Document type and content** - What kind of document is this?
5. **Key themes and topics** - What is this document about?

Return ONLY a valid JSON object with the following structure:

\`\`\`json
{
  "one_sentence_summary": "A brief one-sentence summary including main actors, e.g., 'An email conversation between John Doe and Jane Smith regarding budget approval'",
  "paragraph_summary": "A detailed paragraph (3-5 sentences) explaining the document's content, context, significance, and key points. Include who is involved, what happened, why it matters, and any important outcomes or implications.",
  "date_range_earliest": "YYYY-MM-DD or YYYY-MM-DDTHH:MM format if dates are visible in the document, otherwise null",
  "date_range_latest": "YYYY-MM-DD or YYYY-MM-DDTHH:MM format if dates are visible in the document, otherwise null",
  "category": "One of: court_filing, email, letter, memorandum, report, transcript, financial_document, media_article, book_excerpt, photo_caption, mixed_document, other",
  "content_tags": ["array", "of", "relevant", "document-level", "tags"],
  "rdf_triples": [
    {
      "timestamp": "YYYY-MM-DD or YYYY-MM-DDTHH:MM if available, otherwise omit this field",
      "actor": "PERSON NAME ONLY - Use 'Jeffrey Epstein' when you see jeeitunes@gmail.com or 'jee'",
      "action": "the action verb (e.g., 'sent email to', 'met with', 'testified before', 'paid', 'attended')",
      "target": "PERSON NAME ONLY - not organizations, movies, places (e.g., 'Donald Trump', not 'Donald Trump at party' or '12 Years a Slave')",
      "location": "physical location if mentioned (e.g., 'Mar-a-Lago', 'New York City', 'Palm Beach courthouse'), otherwise omit this field",
      "actor_likely_type": "OPTIONAL - only include if actor is unknown/unnamed/redacted AND there is sufficient evidence to infer their likely type. Type of person - examples include but are not limited to: 'victim', 'witness', 'celebrity', 'political operator', 'staff member', 'law enforcement', 'family member', 'business associate', 'government official'. Use the most specific and appropriate type based on context. Omit entirely if actor is named OR if type cannot be reasonably inferred from context.",
      "tags": ["array", "of", "specific", "tags", "for", "this", "triple", "describing", "the", "nature", "of", "this", "specific", "interaction"],
      "explicit_topic": "short phrase describing the main theme directly evidenced in the surrounding content (e.g., 'biographical facts', 'coordination of business meeting', 'testimony about alleged assault')",
      "implicit_topic": "short phrase describing what the interaction likely relates to, even if not directly stated (e.g., 'relationship cultivation', 'reputation management', 'legal strategy coordination')"
    }
  ]
}
\`\`\`

Guidelines for RDF triples:
- Create a sequential array capturing the key relationships and events in the document
- Include timestamps when dates/times are mentioned in the document
- **CRITICAL - Actor field**: Actor must ALWAYS be a PERSON NAME ONLY
  - ✅ Good: actor: "Jeffrey Epstein" (when you see jeeitunes@gmail.com or jee)
  - ✅ Good: actor: "Donald Trump", "Ghislaine Maxwell"
  - ❌ Bad: actor: "jee" (use "Jeffrey Epstein" instead)
  - ❌ Bad: actor: "FBI" (organization), actor: "United States" (country), actor: "the investigation" (abstract)
  - Only actual human persons can be actors
- **Target field**: Target can be a person, place, organization, or entity
  - ✅ Good: target: "Donald Trump" (person), target: "Hong Kong" (place), target: "FBI" (organization), target: "Mar-a-Lago" (location)
  - ✅ Good: target: "12 Years a Slave" (movie/book), target: "United States Congress" (organization)
  - ❌ Bad: target: "Donald Trump at Mar-a-Lago" (don't combine person with location)
  - If target is a location, ALSO include it in the location field
- **Unknown/Redacted persons**: Use placeholders like "unknown person A", "unknown person B" ONLY when referring to actual unnamed PEOPLE
  - ✅ Good: "unknown person A" for an unnamed victim or redacted individual
  - ❌ Bad: "unknown person A" as placeholder for Jeffrey Epstein when you see jeeitunes@gmail.com or jee
  - **NEW**: When actor is unknown/unnamed/redacted AND you can reasonably infer their type, include "actor_likely_type" field
    - Examples include but are not limited to: "victim", "witness", "celebrity", "political operator", "staff member", "law enforcement", "family member", "business associate", "government official", "legal counsel", "journalist", "minor", "employee", "associate"
    - Choose the most specific and contextually appropriate type that can be reasonably inferred
    - **IMPORTANT**: Only include this field if there is clear contextual evidence. If uncertain or speculative, omit the field entirely rather than guessing
- Use consistent naming (e.g., always "Jeffrey Epstein" not "Epstein" or "Jeffrey" or "jee")
- Actions should be descriptive verb phrases (e.g., "met with", "sent email to", "testified before", "traveled to")
- Focus on person-to-person AND person-to-entity relationships and interactions
- Order triples chronologically when timestamps are available, otherwise by document order
- Extract sufficient triples from each document to accurately capture the nature of relationships and actions documented within

**NEW - Triple-level tags:**
- Each triple should have a "tags" array with specific descriptive tags for THAT INTERACTION
- Tags should describe the nature, context, or category of the specific interaction
- Be specific and descriptive (use snake_case for multi-word tags)
- Examples of triple tags:
  - For "Jeffrey Epstein sent email to Bill Clinton about fundraising": ["email_communication", "political_fundraising", "personal_correspondence"]
  - For "Jane Doe testified before grand jury regarding sexual assault": ["witness_testimony", "sexual_assault_allegations", "legal_proceedings", "grand_jury"]
  - For "Donald Trump met with Vladimir Putin in Helsinki": ["diplomatic_meeting", "international_relations", "summit", "foreign_policy"]
  - For "John Smith transferred $50,000 to offshore account": ["financial_transactions", "offshore_banking", "money_transfer"]
- Triple tags can overlap with or be more specific than document-level tags
- Include 2-6 tags per triple depending on complexity

**NEW - Explicit and Implicit Topics:**
- Each triple must have both an "explicit_topic" and "implicit_topic" field
- Both should be SHORT PHRASES (3-7 words) describing the theme/intent
- **explicit_topic**: What the interaction is DIRECTLY about based on the surrounding text
  - Examples: "biographical facts", "coordination of business meeting", "testimony about alleged assault", "social event attendance", "financial transaction documentation", "legal representation arrangement"
- **implicit_topic**: What the interaction LIKELY relates to or implies, even if not directly stated
  - Examples: "relationship cultivation", "reputation management", "legal strategy coordination", "power networking", "financial concealment", "influence building"
- These help with semantic search and understanding the PURPOSE behind interactions
- Example for "Jeffrey Epstein met with Bill Clinton at Mar-a-Lago":
  - explicit_topic: "social meeting at private club"
  - implicit_topic: "high-level networking and relationship building"
- Example for "Jane Doe testified before grand jury":
  - explicit_topic: "witness testimony in criminal proceedings"
  - implicit_topic: "evidence gathering for prosecution"

Guidelines for document-level content tags:
- Be specific and descriptive (good: "financial_transactions", bad: "money")
- Use snake_case for multi-word tags
- Include both broad topics and specific themes
- Typical tags might include: legal_proceedings, financial_transactions, political_activity, media_coverage, personal_relationships, travel, communications, business_dealings, etc.

If the document is too fragmentary or unreadable to analyze, still provide your best interpretation and mark uncertainty in the summaries.`;

  console.log(`Analyzing ${docId}...`);

  let result = '';
  let usageStats = null;
  let agentCostUSD = 0;

  const agent = query({
    prompt: analysisPrompt,
    options: {
      model: ANALYSIS_MODEL,
      maxTokens: 16000,
      maxTurns: 5,
      allowedTools: [],
    }
  });

  try {
    for await (const message of agent) {
      if (message.type === 'result' && message.subtype === 'success') {
        result = message.result;
        if (message.usage) {
          usageStats = message.usage;
        }
        if (message.total_cost_usd !== undefined) {
          agentCostUSD = message.total_cost_usd;
        }
      } else if (message.type === 'assistant') {
        const textBlocks = message.message.content.filter((c: any) => c.type === 'text');
        for (const block of textBlocks) {
          result += block.text;
        }
      }
    }
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error(`  ❌ Error analyzing ${docId}: ${errorMsg}`);
    return {
      doc_id: docId,
      file_path: filePath,
      analysis: {
        doc_id: docId,
        one_sentence_summary: 'Error during analysis',
        paragraph_summary: 'An error occurred during document analysis.',
        category: 'other',
        content_tags: [],
        rdf_triples: []
      },
      usage: usageStats,
      cost_usd: agentCostUSD,
      error: errorMsg
    };
  }

  // Parse JSON from the result
  const jsonMatch = result.match(/```(?:json)?\s*([\s\S]*?)\s*```/);
  const jsonText = jsonMatch ? jsonMatch[1] : result;
  let analysis: DocumentAnalysis;

  try {
    analysis = JSON.parse(jsonText.trim());
    analysis.doc_id = docId; // Ensure doc_id is set
  } catch (parseError) {
    console.log(`  ⚠️  JSON parse failed for ${docId}, attempting repair...`);

    // Attempt to repair the JSON
    const repairPrompt = `The following JSON response from a document analysis has a parsing error. Please identify and fix the issue, then return ONLY the corrected JSON:

\`\`\`json
${jsonText}
\`\`\`

Parse error: ${parseError instanceof Error ? parseError.message : String(parseError)}

Instructions:
- Find and fix the JSON syntax error (missing quotes, trailing commas, unescaped characters, etc.)
- Ensure all field names are properly quoted
- Ensure all string values are properly escaped
- Return ONLY the valid JSON object, no explanations
- Do NOT modify the content, only fix the syntax`;

    const repairAgent = query({
      prompt: repairPrompt,
      options: {
        model: ANALYSIS_MODEL,
        maxTokens: 16000,
        maxTurns: 3,
        allowedTools: [],
      }
    });

    let repairedText = '';
    try {
      for await (const message of repairAgent) {
        if (message.type === 'result' && message.subtype === 'success') {
          repairedText = message.result;
        } else if (message.type === 'assistant') {
          const textBlocks = message.message.content.filter((c: any) => c.type === 'text');
          for (const block of textBlocks) {
            repairedText += block.text;
          }
        }
      }

      // Extract JSON from repair response
      const repairMatch = repairedText.match(/```(?:json)?\s*([\s\S]*?)```/) || repairedText.match(/\{[\s\S]*\}/);
      if (!repairMatch) {
        throw new Error('No JSON found in repair response');
      }

      const repairedJsonText = repairMatch[1] || repairMatch[0];
      analysis = JSON.parse(repairedJsonText.trim());
      analysis.doc_id = docId;
      console.log(`  ✓ JSON successfully repaired for ${docId}`);
    } catch (repairError) {
      console.error(`  ❌ Repair failed for ${docId}: ${repairError instanceof Error ? repairError.message : String(repairError)}`);
      return {
        doc_id: docId,
        file_path: filePath,
        analysis: {
          doc_id: docId,
          one_sentence_summary: 'Failed to parse analysis',
          paragraph_summary: 'The document analysis could not be parsed correctly.',
          category: 'other',
          content_tags: [],
          rdf_triples: []
        },
        usage: usageStats,
        cost_usd: agentCostUSD,
        error: 'JSON parse error (repair failed)'
      };
    }
  }

  return {
    doc_id: docId,
    file_path: filePath,
    full_text: content,
    analysis,
    usage: usageStats,
    cost_usd: agentCostUSD
  };
}

/**
 * Save analysis results to database
 */
function saveToDatabase(db: Database.Database, result: AnalysisResult): void {
  const insertDoc = db.prepare(`
    INSERT OR REPLACE INTO documents (
      doc_id, file_path, one_sentence_summary, paragraph_summary,
      date_range_earliest, date_range_latest, category, content_tags, full_text,
      analysis_timestamp, input_tokens, output_tokens, cache_read_tokens,
      cost_usd, error
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertTriple = db.prepare(`
    INSERT INTO rdf_triples (
      doc_id, timestamp, actor, action, target, location, actor_likely_type,
      triple_tags, explicit_topic, implicit_topic, sequence_order
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const analysis = result.analysis;

  // Insert document
  insertDoc.run(
    result.doc_id,
    result.file_path,
    analysis.one_sentence_summary,
    analysis.paragraph_summary,
    analysis.date_range_earliest || null,
    analysis.date_range_latest || null,
    analysis.category,
    JSON.stringify(analysis.content_tags),
    result.full_text,
    new Date().toISOString(),
    result.usage?.input_tokens || null,
    result.usage?.output_tokens || null,
    result.usage?.cache_read_input_tokens || null,
    result.cost_usd,
    result.error || null
  );

  // Insert RDF triples in a transaction
  const insertTriplesInTransaction = db.transaction((triples: any[]) => {
    for (let i = 0; i < triples.length; i++) {
      const triple = triples[i];

      // Skip malformed triples that are missing required fields
      if (!triple.actor || !triple.action || !triple.target) {
        console.warn(`Skipping malformed triple in ${result.doc_id}: missing actor, action, or target`);
        continue;
      }

      insertTriple.run(
        result.doc_id,
        triple.timestamp || null,
        triple.actor,
        triple.action,
        triple.target,
        triple.location || null,
        triple.actor_likely_type || null,
        JSON.stringify(triple.tags || []),
        triple.explicit_topic || null,
        triple.implicit_topic || null,
        i
      );
    }
  });

  try {
    insertTriplesInTransaction(analysis.rdf_triples);
  } catch (error) {
    console.error(`Error inserting triples for ${result.doc_id}:`, error);
    throw error;
  }
}

/**
 * Main function to analyze documents
 */
async function main() {
  const args = process.argv.slice(2);
  const dataDir = args[0] || 'data/001_split';
  const maxDocs = args[1] ? parseInt(args[1]) : 100000; // Process all documents by default
  const dbPath = args[2] || 'document_analysis.db';

  console.log(`\n=== Document Analysis Starting ===\n`);
  console.log(`Data directory: ${dataDir}`);
  console.log(`Max documents: ${maxDocs}`);
  console.log(`Database: ${dbPath}\n`);

  // Initialize database
  const db = initDatabase(dbPath);

  // Find all text files
  const files = await fs.readdir(dataDir);
  const allTextFiles = files.filter(f => f.endsWith('.txt'));

  // Get already processed documents
  const processedDocs = new Set(
    db.prepare('SELECT doc_id FROM documents').all().map((r: any) => r.doc_id + '.txt')
  );

  // Filter to unprocessed documents only
  const unprocessedFiles = allTextFiles.filter(f => !processedDocs.has(f));
  const textFiles = unprocessedFiles.slice(0, maxDocs);

  console.log(`Found ${allTextFiles.length} text files total`);
  console.log(`Already processed: ${processedDocs.size}`);
  console.log(`Remaining to analyze: ${unprocessedFiles.length}`);
  console.log(`Analyzing ${textFiles.length} documents in this run\n`);

  const results: AnalysisResult[] = [];
  let totalCost = 0;
  let totalInputTokens = 0;
  let totalOutputTokens = 0;
  let totalCacheReadTokens = 0;

  // Process documents in parallel batches of 20
  const BATCH_SIZE = 20;

  for (let i = 0; i < textFiles.length; i += BATCH_SIZE) {
    const batch = textFiles.slice(i, i + BATCH_SIZE);
    console.log(`\nProcessing batch ${Math.floor(i / BATCH_SIZE) + 1} of ${Math.ceil(textFiles.length / BATCH_SIZE)} (${batch.length} documents)...\n`);

    const batchPromises = batch.map(async (file) => {
      const filePath = path.join(dataDir, file);
      const docId = file.replace('.txt', '');

      try {
        // Read document content
        const content = await fs.readFile(filePath, 'utf-8');

        // Analyze document
        const result = await analyzeDocument(docId, filePath, content);
        return result;
      } catch (error) {
        console.error(`Error processing ${file}:`, error);
        return null;
      }
    });

    const batchResults = await Promise.all(batchPromises);

    for (const result of batchResults) {
      if (result !== null) {
        results.push(result);

        // Update totals
        totalCost += result.cost_usd;
        if (result.usage) {
          totalInputTokens += result.usage.input_tokens;
          totalOutputTokens += result.usage.output_tokens;
          totalCacheReadTokens += result.usage.cache_read_input_tokens || 0;
        }

        // Save to database
        saveToDatabase(db, result);

        console.log(`✓ ${result.doc_id}: ${result.analysis.category} - ${result.analysis.rdf_triples.length} triples`);
      }
    }
  }

  db.close();

  console.log(`\n=== Analysis Complete ===\n`);
  console.log(`Documents analyzed: ${results.length}`);
  console.log(`Total cost: $${totalCost.toFixed(4)}`);
  console.log(`Total tokens: ${(totalInputTokens + totalOutputTokens + totalCacheReadTokens).toLocaleString()}`);
  console.log(`  - Input: ${totalInputTokens.toLocaleString()}`);
  console.log(`  - Output: ${totalOutputTokens.toLocaleString()}`);
  console.log(`  - Cache read: ${totalCacheReadTokens.toLocaleString()}`);

  // Print summary statistics
  const categories = results.reduce((acc, r) => {
    acc[r.analysis.category] = (acc[r.analysis.category] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  console.log(`\nCategories:`);
  for (const [category, count] of Object.entries(categories)) {
    console.log(`  - ${category}: ${count}`);
  }

  const totalTriples = results.reduce((sum, r) => sum + r.analysis.rdf_triples.length, 0);
  console.log(`\nTotal RDF triples extracted: ${totalTriples}`);

  // Save JSON output for inspection
  const jsonOutputPath = 'document_analysis_results.json';
  await fs.writeFile(jsonOutputPath, JSON.stringify(results, null, 2));
  console.log(`\n✓ JSON results saved to: ${jsonOutputPath}`);
  console.log(`✓ Database saved to: ${dbPath}`);
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
