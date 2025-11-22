# Troubleshooting Guide

Common issues and solutions for the Epstein Document Network Explorer.

## Quick Diagnostics

```bash
# Check if API server is running
curl http://localhost:3001/health

# Check database exists and has data
sqlite3 document_analysis.db "SELECT COUNT(*) FROM documents;"
sqlite3 document_analysis.db "SELECT COUNT(*) FROM rdf_triples;"

# Check Node.js version
node --version  # Should be 18.x or higher

# Check port availability
lsof -ti:3001
lsof -ti:5173
```

---

## Server Issues

### Port Already in Use

**Error:**
```
Error: listen EADDRINUSE: address already in use :::3001
```

**Solution:**
```bash
# Find and kill the process using port 3001
lsof -ti:3001 | xargs kill -9

# Or use a different port
PORT=3002 npx tsx api_server.ts
```

---

### Database Not Found

**Error:**
```
SQLITE_CANTOPEN: unable to open database file
```

**Solutions:**

1. **Verify database exists:**
   ```bash
   ls -la document_analysis.db
   ```

2. **Check DB_PATH environment variable:**
   ```bash
   echo $DB_PATH
   # Should match the actual database location
   ```

3. **Fix with absolute path:**
   ```bash
   DB_PATH=/full/path/to/document_analysis.db npx tsx api_server.ts
   ```

---

### Database Locked

**Error:**
```
SQLITE_BUSY: database is locked
```

**Cause:** Another process is accessing the database.

**Solutions:**

1. **Close other database connections:**
   ```bash
   # Find processes using the database
   fuser document_analysis.db
   ```

2. **Ensure WAL mode is enabled** (already configured in api_server.ts):
   ```bash
   sqlite3 document_analysis.db "PRAGMA journal_mode=WAL;"
   ```

3. **Remove stale lock files:**
   ```bash
   rm -f document_analysis.db-wal document_analysis.db-shm
   ```

---

### Server Crashes on Startup

**Symptom:** Server exits immediately after starting.

**Check the logs for specific errors:**

1. **Missing dependencies:**
   ```bash
   npm install
   ```

2. **TypeScript compilation errors:**
   ```bash
   npx tsc --noEmit  # Check for type errors
   ```

3. **Missing tag clusters file:**
   ```bash
   ls -la tag_clusters.json
   # If missing, regenerate:
   npx tsx analysis_pipeline/cluster_tags.ts
   ```

---

## Frontend Issues

### Can't Connect to API

**Symptom:** Network graph doesn't load, console shows fetch errors.

**Solutions:**

1. **Verify API server is running:**
   ```bash
   curl http://localhost:3001/health
   ```

2. **Check VITE_API_BASE_URL:**
   ```bash
   # network-ui/.env.local
   VITE_API_BASE_URL=http://localhost:3001
   ```

3. **Restart the dev server after changing env vars:**
   ```bash
   cd network-ui && npm run dev
   ```

---

### CORS Errors

**Error in console:**
```
Access to fetch at 'http://localhost:3001/api/stats' from origin 'http://localhost:5173' has been blocked by CORS policy
```

**Solutions:**

1. **Add origin to allowed list:**
   ```bash
   ALLOWED_ORIGINS=http://localhost:5173,http://localhost:3000 npx tsx api_server.ts
   ```

2. **Check for trailing slashes** - Origins must match exactly:
   ```
   ✓ http://localhost:5173
   ✗ http://localhost:5173/
   ```

3. **Protocol mismatch:**
   ```
   ✓ https://example.com (if using HTTPS)
   ✗ http://example.com (won't match HTTPS origin)
   ```

---

### Graph Not Rendering

**Symptom:** Sidebars appear but graph area is blank.

**Solutions:**

1. **Check browser console for errors**

2. **Verify data is loading:**
   ```bash
   curl "http://localhost:3001/api/relationships?limit=100"
   ```

3. **Try reducing the limit:**
   - Default is 9600 on desktop
   - Try lowering to 1000 to test

4. **Clear browser cache:**
   ```bash
   # Or hard refresh
   Ctrl+Shift+R (Windows/Linux)
   Cmd+Shift+R (Mac)
   ```

---

### Build Errors

**Symptom:** `npm run build` fails in network-ui.

**Common causes:**

1. **TypeScript errors:**
   ```bash
   cd network-ui && npx tsc --noEmit
   ```

2. **Missing dependencies:**
   ```bash
   cd network-ui && rm -rf node_modules && npm install
   ```

3. **ESLint errors:**
   ```bash
   cd network-ui && npm run lint
   ```

---

## Performance Issues

### Slow Graph Rendering

**Symptom:** Graph is laggy or browser becomes unresponsive.

**Solutions:**

1. **Reduce relationship limit:**
   - Use the slider to reduce from 9600 to 3000-5000

2. **Apply filters:**
   - Select specific tag clusters instead of all
   - Reduce hop distance from Jeffrey Epstein

3. **Use Chrome** - Generally better WebGL performance

4. **Check system resources:**
   - Close other browser tabs
   - Check available RAM

---

### Slow API Responses

**Symptom:** API requests take >5 seconds.

**Diagnostics:**
```bash
# Time a request
time curl "http://localhost:3001/api/relationships?limit=5000"
```

**Solutions:**

1. **Check database indexes exist:**
   ```bash
   sqlite3 document_analysis.db ".indexes"
   ```

2. **Rebuild indexes if needed:**
   ```sql
   DROP INDEX IF EXISTS idx_rdf_triples_actor;
   CREATE INDEX idx_rdf_triples_actor ON rdf_triples(actor);
   -- Repeat for other indexes
   ```

3. **Check disk I/O:**
   - Database should be on SSD if possible
   - Avoid network-mounted filesystems

4. **Reduce query scope:**
   - Lower the limit parameter
   - Apply cluster filters

---

### High Memory Usage

**Symptom:** Node.js process using >1GB RAM.

**Cause:** Large result sets being processed in memory.

**Solutions:**

1. **The 100k row limit is already configured** in api_server.ts

2. **Reduce client-side limits:**
   - Use lower values in the limit slider

3. **Restart the server periodically** if memory grows over time

---

## Data Issues

### Missing Actors/Relationships

**Symptom:** Known actors don't appear in search.

**Diagnostics:**
```bash
# Check if actor exists in database
sqlite3 document_analysis.db "SELECT COUNT(*) FROM rdf_triples WHERE actor LIKE '%Epstein%';"

# Check for alias
sqlite3 document_analysis.db "SELECT * FROM entity_aliases WHERE original_name LIKE '%Epstein%';"
```

**Solutions:**

1. **Entity might be an alias:**
   - Search for the canonical name instead
   - Check entity_aliases table

2. **Filtered out by clusters:**
   - Enable all tag clusters and try again

3. **Filtered by hop distance:**
   - Set hop distance to "Any" to include all actors

---

### Wrong Data Displayed

**Symptom:** Relationships seem incorrect or duplicated.

**Check for data issues:**
```bash
# Check for duplicate relationships
sqlite3 document_analysis.db "
  SELECT actor, action, target, COUNT(*) as cnt
  FROM rdf_triples
  GROUP BY actor, action, target
  HAVING cnt > 10
  ORDER BY cnt DESC
  LIMIT 20;
"
```

**Note:** The API already deduplicates edges between the same actor pairs. Multiple relationships between A and B are grouped into a single visual edge.

---

## Deployment Issues

### Render Build Fails

**Common causes:**

1. **build.sh not executable:**
   ```bash
   git update-index --chmod=+x build.sh
   git commit -m "Make build.sh executable"
   git push
   ```

2. **Node.js version mismatch:**
   - Ensure Render uses Node 18+
   - Add to package.json:
   ```json
   "engines": {
     "node": ">=18.0.0"
   }
   ```

3. **Missing database file:**
   - Ensure DB_PATH points to persistent disk location

---

### 502 Bad Gateway

**Cause:** The Node.js process isn't running or crashed.

**Solutions:**

1. **Check Render logs** for crash details

2. **Verify PORT environment variable:**
   - Render expects the app to listen on `PORT`
   - Our server reads `process.env.PORT`

3. **Health check failing:**
   - Verify `/health` endpoint returns 200

---

## Getting More Help

### Collecting Debug Information

When reporting issues, include:

1. **Node.js version:** `node --version`
2. **npm version:** `npm --version`
3. **OS:** macOS/Windows/Linux
4. **Browser:** Chrome/Firefox/Safari version
5. **Error messages:** Full console output
6. **Steps to reproduce**

### Log Locations

- **API Server:** stdout/stderr
- **Frontend Dev:** Browser console + Vite terminal
- **Render:** Dashboard → Logs

### Contact

- **GitHub Issues:** Report bugs and feature requests
- **Discussions:** Questions and community help
