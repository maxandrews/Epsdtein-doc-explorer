# Deployment Guide

This guide covers deploying the Epstein Document Network Explorer to production environments.

## Deployment Options

| Platform | Recommended For | Notes |
|----------|-----------------|-------|
| **Render** | Primary deployment | Free tier available, persistent disk support |
| **Railway** | Alternative | Similar to Render |
| **VPS** | Self-hosted | Full control, requires more setup |
| **Docker** | Containerized | Portable, reproducible |

---

## Render Deployment (Recommended)

The application is currently deployed on Render at:
https://epstein-doc-explorer-1.onrender.com/

### Prerequisites

- Render account (https://render.com)
- GitHub repository with the codebase
- SQLite database file (`document_analysis.db`)

### Step 1: Create Web Service

1. Log in to Render Dashboard
2. Click **New** → **Web Service**
3. Connect your GitHub repository
4. Configure the service:

| Setting | Value |
|---------|-------|
| **Name** | `epstein-doc-explorer` |
| **Region** | Choose closest to your users |
| **Branch** | `main` |
| **Runtime** | `Node` |
| **Build Command** | `./build.sh` |
| **Start Command** | `npx tsx api_server.ts` |
| **Instance Type** | Free (or Starter for better performance) |

### Step 2: Configure Environment Variables

In the Render dashboard, add these environment variables:

| Variable | Value | Required |
|----------|-------|----------|
| `PORT` | `10000` (Render default) | No |
| `DB_PATH` | `/opt/render/project/src/document_analysis.db` | Yes |
| `ALLOWED_ORIGINS` | `https://yourdomain.com` | Yes |
| `NODE_ENV` | `production` | Recommended |

### Step 3: Set Up Persistent Disk

The SQLite database requires persistent storage:

1. In your web service settings, go to **Disks**
2. Add a new disk:
   - **Name:** `data`
   - **Mount Path:** `/opt/render/project/src/data`
   - **Size:** 1 GB (adjust as needed)

3. Update `DB_PATH` to use the persistent disk:
   ```
   DB_PATH=/opt/render/project/src/data/document_analysis.db
   ```

4. Upload your database to the persistent disk (first deployment):
   ```bash
   # SSH into Render instance or use their shell
   cp document_analysis.db /opt/render/project/src/data/
   ```

### Step 4: Custom Domain (Optional)

1. In Render dashboard, go to **Settings** → **Custom Domains**
2. Add your domain (e.g., `epsteinvisualizer.com`)
3. Configure DNS:
   - Add CNAME record pointing to your Render URL
   - Render provides automatic SSL certificates

4. Update `ALLOWED_ORIGINS` to include your domain:
   ```
   ALLOWED_ORIGINS=https://epsteinvisualizer.com,https://www.epsteinvisualizer.com
   ```

### Render Configuration File

Alternatively, use a `render.yaml` file:

```yaml
# render.yaml
services:
  - type: web
    name: epstein-doc-explorer
    runtime: node
    buildCommand: ./build.sh
    startCommand: npx tsx api_server.ts
    envVars:
      - key: NODE_ENV
        value: production
      - key: DB_PATH
        value: /opt/render/project/src/data/document_analysis.db
      - key: ALLOWED_ORIGINS
        value: https://epsteinvisualizer.com,https://www.epsteinvisualizer.com
    disk:
      name: data
      mountPath: /opt/render/project/src/data
      sizeGB: 1
```

---

## Docker Deployment

### Dockerfile

```dockerfile
# Dockerfile
FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm ci --only=production

COPY network-ui/package*.json ./network-ui/
RUN cd network-ui && npm ci --only=production

# Copy source code
COPY . .

# Build frontend
RUN cd network-ui && npm run build

# Expose port
EXPOSE 3001

# Start server
CMD ["npx", "tsx", "api_server.ts"]
```

### Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  app:
    build: .
    ports:
      - "3001:3001"
    environment:
      - PORT=3001
      - DB_PATH=/app/data/document_analysis.db
      - ALLOWED_ORIGINS=http://localhost:3001
    volumes:
      - ./data:/app/data
    restart: unless-stopped
```

### Build and Run

```bash
# Build image
docker build -t epstein-explorer .

# Run container
docker run -d \
  -p 3001:3001 \
  -v $(pwd)/document_analysis.db:/app/data/document_analysis.db \
  -e DB_PATH=/app/data/document_analysis.db \
  -e ALLOWED_ORIGINS=http://localhost:3001 \
  epstein-explorer

# Or with docker-compose
docker-compose up -d
```

---

## VPS Deployment

### Requirements

- Ubuntu 20.04+ or similar Linux distribution
- Node.js 18+
- nginx (reverse proxy)
- PM2 (process manager)
- SSL certificate (Let's Encrypt recommended)

### Step 1: Server Setup

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Node.js 20
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# Install PM2
sudo npm install -g pm2

# Install nginx
sudo apt install -y nginx
```

### Step 2: Deploy Application

```bash
# Clone repository
cd /var/www
sudo git clone https://github.com/jslabxyz/Epstein-doc-explorer.git
cd Epstein-doc-explorer

# Install dependencies and build
npm install
cd network-ui && npm install && npm run build && cd ..

# Copy database (from local machine)
# scp document_analysis.db user@server:/var/www/Epstein-doc-explorer/
```

### Step 3: Configure PM2

```bash
# Create PM2 ecosystem file
cat > ecosystem.config.js << 'EOF'
module.exports = {
  apps: [{
    name: 'epstein-explorer',
    script: 'npx',
    args: 'tsx api_server.ts',
    cwd: '/var/www/Epstein-doc-explorer',
    env: {
      NODE_ENV: 'production',
      PORT: 3001,
      DB_PATH: '/var/www/Epstein-doc-explorer/document_analysis.db',
      ALLOWED_ORIGINS: 'https://yourdomain.com'
    }
  }]
};
EOF

# Start application
pm2 start ecosystem.config.js

# Save PM2 configuration
pm2 save

# Set PM2 to start on boot
pm2 startup
```

### Step 4: Configure nginx

```nginx
# /etc/nginx/sites-available/epstein-explorer
server {
    listen 80;
    server_name yourdomain.com www.yourdomain.com;

    location / {
        proxy_pass http://localhost:3001;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
    }
}
```

```bash
# Enable site
sudo ln -s /etc/nginx/sites-available/epstein-explorer /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

### Step 5: SSL with Let's Encrypt

```bash
# Install Certbot
sudo apt install -y certbot python3-certbot-nginx

# Obtain certificate
sudo certbot --nginx -d yourdomain.com -d www.yourdomain.com

# Auto-renewal is configured automatically
```

---

## Environment Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `PORT` | Server port | `3001` |
| `DB_PATH` | Path to SQLite database | `/app/data/document_analysis.db` |
| `ALLOWED_ORIGINS` | CORS allowed origins (comma-separated) | `https://example.com` |
| `NODE_ENV` | Environment mode | `production` |

---

## Health Monitoring

### Health Check Endpoint

```
GET /health
```

Response:
```json
{
  "status": "ok",
  "uptime": 3600.123,
  "timestamp": "2024-01-15T12:00:00.000Z"
}
```

### Render Health Checks

Render automatically monitors the `/health` endpoint. Configure in settings:
- **Health Check Path:** `/health`
- **Health Check Interval:** 30 seconds

### External Monitoring

Consider using:
- **UptimeRobot** - Free uptime monitoring
- **Pingdom** - Professional monitoring
- **Better Uptime** - Status pages

---

## Database Considerations

### SQLite in Production

SQLite works well for this application because:
- Read-heavy workload (no writes from API)
- Single-server deployment
- WAL mode enabled for better concurrency

**Limitations:**
- Not suitable for multi-server deployments
- No built-in replication

### Backup Strategy

```bash
# Create backup
sqlite3 document_analysis.db ".backup backup_$(date +%Y%m%d).db"

# Automated daily backup (cron)
0 2 * * * sqlite3 /path/to/document_analysis.db ".backup /backups/backup_$(date +\%Y\%m\%d).db"
```

### Migration to PostgreSQL

For larger scale deployments, consider migrating to PostgreSQL. The schema is compatible with minimal changes.

---

## Performance Tuning

### API Server

The server is already optimized with:
- WAL mode for SQLite
- Database row limits (100k max)
- Rate limiting (1000 req/15min)
- CORS caching (86400 seconds)

### Frontend

Production build includes:
- Code splitting
- Tree shaking
- Minification
- Gzip compression (via nginx/CDN)

### Caching (nginx)

```nginx
# Add to nginx location block
location /api {
    proxy_pass http://localhost:3001;
    # ... other proxy settings

    # Cache API responses (optional)
    proxy_cache_valid 200 5m;
    add_header X-Cache-Status $upstream_cache_status;
}

# Cache static assets
location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2)$ {
    expires 1y;
    add_header Cache-Control "public, immutable";
}
```

---

## Troubleshooting Deployment

### Common Issues

**Build fails on Render:**
- Check Node.js version (requires 18+)
- Verify `build.sh` has execute permissions
- Check build logs for npm errors

**Database not found:**
- Verify `DB_PATH` environment variable
- Check persistent disk is mounted correctly
- Ensure database file exists at the specified path

**CORS errors:**
- Verify `ALLOWED_ORIGINS` includes your domain
- Check for trailing slashes in origins
- Ensure protocol matches (http vs https)

**502 Bad Gateway:**
- Check if the Node.js process is running
- Verify the port configuration
- Check PM2/process manager logs

### Viewing Logs

**Render:**
- Dashboard → Logs tab

**PM2:**
```bash
pm2 logs epstein-explorer
pm2 logs epstein-explorer --lines 100
```

**Docker:**
```bash
docker logs <container_id>
docker logs -f <container_id>  # Follow logs
```

---

## Security Checklist

- [ ] HTTPS enabled (SSL certificate)
- [ ] CORS origins restricted to known domains
- [ ] Rate limiting enabled
- [ ] No sensitive data in environment variables committed to git
- [ ] Database file not publicly accessible
- [ ] Regular security updates for Node.js and dependencies
- [ ] Firewall configured (only ports 80/443 open)
