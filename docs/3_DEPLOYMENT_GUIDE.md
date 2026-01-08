# Deployment & Operations Guide

> Docker setup, cloud deployment, CLI usage, and production operations

---

## Table of Contents
1. [Local Docker Setup](#local-docker-setup)
2. [Cloud Deployment](#cloud-deployment)
3. [CLI Reference](#cli-reference)
4. [Production Operations](#production-operations)

---

## 1. Local Docker Setup

### 1.1 Prerequisites
```bash
# Required
- Docker 20.10+
- Docker Compose 2.0+
- 4GB RAM minimum
- 10GB disk space

# Optional
- Git (for version control)
- Python 3.11+ (for local development)
```

### 1.2 Quick Start

```bash
# 1. Clone repository
git clone https://github.com/Akin-ctrl/Stock_pipeline.git
cd Stock_pipeline

# 2. Configure environment
cp .env.example .env
# Edit .env with your settings (email, Slack, etc.)

# 3. Start all services
docker compose up -d --build

# 4. Verify services are running
docker compose ps

# Expected output:
# NAME                    STATUS              PORTS
# stock_pipeline_db       Up (healthy)        5432
# stock_pipeline_app      Up                  -
# stock_pipeline_airflow  Up (healthy)        8080
# stock_pipeline_pgadmin  Up                  5050
# stock_pipeline_scheduler Up (healthy)       -
```

### 1.3 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **PgAdmin** | http://localhost:5050 | admin@stockpipeline.com / admin |
| **PostgreSQL** | localhost:5432 | stockuser / changeme |

### 1.4 First Run

```bash
# Verify database is initialized
docker compose exec db psql -U stockuser -d stock_pipeline -c "\dt"

# Expected tables:
# - dim_sectors
# - dim_stocks
# - fact_daily_prices
# - fact_technical_indicators
# - alert_history

# Check if DAG is loaded (wait 2-3 minutes after startup)
docker compose exec airflow-scheduler airflow dags list | grep nigerian

# Manual trigger (testing)
docker compose exec airflow-scheduler airflow dags trigger nigerian_stock_pipeline

# View logs
docker compose logs -f app
docker compose logs -f airflow-scheduler
```

### 1.5 Container Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Docker Network                         │
│                  (stock_network)                        │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  PostgreSQL  │  │   PgAdmin    │  │  App         │   │
│  │  :5432       │◄─│   :5050      │  │  Container   │   │
│  │  (DB)        │  │  (GUI)       │  │  (ETL)       │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
│         ▲                                    ▲          │
│         │                                    │          │
│  ┌──────────────┐  ┌──────────────┐          │          │
│  │  Airflow     │  │  Airflow     │          │          │
│  │  Webserver   │  │  Scheduler   │──────────┘          │
│  │  :8080       │  │  (Cron)      │                     │
│  └──────────────┘  └──────────────┘                     │
│                                                         │
│  Volumes:                                               │
│  - postgres_data  (Persistent DB)                       │
│  - airflow_logs   (Execution logs)                      │
│  - app/data       (Raw/processed CSV)                   │
│  - app/logs       (Application logs)                    │
└─────────────────────────────────────────────────────────┘
```

### 1.6 Restart Policies

All containers configured with `restart: unless-stopped`:
- Auto-restart on failure
- Auto-restart on system reboot
- Only stop when manually stopped

```yaml
# docker-compose.yml
services:
  db:
    restart: unless-stopped  # Survives reboots
  app:
    restart: unless-stopped
  airflow-webserver:
    restart: unless-stopped
  airflow-scheduler:
    restart: unless-stopped
  pgadmin:
    restart: unless-stopped
```

### 1.7 Common Docker Commands

```bash
# View all containers
docker compose ps

# View logs (all services)
docker compose logs -f

# View logs (specific service)
docker compose logs -f app
docker compose logs -f airflow-scheduler

# Restart specific service
docker compose restart app
docker compose restart airflow-scheduler

# Stop all services
docker compose down

# Stop and remove volumes (CAUTION: deletes all data)
docker compose down -v

# Rebuild after code changes
docker compose up -d --build app

# Execute command in container
docker compose exec app python -m app.cli list-stocks
docker compose exec db psql -U stockuser -d stock_pipeline

# Clean up unused images
docker image prune -f
```

---

## 2. Cloud Deployment

### 2.1 Why Deploy to Cloud?

**Problem**: Local laptop can't run 24/7  
**Solution**: Deploy to always-on cloud server

**Benefits:**
- ✅ 24/7 operation without laptop dependency
- ✅ Better uptime (99.9% vs laptop availability)
- ✅ Remote access from anywhere
- ✅ Automatic daily execution
- ✅ Professional infrastructure

### 2.2 Option 1: DigitalOcean Droplet (Recommended)

**Cost**: $12/month (Basic Droplet)  
**Setup Time**: ~30 minutes  
**Difficulty**: ⭐⭐ (Easiest)

#### Step 1: Create Droplet
```bash
# 1. Sign up at https://www.digitalocean.com
# 2. Create Droplet:
#    - Image: Docker on Ubuntu 22.04 (Marketplace)
#    - Plan: Basic - $12/mo (2GB RAM, 1 vCPU, 50GB SSD)
#    - Region: Closest to you (e.g., London for Nigeria)
#    - Authentication: SSH keys (generate if needed)
#    - Hostname: nigerian-stock-pipeline
t
# 3. Note the droplet IP address (e.g., 134.209.xxx.xxx)
```

#### Step 2: Transfer Files
```bash
# On your local machine
cd /home/Stock_pipeline

# Create archive
tar -czf stock_pipeline.tar.gz \
  docker-compose.yml \
  .env \
  app/ \
  airflow/ \
  tests/ \
  scripts/

# Transfer to droplet
scp stock_pipeline.tar.gz root@<droplet-ip>:/root/

# SSH into droplet
ssh root@<droplet-ip>

# Extract files
cd /root
tar -xzf stock_pipeline.tar.gz
cd Stock_pipeline
```

#### Step 3: Start Services
```bash
# On the droplet
docker compose up -d --build

# Verify
docker compose ps

# Check logs
docker compose logs -f
```

#### Step 4: Access Services
```bash
# Airflow UI: http://<droplet-ip>:8080
# PgAdmin: http://<droplet-ip>:5050
# SSH for CLI: ssh root@<droplet-ip>
```

#### Step 5: Configure Firewall (Optional)
```bash
# Allow only necessary ports
ufw allow 22    # SSH
ufw allow 8080  # Airflow
ufw allow 5050  # PgAdmin (optional)
ufw enable
```

### 2.3 Option 2: AWS EC2

**Cost**: $10-20/month (t3.small)  
**Setup Time**: ~1 hour  
**Difficulty**: ⭐⭐⭐ (More complex)

```bash
# 1. Launch EC2 instance (t3.small, Ubuntu 22.04)
# 2. Install Docker manually
sudo apt update
sudo apt install -y docker.io docker-compose
sudo systemctl enable docker

# 3. Transfer files (same as DigitalOcean)
# 4. Start services
cd Stock_pipeline
docker compose up -d --build

# 5. Configure Security Group (allow ports 22, 8080, 5050)
```

### 2.4 Option 3: AWS Lightsail

**Cost**: $5-10/month  
**Setup Time**: ~45 minutes  
**Difficulty**: ⭐⭐ (Easier than EC2)

```bash
# 1. Create Lightsail instance (Ubuntu 22.04)
# 2. Select instance plan ($10/month = 2GB RAM)
# 3. Download SSH key
# 4. Follow same transfer/setup steps as DigitalOcean
```

### 2.5 Option 4: Render / Railway (Managed)

**Cost**: $15-25/month  
**Setup Time**: ~20 minutes (automated)  
**Difficulty**: ⭐ (Easiest, fully managed)

**Note**: Requires converting to single-service deployment (no docker-compose)

### 2.6 Option 5: Home Raspberry Pi

**Cost**: $0/month (after hardware)  
**Setup Time**: ~2 hours  
**Difficulty**: ⭐⭐⭐⭐ (Requires hardware)

```bash
# Hardware: Raspberry Pi 4 (4GB RAM) + SD card
# Install Raspberry Pi OS
# Install Docker
# Configure static IP
# Port forwarding on router
# Same deployment steps as cloud
```

### 2.7 Cloud Deployment Checklist

```bash
✅ Server created with 2GB+ RAM
✅ Docker and Docker Compose installed
✅ Files transferred to server
✅ .env file configured with production values
✅ Services started: docker compose up -d
✅ DAG unpaused in Airflow UI
✅ Firewall configured (ports 22, 8080)
✅ SSH access tested
✅ First pipeline run successful
✅ Logs verified (no errors)
✅ Email/Slack notifications configured
✅ Bookmark Airflow URL: http://<server-ip>:8080
```

---

## 3. CLI Reference

### 3.1 CLI Overview

The CLI provides **30+ commands** for manual operations:
- Database inspection
- Pipeline execution
- Report generation
- Notification testing
- Stock management

### 3.2 Access CLI

**Local Docker:**
```bash
docker compose exec app python -m app.cli <command>
```

**Cloud Server:**
```bash
ssh root@<server-ip>
cd Stock_pipeline
docker compose exec app python -m app.cli <command>
```

### 3.3 Common Commands

#### Database Commands
```bash
# Initialize database (first time only)
python -m app.cli init-db

# Check database status
python -m app.cli check-db

# View statistics
python -m app.cli db-stats
```

#### Stock Commands
```bash
# List all stocks
python -m app.cli list-stocks

# Search for stock
python -m app.cli search-stock DANGCEM

# Sync stocks from NGX (update master list)
python -m app.cli sync-stocks

# View stock details
python -m app.cli stock-info MTNN
```

#### Price Commands
```bash
# View latest prices (all stocks)
python -m app.cli latest-prices

# View price history (specific stock)
python -m app.cli price-history AIRTELAFRI --days 30

# Check data quality
python -m app.cli check-data-quality

# View quality distribution
python -m app.cli quality-report
```

#### Pipeline Commands
```bash
# Run full pipeline manually
python -m app.cli run-pipeline

# Run specific date
python -m app.cli run-pipeline --date 2025-12-31

# View pipeline status
python -m app.cli pipeline-status

# View execution history
python -m app.cli pipeline-history --days 7
```

#### Indicator Commands *(Available day 21+)*
```bash
# Calculate indicators for specific stock
python -m app.cli calculate-indicators DANGCEM

# View latest indicators (all stocks)
python -m app.cli latest-indicators

# View indicator history
python -m app.cli indicator-history MTNN --days 30

# Find MA crossovers
python -m app.cli ma-crossovers --signal golden
```

#### Alert Commands *(Available day 21+)*
```bash
# Evaluate alerts (manual trigger)
python -m app.cli evaluate-alerts

# View recent alerts
python -m app.cli recent-alerts --days 7

# View alerts by severity
python -m app.cli alerts-by-severity CRITICAL

# View alerts by stock
python -m app.cli stock-alerts ZENITHBANK
```

#### Advisory Commands *(Available day 21+)*
```bash
# Generate recommendations
python -m app.cli generate-recommendations

# View top buy picks
python -m app.cli top-picks --signal BUY --count 10

# View top sell signals
python -m app.cli top-picks --signal SELL --count 10

# Stock recommendation details
python -m app.cli stock-recommendation DANGCEM
```

#### Notification Commands
```bash
# Test email configuration
python -m app.cli test-email

# Test Slack webhook
python -m app.cli test-slack

# Send test alert
python -m app.cli send-test-alert
```

#### Report Commands
```bash
# Generate daily report
python -m app.cli generate-report --type daily

# Generate weekly report
python -m app.cli generate-report --type weekly

# Generate monthly report
python -m app.cli generate-report --type monthly

# Export data to CSV
python -m app.cli export-data --output /app/reports/export.csv
```

### 3.4 CLI Examples

```bash
# Morning routine: Check overnight alerts
docker compose exec app python -m app.cli recent-alerts --days 1
docker compose exec app python -m app.cli top-picks --signal BUY --count 5

# Weekly review
docker compose exec app python -m app.cli pipeline-history --days 7
docker compose exec app python -m app.cli generate-report --type weekly

# Troubleshooting
docker compose exec app python -m app.cli check-db
docker compose exec app python -m app.cli check-data-quality
docker compose exec app python -m app.cli pipeline-status

# Manual run (if scheduled run failed)
docker compose exec app python -m app.cli run-pipeline
```

---

## 4. Production Operations

### 4.1 Daily Automated Execution

**Schedule**: Every day at 3:00 PM WAT (2:00 PM UTC)  
**Trigger**: Airflow scheduler  
**No manual intervention required**

**Airflow DAG Tasks:**
1. `run_etl_pipeline` - Main 8-stage pipeline (~37s)
2. `generate_daily_summary` - Stats and logging (~1s)
3. `check_sla` - Verify execution completed (~1s)

### 4.2 Monitoring

#### Check Pipeline Status
```bash
# Via Airflow UI
http://<server-ip>:8080
# Navigate to: DAGs → nigerian_stock_pipeline
# View: Recent runs, task status, logs

# Via CLI
docker compose exec app python -m app.cli pipeline-status
docker compose exec app python -m app.cli pipeline-history --days 7
```

#### Check Data Quality
```bash
# Quality distribution
docker compose exec app python -m app.cli quality-report

# Example output:
# GOOD: 77 records (50%)
# INCOMPLETE: 77 records (50%)
# POOR: 0 records (0%)
```

#### View Logs
```bash
# Application logs
docker compose logs -f app

# Airflow scheduler logs
docker compose logs -f airflow-scheduler

# Database logs
docker compose logs -f db
```

### 4.3 Troubleshooting

#### Pipeline Failed
```bash
# 1. Check Airflow UI for error message
# 2. View logs
docker compose logs app | tail -100

# 3. Check database connectivity
docker compose exec app python -m app.cli check-db

# 4. Manual retry
docker compose exec app python -m app.cli run-pipeline

# 5. If persistent, restart services
docker compose restart app airflow-scheduler
```

#### No Data Loaded
```bash
# 1. Verify NGX website is accessible
curl https://www.african-markets.com/en/stock-markets/ngx/listed-companies

# 2. Check scraper logs
docker compose logs app | grep "Fetching NGX"

# 3. Manual test
docker compose exec app python -m app.cli sync-stocks
docker compose exec app python -m app.cli run-pipeline
```

#### Notifications Not Sending
```bash
# 1. Verify .env configuration
docker compose exec app cat /app/.env | grep NOTIFICATION

# 2. Test email
docker compose exec app python -m app.cli test-email

# 3. Test Slack
docker compose exec app python -m app.cli test-slack

# 4. Check notification service logs
docker compose logs app | grep "notification"
```

#### Database Full
```bash
# Check disk usage
docker compose exec db df -h

# Check database size
docker compose exec db psql -U stockuser -d stock_pipeline -c \
  "SELECT pg_size_pretty(pg_database_size('stock_pipeline'));"

# Archive old data (optional, after 2+ years)
docker compose exec db psql -U stockuser -d stock_pipeline -c \
  "DELETE FROM fact_daily_prices WHERE price_date < '2023-01-01';"
```

### 4.4 Backup & Recovery

#### Manual Backup
```bash
# Backup database
docker compose exec db pg_dump -U stockuser stock_pipeline > backup_$(date +%Y%m%d).sql

# Backup configuration
tar -czf config_backup.tar.gz .env docker-compose.yml

# Transfer to safe location
scp backup_*.sql user@backup-server:/backups/
```

#### Restore from Backup
```bash
# Stop services
docker compose down

# Restore database
docker compose up -d db
cat backup_20251231.sql | docker compose exec -T db psql -U stockuser stock_pipeline

# Restart all services
docker compose up -d
```

#### Automated Backups (Recommended)
```bash
# Add cron job on cloud server
crontab -e

# Daily backup at 4AM (after pipeline runs)
0 4 * * * cd /root/Stock_pipeline && docker compose exec -T db pg_dump -U stockuser stock_pipeline | gzip > /backups/stock_pipeline_$(date +\%Y\%m\%d).sql.gz
```

### 4.5 Updating the System

#### Code Updates
```bash
# On local machine (after changes)
git push origin main

# On cloud server
ssh root@<server-ip>
cd Stock_pipeline
git pull origin main

# Rebuild and restart
docker compose down
docker compose up -d --build
```

#### Database Schema Changes
```bash
# 1. Create migration script
# 2. Test locally first (ALWAYS)
# 3. Backup production database
docker compose exec db pg_dump -U stockuser stock_pipeline > pre_migration_backup.sql

# 4. Apply migration
docker compose exec db psql -U stockuser stock_pipeline < migration.sql

# 5. Verify
docker compose exec app python -m app.cli check-db
```

### 4.6 Scaling Considerations

**Current Capacity:**
- 154 stocks
- 154 prices/day
- ~37 seconds execution time
- ~180MB/year storage

**If scaling to 500+ stocks:**
```yaml
# docker-compose.yml adjustments
db:
  environment:
    - POSTGRES_MAX_CONNECTIONS=100  # Increase from 20
    
app:
  deploy:
    resources:
      limits:
        memory: 2G  # Increase from 1G
```

### 4.7 Production .env Template

```bash
# Database
DATABASE_HOST=db
DATABASE_PORT=5432
DATABASE_NAME=stock_pipeline
DATABASE_USER=stockuser
DATABASE_PASSWORD=CHANGE_ME_STRONG_PASSWORD  # Change in production!

# Email Notifications
NOTIFICATION_EMAIL_ENABLED=true
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_16_char_app_password  # Gmail app password
NOTIFICATION_FROM_EMAIL=alerts@yourdomain.com
NOTIFICATION_EMAILS=investor@yourdomain.com,analyst@yourdomain.com

# Slack Notifications (optional)
NOTIFICATION_SLACK_ENABLED=false
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# Pipeline Settings
PIPELINE_BATCH_SIZE=50
PIPELINE_MAX_ERRORS=10
PIPELINE_LOOKBACK_DAYS=30

# Logging
LOG_LEVEL=INFO
```

---

## 📞 Support

### Common Issues
- **DAG not appearing**: Wait 2-3 minutes after startup, check `docker compose logs airflow-scheduler`
- **Connection refused**: Check service status with `docker compose ps`
- **Out of memory**: Increase Docker resource limits
- **Slow performance**: Check disk space, database connections

### Getting Help
1. Check logs: `docker compose logs -f <service>`
2. Review Airflow UI: http://localhost:8080
3. Test CLI commands for diagnostics
4. Review [GitHub Issues](https://github.com/Akin-ctrl/Stock_pipeline/issues)

---

**Last Updated**: December 31, 2025  
**Version**: 1.0.0
