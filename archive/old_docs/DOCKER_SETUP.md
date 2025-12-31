# 🐳 Docker Setup Guide

## Quick Start

### 1. Build and Start All Services
```bash
docker-compose up -d --build
```

### 2. Access Services
- **PostgreSQL**: `localhost:5432`
  - Username: `stockuser`
  - Password: `changeme`
  - Database: `stock_pipeline`

- **pgAdmin**: http://localhost:5050
  - Email: `admin@stockpipeline.com`
  - Password: `admin`

- **Airflow**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`

- **Tableau Desktop Connection**:
  - Server: `localhost`
  - Port: `5432`
  - Database: `stock_pipeline`
  - Use views: `vw_investment_dashboard`, `vw_latest_stock_prices`

### 3. Initialize Database
```bash
# The database is automatically initialized from schema.sql
# To verify:
docker exec -it stock_pipeline_db psql -U stockuser -d stock_pipeline -c "\dt"
```

### 4. Run Pipeline Manually
```bash
docker exec -it stock_pipeline_app python scripts/init_db.py
```

## Services Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Docker Network                     │
│                  (stock_network)                    │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │
│  │  PostgreSQL  │  │   pgAdmin    │  │   App    │ │
│  │    :5432     │◄─│    :5050     │  │Container │ │
│  └──────────────┘  └──────────────┘  └──────────┘ │
│         ▲                                    │      │
│         │                                    ▼      │
│  ┌──────────────┐                    ┌──────────┐ │
│  │   Airflow    │                    │  Data    │ │
│  │    :8080     │────────────────────│ Volumes  │ │
│  └──────────────┘                    └──────────┘ │
│                                                     │
│  External: Tableau Desktop ────► PostgreSQL :5432  │
└─────────────────────────────────────────────────────┘
```

## Volume Mounts

### Persistent Data
- `postgres_data`: Database files
- `pgadmin_data`: pgAdmin configuration
- `airflow_data`: Airflow metadata

### Shared Directories
- `./data` → `/app/data`: Stock data (raw/processed)
- `./logs` → `/app/logs`: Application logs
- `./reports` → `/app/reports`: Generated reports
- `./airflow/dags` → `/opt/airflow/dags`: Airflow DAGs

## Common Commands

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f app
docker-compose logs -f postgres
docker-compose logs -f airflow
```

### Restart Services
```bash
docker-compose restart app
docker-compose restart airflow
```

### Stop All Services
```bash
docker-compose down
```

### Stop and Remove Volumes (Clean Slate)
```bash
docker-compose down -v
```

### Execute Commands Inside Containers
```bash
# App container
docker exec -it stock_pipeline_app bash

# PostgreSQL
docker exec -it stock_pipeline_db psql -U stockuser -d stock_pipeline

# Airflow
docker exec -it stock_pipeline_airflow bash
```

## Development Workflow

### 1. Code Changes
Code is automatically available in containers via volume mounts.
No rebuild needed for Python changes.

### 2. Dependency Changes
```bash
# Update requirements.txt, then rebuild
docker-compose up -d --build app
```

### 3. Database Schema Changes
```bash
# Apply schema manually
docker exec -i stock_pipeline_db psql -U stockuser -d stock_pipeline < schema.sql
```

## Tableau Integration

### Desktop Connection
1. Open Tableau Desktop
2. Connect to PostgreSQL:
   - Server: `localhost`
   - Port: `5432`
   - Database: `stock_pipeline`
   - Authentication: Username & Password
   - Username: `stockuser`
   - Password: `changeme`

### Recommended Tables/Views
- `vw_investment_dashboard` - Complete investment view
- `vw_latest_stock_prices` - Current market snapshot
- `fact_daily_prices` - Historical prices
- `fact_technical_indicators` - Technical analysis
- `alert_history` - Investment alerts

### Example Tableau Queries
```sql
-- Top Performers Today
SELECT stock_code, company_name, close_price, change_1d_pct
FROM vw_latest_stock_prices
WHERE change_1d_pct > 0
ORDER BY change_1d_pct DESC
LIMIT 10;

-- Stocks with Active Alerts
SELECT *
FROM vw_investment_dashboard
WHERE active_alerts_count > 0
ORDER BY active_alerts_count DESC;
```

## Troubleshooting

### Port Already in Use
```bash
# Find process using port
sudo lsof -i :5432
sudo lsof -i :5050
sudo lsof -i :8080

# Kill process or change port in docker-compose.yml
```

### Database Connection Failed
```bash
# Check if PostgreSQL is ready
docker exec stock_pipeline_db pg_isready -U stockuser

# Restart PostgreSQL
docker-compose restart postgres
```

### App Health Check Failing
```bash
# Check logs
docker-compose logs app

# Manually test connection
docker exec -it stock_pipeline_app python -c "from app.config import get_db; print(get_db().health_check())"
```

### Airflow Not Starting
```bash
# Check logs
docker-compose logs airflow

# Common issue: Permissions
chmod -R 777 ./airflow/logs
```

## Environment Variables

Edit `.env.docker` to configure:
- Database credentials
- Alert thresholds
- Data source URLs
- Notification settings

## Production Checklist

Before deploying to production:
- [ ] Change default passwords in `.env.docker`
- [ ] Use secrets management (Docker secrets or vault)
- [ ] Enable SSL/TLS for PostgreSQL
- [ ] Configure backup strategy for postgres_data volume
- [ ] Set up monitoring (Prometheus/Grafana)
- [ ] Configure email/Slack notifications
- [ ] Review and adjust resource limits
- [ ] Enable log aggregation
- [ ] Set up CI/CD pipeline

## Backup & Restore

### Backup Database
```bash
docker exec stock_pipeline_db pg_dump -U stockuser stock_pipeline > backup_$(date +%Y%m%d).sql
```

### Restore Database
```bash
docker exec -i stock_pipeline_db psql -U stockuser -d stock_pipeline < backup_20251206.sql
```

## Support

For issues or questions:
1. Check logs: `docker-compose logs`
2. Verify health: `docker-compose ps`
3. Review ARCHITECTURE.md
4. Check IMPLEMENTATION_STATUS.md
