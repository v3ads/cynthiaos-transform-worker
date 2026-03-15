# CynthiaOS System Documentation

**Last Updated:** March 2026
**Version:** 1.0.0

CynthiaOS is a modern, modular property management operating system. It implements a robust Medallion data architecture (Bronze, Silver, Gold) to process property reports, transform them into normalized entities, and serve actionable analytics via a Next.js frontend dashboard.

---

## 1. System Architecture

The system is built on a microservices architecture deployed on Railway, utilizing Node.js, TypeScript, Next.js, and PostgreSQL.

### 1.1 Core Technologies
- **Runtime:** Node.js 20+ (Dockerized)
- **Language:** TypeScript
- **Web Frameworks:** Express.js (Workers/API), Next.js 14 App Router (Frontend)
- **Database:** PostgreSQL (hosted on Neon)
- **Hosting / Deployment:** Railway
- **Source Control:** GitHub

### 1.2 Component Overview
The platform consists of four primary services:

1. **cynthiaos-ingestion-worker**
   - **Role:** Entry point for raw data.
   - **Responsibility:** Receives raw JSON payloads (e.g., AppFolio reports), stores the exact raw event, and promotes it to the Bronze layer. It then orchestrates the pipeline by triggering the transform worker.
2. **cynthiaos-transform-worker**
   - **Role:** Data transformation and business logic engine.
   - **Responsibility:** Normalizes Bronze data into structured Silver entities (e.g., parsing rent rolls into discrete leases) and computes high-level analytics for the Gold layer (e.g., lease expiration tracking).
3. **cynthiaos-api**
   - **Role:** Backend-for-Frontend (BFF).
   - **Responsibility:** Connects directly to the Gold layer in the database to serve paginated, read-only JSON data to the application layer.
4. **cynthiaos-app**
   - **Role:** Staff-facing dashboard.
   - **Responsibility:** A Next.js frontend providing UI views for lease expirations, upcoming renewals, and portfolio-wide metrics.

---

## 2. Medallion Data Pipeline

CynthiaOS utilizes a Medallion architecture to ensure data lineage, auditability, and clean separation of concerns.

### 2.1 The Pipeline Flow

1. **Raw Ingestion:** A payload is POSTed to the Ingestion Worker. It is saved exactly as received in `raw_ingestion_events`.
2. **Bronze Layer:** The raw data is lightly structured into `bronze_appfolio_reports` and a `pipeline_metadata` record is created to track its state.
3. **Silver Layer:** The Transform Worker processes the Bronze record, parsing nested JSON into normalized relational data stored in `silver_appfolio_reports`. The metadata state is updated to `silver`.
4. **Gold Layer:** The Transform Worker extracts specific business entities (e.g., individual leases) from the Silver layer, calculates metrics (e.g., `days_until_expiration`), and stores them in `gold_lease_expirations`.

### 2.2 Pipeline Orchestration
The pipeline is fully automated via HTTP triggers:
- `POST /ingest/report` (Ingestion Worker) writes Bronze data, then immediately makes an async HTTP call to `POST /transform/run` (Transform Worker).
- `POST /transform/run` writes Silver data, then immediately makes an async HTTP call to `POST /gold/run` (Transform Worker).

### 2.3 Database Schema (Neon Postgres)

- **`ingestion_jobs`**: Tracks pipeline job lifecycle (`started`, `completed`).
- **`raw_ingestion_events`**: Immutable ledger of incoming payloads.
- **`bronze_appfolio_reports`**: Raw reports with basic metadata (source, date).
- **`silver_appfolio_reports`**: Normalized report data mapped back to Bronze IDs.
- **`gold_lease_expirations`**: Pre-computed analytics (one row per tenant lease) optimized for fast querying.
- **`pipeline_metadata`**: State machine tracking for each record (`stage`: bronze/silver/gold, `status`: created/processed/failed).

---

## 3. API Reference

### 3.1 Ingestion Worker (`cynthiaos-ingestion-worker`)
- **`GET /health`**: Returns service and DB connectivity status.
- **`POST /ingest/report`**: Primary ingestion endpoint.
  - **Payload:** `{ "source": string, "report_type": string, "report_date": string, "payload": object }`
  - **Action:** Writes to Bronze and triggers Transform.

### 3.2 Transform Worker (`cynthiaos-transform-worker`)
- **`GET /health`**: Returns service and DB connectivity status.
- **`POST /transform/run`**: Promotes oldest unprocessed Bronze record to Silver. Auto-triggers Gold run.
- **`POST /gold/run`**: Promotes oldest unprocessed Silver rent_roll record to Gold.

### 3.3 API Service (`cynthiaos-api`)
All endpoints support pagination via `?page=1&limit=20`.
- **`GET /health`**: Returns service and DB connectivity status.
- **`GET /api/v1/leases/expirations`**: Returns all leases ordered by days until expiration.
- **`GET /api/v1/leases/upcoming-renewals`**: Returns leases expiring within the next 90 days.
- **`GET /api/v1/leases/expiring`**: Returns leases expiring within the next 30 days.

---

## 4. Frontend App (`cynthiaos-app`)

The App layer is a Next.js 14 application providing the visual interface for the system.

- **`/dashboard`**: High-level overview with summary stat cards (Total Leases, 30d Expirations, 90d Renewals) and a preview table.
- **`/leases/expiring`**: Dedicated view for leases expiring within 30 days.
- **`/leases/upcoming-renewals`**: Dedicated view for leases requiring renewal action within 90 days.
- **`/leases/expirations`**: Complete, paginated list of all portfolio leases.

The app uses server-side rendering/fetching with robust error boundaries and loading states, communicating exclusively with `cynthiaos-api`.

---

## 5. Deployment Guide

All services are containerized and deployed to Railway.

### 5.1 Environment Variables
- **Workers & API:** Require `DATABASE_URL` (Neon Postgres connection string).
- **App:** Requires `NEXT_PUBLIC_API_URL` (URL of the deployed `cynthiaos-api`).

### 5.2 Deployment Process
Deployments are triggered via GitHub pushes to the `main` branch or manually via the Railway CLI/API.

**Standard Railway Deployment Script:**
```python
import requests
RAILWAY_TOKEN = "<your_token>"
SERVICE_ID = "<service_id>"
ENVIRONMENT_ID = "<env_id>"

requests.post(
    "https://backboard.railway.app/graphql/v2",
    headers={"Authorization": f"Bearer {RAILWAY_TOKEN}"},
    json={
        "query": "mutation { serviceInstanceDeploy(serviceId: \"...\", environmentId: \"...\") }"
    }
)
```

### 5.3 Live URLs (Production)
- **Ingestion:** `https://cynthiaos-ingestion-worker-production-8068.up.railway.app`
- **Transform:** `https://cynthiaos-transform-worker-production.up.railway.app`
- **API:** `https://cynthiaos-api-production.up.railway.app`
- **App:** `https://cynthiaos-app-production.up.railway.app`
