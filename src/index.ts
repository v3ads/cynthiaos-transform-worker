import express, { Request, Response } from "express";
import postgres from "postgres";
import http from "http";

const app = express();
const PORT = parseInt(process.env.PORT ?? "3002", 10);
const SERVICE_NAME = "cynthiaos-transform-worker";

// ── Internal self-call helper ─────────────────────────────────────────────────
// Fires an HTTP POST to a path on this same service (localhost:PORT).
// Used to chain /gold/run after /transform/run without an external round-trip.
function selfPost(path: string): void {
  const options = {
    hostname: "127.0.0.1",
    port: PORT,
    path,
    method: "POST",
    headers: { "Content-Type": "application/json", "Content-Length": 0 },
  };
  const req = http.request(options, (res) => {
    let body = "";
    res.on("data", (chunk) => { body += chunk; });
    res.on("end", () => {
      console.log(`[${SERVICE_NAME}] selfPost ${path} → HTTP ${res.statusCode} — ${body.slice(0, 200)}`);
    });
  });
  req.on("error", (err) => {
    console.error(`[${SERVICE_NAME}] selfPost ${path} error:`, err.message);
  });
  req.end();
}

app.use(express.json());

// ── Database client ───────────────────────────────────────────────────────────
function getDb(): postgres.Sql {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL environment variable is not set");
  }
  return postgres(databaseUrl, { ssl: "require", max: 5, idle_timeout: 30 });
}

// ── Database connectivity state ───────────────────────────────────────────────
let dbConnected = false;
let dbTimestamp: string | null = null;

async function checkDatabaseConnectivity(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    console.log(`[${SERVICE_NAME}] DATABASE_URL not set — skipping DB check`);
    return;
  }
  try {
    const sql = getDb();
    const result = await sql`SELECT NOW() AS now`;
    dbTimestamp = result[0].now.toISOString();
    dbConnected = true;
    console.log(`[${SERVICE_NAME}] DB connectivity verified — SELECT NOW() = ${dbTimestamp}`);
    await sql.end();
  } catch (err) {
    console.error(`[${SERVICE_NAME}] DB connectivity check FAILED:`, err);
    dbConnected = false;
  }
}

// ── Interfaces ────────────────────────────────────────────────────────────────

interface BronzeAppfolioReport {
  id: string;
  report_type: string;
  report_date: string;
  raw_data: Record<string, unknown>;
  ingested_at: Date;
}

interface SilverAppfolioReport {
  id: string;
  bronze_report_id: string;
  report_type: string;
  report_date: string;
  normalized_data: Record<string, unknown>;
  transformed_at: Date;
}

interface PipelineMetadata {
  id: string;
  bronze_report_id: string | null;
  stage: string;
  status: string;
  created_at: Date;
  updated_at: Date;
}

// ── Pipeline metadata ─────────────────────────────────────────────────────────

async function insertPipelineMetadata(
  sql: postgres.Sql,
  bronzeReportId: string | null,
  stage: string,
  status: string
): Promise<PipelineMetadata> {
  const rows = await sql<PipelineMetadata[]>`
    INSERT INTO pipeline_metadata (bronze_report_id, stage, status, created_at, updated_at)
    VALUES (${bronzeReportId}, ${stage}, ${status}, NOW(), NOW())
    RETURNING *
  `;
  const meta = rows[0];
  console.log(`[${SERVICE_NAME}] insertPipelineMetadata — id=${meta.id} stage=${meta.stage} status=${meta.status} bronze_report_id=${meta.bronze_report_id}`);
  return meta;
}

// ── Normalize bronze payload into silver structure ────────────────────────────

function normalizeBronzePayload(bronze: BronzeAppfolioReport): Record<string, unknown> {
  const raw = bronze.raw_data as Record<string, unknown>;
  const rows = Array.isArray(raw.rows) ? (raw.rows as Record<string, unknown>[]) : [];
  const summary = (raw.summary ?? {}) as Record<string, unknown>;

  return {
    source: "appfolio",
    report_type: bronze.report_type,
    report_date: bronze.report_date,
    bronze_report_id: bronze.id,
    transformed_at: new Date().toISOString(),
    row_count: rows.length,
    rows: rows.map((r) => ({
      property_id: r.property_id ?? null,
      unit: r.unit ?? null,
      tenant: r.tenant ?? null,
      rent: typeof r.rent === "number" ? r.rent : null,
      status: r.status ?? null,
    })),
    summary: {
      total_units: summary.total_units ?? rows.length,
      total_rent:
        summary.total_rent ??
        rows.reduce(
          (acc, r) => acc + (typeof r.rent === "number" ? r.rent : 0),
          0
        ),
      occupancy_rate: summary.occupancy_rate ?? null,
    },
  };
}

// ── transformBronzeReport ─────────────────────────────────────────────────────

async function transformBronzeReport(
  sql: postgres.Sql,
  bronzeId?: string
): Promise<{ bronze: BronzeAppfolioReport; silver: SilverAppfolioReport; meta: PipelineMetadata }> {
  let bronzeRows: BronzeAppfolioReport[];

  if (bronzeId) {
    bronzeRows = await sql<BronzeAppfolioReport[]>`
      SELECT * FROM bronze_appfolio_reports WHERE id = ${bronzeId} LIMIT 1
    `;
  } else {
    bronzeRows = await sql<BronzeAppfolioReport[]>`
      SELECT * FROM bronze_appfolio_reports ORDER BY ingested_at DESC LIMIT 1
    `;
  }

  if (bronzeRows.length === 0) {
    throw new Error("No bronze report found to transform");
  }

  const bronze = bronzeRows[0];
  console.log(
    `[${SERVICE_NAME}] transformBronzeReport — reading bronze id=${bronze.id} type=${bronze.report_type}`
  );

  const normalizedData = normalizeBronzePayload(bronze);

  const reportDateStr =
    typeof bronze.report_date === "string"
      ? bronze.report_date
      : new Date(bronze.report_date).toISOString().slice(0, 10);

  const silverRows = await sql<SilverAppfolioReport[]>`
    INSERT INTO silver_appfolio_reports
      (bronze_report_id, report_type, report_date, normalized_data, transformed_at)
    VALUES (
      ${bronze.id},
      ${bronze.report_type},
      ${reportDateStr}::date,
      ${sql.json(normalizedData as any)},
      NOW()
    )
    RETURNING *
  `;

  const silver = silverRows[0];
  console.log(`[${SERVICE_NAME}] transformBronzeReport — inserted silver id=${silver.id}`);

  // Insert pipeline_metadata — silver stage processed
  const meta = await insertPipelineMetadata(sql, bronze.id, "silver", "processed");

  return { bronze, silver, meta };
}

// ── triggerGold — fire-and-forget internal call to POST /gold/run ────────────
// Mirrors the triggerTransform pattern in ingestion-worker.
// Called after a Silver insert completes so the full Bronze→Silver→Gold chain
// runs automatically from a single POST /ingest/report call.
async function triggerGold(): Promise<void> {
  const port = process.env.PORT ?? "3002";
  const url = `http://localhost:${port}/gold/run`;
  try {
    const resp = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" } });
    const body = await resp.json() as Record<string, unknown>;
    if (body.processed) {
      console.log(`[${SERVICE_NAME}] triggerGold — gold promotion succeeded gold_ids=${JSON.stringify(body.gold_ids)}`);
    } else {
      console.log(`[${SERVICE_NAME}] triggerGold — gold/run responded: processed=false reason=${body.reason ?? "unknown"}`);
    }
  } catch (err) {
    // Non-fatal: log and continue — the Silver record is already committed
    console.warn(`[${SERVICE_NAME}] triggerGold — fire-and-forget call failed (non-fatal):`, err);
  }
}

// ── POST /transform/test ──────────────────────────────────────────────────────
app.post("/transform/test", async (_req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();
    const { bronze, silver, meta } = await transformBronzeReport(sql);

    res.status(200).json({
      success: true,
      silver_id: silver.id,
      silver: {
        id: silver.id,
        bronze_report_id: silver.bronze_report_id,
        report_type: silver.report_type,
        report_date: silver.report_date,
        normalized_data: silver.normalized_data,
        transformed_at: silver.transformed_at,
      },
      pipeline_metadata: {
        id: meta.id,
        bronze_report_id: meta.bronze_report_id,
        stage: meta.stage,
        status: meta.status,
        created_at: meta.created_at,
      },
      source_bronze: {
        id: bronze.id,
        report_type: bronze.report_type,
        report_date: bronze.report_date,
        ingested_at: bronze.ingested_at,
      },
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[${SERVICE_NAME}] POST /transform/test error:`, message);
    res.status(500).json({ success: false, error: message });
  } finally {
    if (sql) await sql.end();
  }
});

// ── POST /transform/run ─────────────────────────────────────────────────────
// Finds the oldest bronze record with pipeline_metadata stage='bronze',
// status='created' that has NOT yet been transformed (no silver record exists
// for it), transforms it, and writes silver + pipeline_metadata records.
app.post("/transform/run", async (_req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();

    // 1. Find oldest unprocessed bronze record
    // A bronze record is "unprocessed" when pipeline_metadata has
    // stage='bronze', status='created' for it and no silver record exists yet.
    const candidates = await sql<{ bronze_report_id: string; meta_id: string; created_at: Date }[]>`
      SELECT pm.bronze_report_id, pm.id AS meta_id, pm.created_at
      FROM pipeline_metadata pm
      WHERE pm.stage = 'bronze'
        AND pm.status = 'created'
        AND pm.bronze_report_id IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM silver_appfolio_reports s
          WHERE s.bronze_report_id = pm.bronze_report_id
        )
      ORDER BY pm.created_at ASC
      LIMIT 1
    `;

    if (candidates.length === 0) {
      res.status(200).json({
        success: true,
        processed: false,
        message: "No unprocessed bronze records found",
      });
      return;
    }

    const { bronze_report_id, meta_id } = candidates[0];
    console.log(`[${SERVICE_NAME}] POST /transform/run — found bronze_report_id=${bronze_report_id} meta_id=${meta_id}`);

    // 2. Transform the bronze record (inserts silver + pipeline_metadata silver)
    const { bronze, silver, meta } = await transformBronzeReport(sql, bronze_report_id);

    // 3. Mark the bronze pipeline_metadata record as 'processed'
    await sql`
      UPDATE pipeline_metadata
      SET status = 'processed', updated_at = NOW()
      WHERE id = ${meta_id}
    `;
    console.log(`[${SERVICE_NAME}] POST /transform/run — marked bronze meta ${meta_id} as processed`);

    // 4. Auto-trigger Gold promotion (fire-and-forget — Silver is already committed)
    triggerGold().catch(() => { /* already logged inside triggerGold */ });
    console.log(`[${SERVICE_NAME}] POST /transform/run — Gold promotion triggered`);

    res.status(200).json({
      success: true,
      processed: true,
      bronze_report_id: bronze.id,
      silver_id: silver.id,
      source_bronze: {
        id: bronze.id,
        report_type: bronze.report_type,
        report_date: bronze.report_date,
        ingested_at: bronze.ingested_at,
      },
      silver: {
        id: silver.id,
        bronze_report_id: silver.bronze_report_id,
        report_type: silver.report_type,
        report_date: silver.report_date,
        transformed_at: silver.transformed_at,
      },
      pipeline_metadata_silver: {
        id: meta.id,
        bronze_report_id: meta.bronze_report_id,
        stage: meta.stage,
        status: meta.status,
        created_at: meta.created_at,
      },
    });

    // Note: triggerGold() above (fire-and-forget fetch) is the sole Gold trigger.
    // The selfPost("/gold/run") duplicate has been removed to prevent the race
    // condition that caused duplicate gold rows (TASK-029 idempotency fix).
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[${SERVICE_NAME}] POST /transform/run error:`, message);
    res.status(500).json({ success: false, error: message });
  } finally {
    if (sql) await sql.end();
  }
});

// ── Gold layer interfaces ─────────────────────────────────────────────────────

interface GoldLeaseExpiration {
  id: string;
  bronze_report_id: string | null;
  tenant_id: string;
  unit_id: string;
  lease_start_date: string | null;
  lease_end_date: string | null;
  days_until_expiration: number | null;
  created_at: Date;
}

// ── POST /gold/run ─────────────────────────────────────────────────────
//
// Reads the next eligible silver record that has NOT yet been promoted to the
// Gold layer. Eligibility criteria:
//   - silver_appfolio_reports.report_type = 'rent_roll' (lease-related data)
//   - no gold_lease_expirations row already exists for the same bronze_report_id
//     (idempotency guard)
//
// Extracts per-row lease fields from normalized_data.rows, inserts one
// gold_lease_expirations row per tenant row, and records pipeline_metadata
// stage='gold', status='processed'.
//
// Returns:
//   { processed: true,  gold_ids: [...], silver_id, bronze_report_id }
//   { processed: false, reason: "..." }  when nothing eligible is found
app.post("/gold/run", async (_req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();

    // 1. Find the oldest silver record eligible for Gold promotion.
    //    Eligible = report_type is rent_roll AND no gold row exists yet for
    //    that bronze_report_id (prevents duplicate gold rows on re-run).
    const candidates = await sql<{ silver_id: string; bronze_report_id: string; report_date: string; normalized_data: Record<string, unknown> }[]>`
      SELECT
        s.id          AS silver_id,
        s.bronze_report_id,
        s.report_date::text AS report_date,
        s.normalized_data
      FROM silver_appfolio_reports s
      WHERE s.report_type = 'rent_roll'
        AND NOT EXISTS (
          SELECT 1 FROM gold_lease_expirations g
          WHERE g.bronze_report_id = s.bronze_report_id
        )
      ORDER BY s.transformed_at ASC
      LIMIT 1
    `;

    if (candidates.length === 0) {
      console.log(`[${SERVICE_NAME}] POST /gold/run — no eligible silver records found`);
      res.status(200).json({
        success: true,
        processed: false,
        reason: "No eligible silver rent_roll records without existing gold rows",
      });
      return;
    }

    const raw = candidates[0];
    const silver_id = raw.silver_id;
    const bronze_report_id = raw.bronze_report_id;
    const normalized_data = raw.normalized_data;
    // Normalize report_date: postgres may return a Date object despite ::text cast
    const rdRaw: unknown = raw.report_date;
    const report_date: string | null = rdRaw
      ? (rdRaw instanceof Date
          ? rdRaw.toISOString().slice(0, 10)
          : String(rdRaw).slice(0, 10))
      : null;
    console.log(`[${SERVICE_NAME}] POST /gold/run — processing silver_id=${silver_id} bronze_report_id=${bronze_report_id} report_date=${report_date}`);

    // 2. Extract rows from normalized_data
    const rows = Array.isArray((normalized_data as any).rows)
      ? ((normalized_data as any).rows as Record<string, unknown>[])
      : [];

    if (rows.length === 0) {
      console.log(`[${SERVICE_NAME}] POST /gold/run — silver record has no rows, skipping`);
      res.status(200).json({
        success: true,
        processed: false,
        reason: `Silver record ${silver_id} has no rows in normalized_data`,
        silver_id,
        bronze_report_id,
      });
      return;
    }

    // 3. Calculate days_until_expiration for each row and insert gold records.
    //    Lease fields are sourced from the row payload; if not present we use
    //    the report_date as lease_start_date and derive a synthetic end date
    //    (report_date + 12 months) so the Gold table is always populated.
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);

    const goldIds: string[] = [];

    for (const row of rows) {
      const tenantId = String(row.tenant ?? row.tenant_id ?? "unknown");
      const unitId   = String(row.unit   ?? row.unit_id   ?? "unknown");

      // Derive lease dates: prefer explicit fields, fall back to report_date
      const leaseStart: string | null =
        typeof row.lease_start_date === "string" ? row.lease_start_date
        : typeof row.lease_start === "string"    ? row.lease_start
        : report_date ?? null;

      const leaseEnd: string | null =
        typeof row.lease_end_date === "string" ? row.lease_end_date
        : typeof row.lease_end === "string"    ? row.lease_end
        : (() => {
            // Synthetic: report_date + 12 months
            if (!report_date) return null;
            const d = new Date(report_date);
            d.setFullYear(d.getFullYear() + 1);
            return d.toISOString().slice(0, 10);
          })();

      // Calculate days_until_expiration
      let daysUntilExpiration: number | null = null;
      if (leaseEnd) {
        const endDate = new Date(leaseEnd);
        endDate.setUTCHours(0, 0, 0, 0);
        daysUntilExpiration = Math.round(
          (endDate.getTime() - today.getTime()) / (1000 * 60 * 60 * 24)
        );
      }

      // Insert gold record — pass dates as plain YYYY-MM-DD strings; Postgres
      // will coerce them to DATE via the column type. Do NOT concatenate '::date'
      // inside a tagged template literal as the postgres driver treats interpolated
      // values as parameters, not raw SQL fragments.
      const goldRows = await sql<GoldLeaseExpiration[]>`
        INSERT INTO gold_lease_expirations
          (bronze_report_id, tenant_id, unit_id, lease_start_date, lease_end_date, days_until_expiration, created_at)
        VALUES (
          ${bronze_report_id},
          ${tenantId},
          ${unitId},
          ${leaseStart},
          ${leaseEnd},
          ${daysUntilExpiration},
          NOW()
        )
        ON CONFLICT (bronze_report_id, tenant_id, unit_id) DO NOTHING
        RETURNING *
      `;

      const gold = goldRows[0];
      goldIds.push(gold.id);
      console.log(
        `[${SERVICE_NAME}] POST /gold/run — inserted gold id=${gold.id} tenant=${tenantId} unit=${unitId} days_until_expiration=${daysUntilExpiration}`
      );
    }

    // 4. Record pipeline_metadata stage='gold', status='processed'
    const meta = await insertPipelineMetadata(sql, bronze_report_id, "gold", "processed");
    console.log(`[${SERVICE_NAME}] POST /gold/run — pipeline_metadata id=${meta.id} stage=gold status=processed`);

    res.status(200).json({
      success: true,
      processed: true,
      silver_id,
      bronze_report_id,
      gold_row_count: goldIds.length,
      gold_ids: goldIds,
      pipeline_metadata: {
        id: meta.id,
        bronze_report_id: meta.bronze_report_id,
        stage: meta.stage,
        status: meta.status,
        created_at: meta.created_at,
      },
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[${SERVICE_NAME}] POST /gold/run error:`, message);
    res.status(500).json({ success: false, error: message });
  } finally {
    if (sql) await sql.end();
  }
});

// ── Health check ──────────────────────────────────────────────────────────────
app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({
    service: SERVICE_NAME,
    status: "ok",
    timestamp: new Date().toISOString(),
    db: {
      connected: dbConnected,
      verified_at: dbTimestamp,
    },
  });
});

// ── Catch-all ─────────────────────────────────────────────────────────────────
app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: "not_found" });
});

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", async () => {
  console.log(`[${SERVICE_NAME}] listening on port ${PORT}`);
  await checkDatabaseConnectivity();
});

export default app;
