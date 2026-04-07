import express, { Request, Response } from "express";
import postgres from "postgres";
import http from "http";

import {
  BronzeAppfolioReport,
  SilverAppfolioReport,
  PipelineMetadata,
  TransformContext,
} from "./types";
import { getStrategy, isSupported, getSupportedTypes } from "./strategies/registry";

const app = express();
const PORT = parseInt(process.env.PORT ?? "3002", 10);
const SERVICE_NAME = "cynthiaos-transform-worker";

// ── Internal self-call helper ─────────────────────────────────────────────────
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

// ── Derive report date string from a BronzeAppfolioReport ────────────────────

function deriveReportDate(bronze: BronzeAppfolioReport): string | null {
  const rdRaw: unknown = bronze.report_date;
  if (!rdRaw) return null;
  if (rdRaw instanceof Date) return rdRaw.toISOString().slice(0, 10);
  return String(rdRaw).slice(0, 10);
}

// ── Silver transform (strategy-driven) ───────────────────────────────────────
//
// Selects the appropriate TransformStrategy for the Bronze record's report_type,
// delegates normalisation, and writes the Silver record + pipeline_metadata.

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
  const reportDate = deriveReportDate(bronze);
  const strategy = getStrategy(bronze.report_type);
  const supported = isSupported(bronze.report_type);

  console.log(
    `[${SERVICE_NAME}] transformBronzeReport — bronze id=${bronze.id} type=${bronze.report_type} supported=${supported}`
  );

  // Delegate Silver normalisation to the strategy
  const ctx: TransformContext = { sql, bronze, reportDate };
  const { normalized_data } = strategy.normalizeSilver(ctx);

  const reportDateStr = reportDate ?? new Date().toISOString().slice(0, 10);

  const silverRows = await sql<SilverAppfolioReport[]>`
    INSERT INTO silver_appfolio_reports
      (bronze_report_id, report_type, report_date, normalized_data, transformed_at)
    VALUES (
      ${bronze.id},
      ${bronze.report_type},
      ${reportDateStr}::date,
      ${sql.json(normalized_data as any)},
      NOW()
    )
    RETURNING *
  `;

  const silver = silverRows[0];
  console.log(`[${SERVICE_NAME}] transformBronzeReport — inserted silver id=${silver.id} type=${silver.report_type}`);

  const meta = await insertPipelineMetadata(sql, bronze.id, "silver", "processed");

  return { bronze, silver, meta };
}

// ── Gold promotion (strategy-driven) ─────────────────────────────────────────
//
// Finds the oldest Silver record that has NOT yet been promoted to Gold,
// resolves the strategy for its report_type, and delegates Gold promotion.
// Unlike the old implementation, there is NO hardcoded report_type filter —
// every report type is eligible; the strategy decides what to do.

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
app.post("/transform/run", async (_req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();

    // Find oldest unprocessed bronze record
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
    console.log(`[${SERVICE_NAME}] POST /transform/run — processing bronze_report_id=${bronze_report_id}`);

    const { bronze, silver, meta } = await transformBronzeReport(sql, bronze_report_id);

    // Mark the bronze pipeline_metadata record as 'processed'
    await sql`
      UPDATE pipeline_metadata
      SET status = 'processed', updated_at = NOW()
      WHERE id = ${meta_id}
    `;
    console.log(`[${SERVICE_NAME}] POST /transform/run — marked bronze meta ${meta_id} as processed`);

    // Auto-trigger Gold promotion (fire-and-forget)
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
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[${SERVICE_NAME}] POST /transform/run error:`, message);
    res.status(500).json({ success: false, error: message });
  } finally {
    if (sql) await sql.end();
  }
});

// ── POST /gold/run ─────────────────────────────────────────────────────────
//
// Finds the next eligible Silver record (any report_type — no hardcoded filter),
// resolves the strategy for its report_type, and delegates Gold promotion.
// Unsupported report types are skipped gracefully (no crash, no data corruption).
app.post("/gold/run", async (_req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();

    // Find the oldest Silver record that has NOT yet been promoted to Gold.
    // No report_type filter — the strategy decides what to do.
    const candidates = await sql<{
      silver_id: string;
      bronze_report_id: string;
      report_type: string;
      report_date: string;
      normalized_data: Record<string, unknown>;
    }[]>`
      SELECT
        s.id            AS silver_id,
        s.bronze_report_id,
        s.report_type,
        s.report_date::text AS report_date,
        s.normalized_data
      FROM silver_appfolio_reports s
      WHERE NOT EXISTS (
        SELECT 1 FROM pipeline_metadata pm
        WHERE pm.bronze_report_id = s.bronze_report_id
          AND pm.stage = 'gold'
      )
      ORDER BY s.transformed_at ASC
      LIMIT 1
    `;

    if (candidates.length === 0) {
      console.log(`[${SERVICE_NAME}] POST /gold/run — no eligible silver records found`);
      res.status(200).json({
        success: true,
        processed: false,
        reason: "No Silver records pending Gold promotion",
      });
      return;
    }

    const raw = candidates[0];
    const { silver_id, bronze_report_id, report_type } = raw;
    const reportDate: string | null = raw.report_date
      ? String(raw.report_date).slice(0, 10)
      : null;

    console.log(`[${SERVICE_NAME}] POST /gold/run — processing silver_id=${silver_id} report_type=${report_type} bronze_report_id=${bronze_report_id}`);

    // Fetch the full Silver record
    const silverRows = await sql<SilverAppfolioReport[]>`
      SELECT * FROM silver_appfolio_reports WHERE id = ${silver_id} LIMIT 1
    `;
    const silver = silverRows[0];

    // Fetch the Bronze record for the strategy context
    const bronzeRows = await sql<BronzeAppfolioReport[]>`
      SELECT * FROM bronze_appfolio_reports WHERE id = ${bronze_report_id} LIMIT 1
    `;
    const bronze = bronzeRows[0];

    // Resolve strategy and delegate Gold promotion
    const strategy = getStrategy(report_type);
    const supported = isSupported(report_type);
    console.log(`[${SERVICE_NAME}] POST /gold/run — strategy resolved report_type=${report_type} supported=${supported}`);

    const ctx = { sql, bronze, silver, reportDate };
    const result = await strategy.promoteGold(ctx);

    // Record pipeline_metadata stage='gold'
    const goldStatus = result.skipped ? "skipped" : "processed";
    const meta = await insertPipelineMetadata(sql, bronze_report_id, "gold", goldStatus);
    console.log(`[${SERVICE_NAME}] POST /gold/run — pipeline_metadata id=${meta.id} stage=gold status=${goldStatus}`);

    res.status(200).json({
      success: true,
      processed: !result.skipped,
      silver_id,
      bronze_report_id,
      report_type,
      supported,
      gold_row_count: result.gold_ids.length,
      gold_ids: result.gold_ids,
      skipped: result.skipped,
      skip_reason: result.skip_reason ?? null,
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

// ── GET /strategies ───────────────────────────────────────────────────────────
// Diagnostic endpoint: returns the list of registered report types.
app.get("/strategies", (_req: Request, res: Response) => {
  res.status(200).json({
    success: true,
    supported_report_types: getSupportedTypes(),
  });
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
    supported_report_types: getSupportedTypes(),
  });
});

// ── Catch-all ─────────────────────────────────────────────────────────────────
app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: "not_found" });
});

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", async () => {
  console.log(`[${SERVICE_NAME}] listening on port ${PORT}`);
  console.log(`[${SERVICE_NAME}] registered strategies: ${getSupportedTypes().join(", ")}`);
  await checkDatabaseConnectivity();
});

export default app;
