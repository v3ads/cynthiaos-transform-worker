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
import {
  validateSilver,
  runIntegrityChecks,
  ensurePipelineLogsTable,
  logSilverValidation,
  logGoldPromotion,
  logIntegrityReport,
} from "./validation";
import { generateSystemActions } from "./validation/actionGenerator";
import { repairLease120UnitId } from "./repairs/repairLease120UnitId";

const app: express.Express = express();
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

    // Ensure pipeline_logs table exists on every startup
    await ensurePipelineLogsTable(sql);
    console.log(`[${SERVICE_NAME}] pipeline_logs table verified`);

    // One-time, idempotent repair for the known doubled-prefix lease unit ID.
    // The repair validates the exact tenant and lease end date before touching data.
    const lease120Repair = await repairLease120UnitId(sql);
    console.log(
      `[${SERVICE_NAME}] lease 120-a repair — reason=${lease120Repair.reason} ` +
        `unit_id=${lease120Repair.unit_id ?? "none"} ` +
        `lease_end_date=${lease120Repair.lease_end_date ?? "none"}`
    );

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

async function transformBronzeReport(
  sql: postgres.Sql,
  bronzeId?: string,
  reportType?: string
): Promise<{ bronze: BronzeAppfolioReport; silver: SilverAppfolioReport; meta: PipelineMetadata }> {
  let bronzeRows: BronzeAppfolioReport[];

  if (bronzeId) {
    bronzeRows = await sql<BronzeAppfolioReport[]>`
      SELECT * FROM bronze_appfolio_reports WHERE id = ${bronzeId} LIMIT 1
    `;
  } else if (reportType) {
    bronzeRows = await sql<BronzeAppfolioReport[]>`
      SELECT *
      FROM bronze_appfolio_reports
      WHERE report_type = ${reportType}
      ORDER BY report_date DESC, ingested_at DESC
      LIMIT 1
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

  // ── VALIDATION HOOK: Silver phase ─────────────────────────────────────────
  // Runs after normalization, before DB write. Non-blocking — warnings only.
  const validationResult = validateSilver(bronze.report_type, normalized_data);
  if (validationResult.anomaly_count > 0) {
    console.warn(
      `[${SERVICE_NAME}] SILVER_VALIDATION WARN — type=${bronze.report_type} anomalies=${validationResult.anomaly_count}:`,
      validationResult.anomalies.map((a) => `${a.field}: ${a.issue}`).join(" | ")
    );
  }
  // Log to pipeline_logs (fire-and-forget — must not block Silver write)
  logSilverValidation(sql, bronze.id, validationResult).catch((err) => {
    console.error(`[${SERVICE_NAME}] logSilverValidation failed (non-fatal):`, err);
  });

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
app.post("/transform/test", async (req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();
    const bronzeId = typeof req.query.bronze_id === "string" ? req.query.bronze_id : undefined;
    const reportType = typeof req.query.report_type === "string" ? req.query.report_type : undefined;
    const { bronze, silver, meta } = await transformBronzeReport(sql, bronzeId, reportType);

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
app.post("/transform/run", async (req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();

    const requestedBronzeId =
      typeof req.body?.bronze_report_id === "string"
        ? req.body.bronze_report_id
        : typeof req.query.bronze_id === "string"
          ? req.query.bronze_id
          : undefined;
    const requestedReportType =
      typeof req.body?.report_type === "string"
        ? req.body.report_type
        : typeof req.query.report_type === "string"
          ? req.query.report_type
          : undefined;

    // Find a Bronze metadata entry that has not yet produced a newer Silver row.
    // Repeated same-day AppFolio refreshes upsert the Bronze row, so the queue must
    // compare stage timestamps rather than only testing whether any Silver row exists.
    // The join to bronze_appfolio_reports prevents stale orphan metadata from blocking
    // the whole queue, and explicit bronze_report_id/report_type requests let ingestion
    // process the report it just wrote instead of draining unrelated historical backlog.
    const candidates = await sql<{ bronze_report_id: string; meta_id: string; created_at: Date }[]>`
      SELECT pm.bronze_report_id, pm.id AS meta_id, pm.created_at
      FROM pipeline_metadata pm
      JOIN bronze_appfolio_reports b ON b.id = pm.bronze_report_id
      WHERE pm.stage = 'bronze'
        AND pm.status = 'created'
        AND pm.bronze_report_id IS NOT NULL
        AND (${requestedBronzeId ?? null}::uuid IS NULL OR pm.bronze_report_id = ${requestedBronzeId ?? null}::uuid)
        AND (${requestedReportType ?? null}::text IS NULL OR b.report_type = ${requestedReportType ?? null})
        AND NOT EXISTS (
          SELECT 1 FROM silver_appfolio_reports s
          WHERE s.bronze_report_id = pm.bronze_report_id
            AND s.transformed_at >= pm.created_at
        )
      ORDER BY
        CASE WHEN ${requestedBronzeId ?? null}::uuid IS NOT NULL THEN pm.created_at END DESC,
        pm.created_at ASC
      LIMIT 1
    `;

    if (candidates.length === 0) {
      res.status(200).json({
        success: true,
        processed: false,
        message: requestedBronzeId || requestedReportType
          ? "No matching unprocessed bronze records found"
          : "No unprocessed bronze records found",
      });
      return;
    }

    const { bronze_report_id, meta_id } = candidates[0];
    console.log(`[${SERVICE_NAME}] POST /transform/run — processing bronze_report_id=${bronze_report_id}`);

    const { bronze, silver, meta } = await transformBronzeReport(sql, bronze_report_id);

    // Mark all older created Bronze metadata for this report as processed once a newer
    // Silver row exists, preventing duplicate same-day metadata from re-queuing forever.
    const markedRows = await sql<{ id: string }[]>`
      UPDATE pipeline_metadata
      SET status = 'processed', updated_at = NOW()
      WHERE stage = 'bronze'
        AND status = 'created'
        AND bronze_report_id = ${bronze_report_id}
        AND created_at <= ${silver.transformed_at}
      RETURNING id
    `;
    console.log(`[${SERVICE_NAME}] POST /transform/run — marked ${markedRows.length} bronze metadata rows processed; selected_meta=${meta_id}`);

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
// Finds the next eligible Silver record (supported report types only),
// resolves the strategy, delegates Gold promotion, logs the result,
// and runs integrity checks after every successful promotion.
app.post("/gold/run", async (_req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();

    const supportedTypes = getSupportedTypes();

      // Bulk-skip all unsupported Silver records that are still pending Gold promotion.
      // Use transformed_at vs. Gold metadata time so a refreshed same-day Bronze row that
      // reuses the same bronze_report_id is not hidden by an older Gold metadata row.
      await sql`
        INSERT INTO pipeline_metadata (bronze_report_id, stage, status)
        SELECT s.bronze_report_id, 'gold', 'skipped'
        FROM silver_appfolio_reports s
        WHERE s.report_type != ALL(${supportedTypes})
          AND NOT EXISTS (
            SELECT 1 FROM pipeline_metadata pm
            WHERE pm.bronze_report_id = s.bronze_report_id
              AND pm.stage = 'gold'
              AND pm.created_at >= s.transformed_at
          )
        ON CONFLICT DO NOTHING
      `;

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
      WHERE s.report_type = ANY(${supportedTypes})
        AND NOT EXISTS (
          SELECT 1 FROM pipeline_metadata pm
          WHERE pm.bronze_report_id = s.bronze_report_id
            AND pm.stage = 'gold'
            AND pm.created_at >= s.transformed_at
        )
      ORDER BY s.transformed_at ASC
      LIMIT 1
    `;

    if (candidates.length === 0) {
      console.log(`[${SERVICE_NAME}] POST /gold/run — no eligible silver records found`);

      // ── INTEGRITY CHECK: runs even when queue is empty ──────────────────
      // This catches regressions that happened outside the normal pipeline
      // (e.g., manual DB edits, failed re-promotions from a previous run).
      const integrityReport = await runIntegrityChecks(sql);
      await logIntegrityReport(sql, integrityReport).catch(() => {});

      if (!integrityReport.all_passed) {
        const failedChecks = integrityReport.checks.filter((c) => !c.passed);
        console.warn(
          `[${SERVICE_NAME}] INTEGRITY CHECK FAILED — ${failedChecks.length} issue(s):`,
          failedChecks.map((c) => `${c.table}: ${c.detail}`).join(" | ")
        );
      }

      // Regenerate actions from current Gold even when nothing new promoted,
      // so the queue reflects the latest canonical state (Release 2).
      await generateSystemActions(sql).catch((err) =>
        console.error(`[${SERVICE_NAME}] generateSystemActions (empty queue) failed:`, err)
      );

      res.status(200).json({
        success: true,
        processed: false,
        reason: "No Silver records pending Gold promotion",
        integrity: integrityReport,
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

    // ── VALIDATION HOOK: Gold phase ───────────────────────────────────────
    // Log the Gold promotion result (non-blocking)
    logGoldPromotion(
      sql,
      bronze_report_id,
      report_type,
      result.gold_ids.length,
      result.skipped,
      result.skip_reason
    ).catch((err) => {
      console.error(`[${SERVICE_NAME}] logGoldPromotion failed (non-fatal):`, err);
    });

    // Record pipeline_metadata stage='gold'
    const goldStatus = result.skipped ? "skipped" : "processed";
    const meta = await insertPipelineMetadata(sql, bronze_report_id, "gold", goldStatus);
    console.log(`[${SERVICE_NAME}] POST /gold/run — pipeline_metadata id=${meta.id} stage=gold status=${goldStatus}`);

    // ── INTEGRITY CHECK: runs after every successful Gold promotion ───────
    // Fire-and-forget — must not block the response
    runIntegrityChecks(sql)
      .then((integrityReport) => {
        logIntegrityReport(sql!, integrityReport).catch(() => {});
        if (!integrityReport.all_passed) {
          const failedChecks = integrityReport.checks.filter((c) => !c.passed);
          console.warn(
            `[${SERVICE_NAME}] POST-GOLD INTEGRITY WARN — ${failedChecks.length} issue(s):`,
            failedChecks.map((c) => `${c.table}: ${c.detail}`).join(" | ")
          );
        }
      })
      .catch((err) => {
        console.error(`[${SERVICE_NAME}] runIntegrityChecks failed (non-fatal):`, err);
      });

    // ── ACTION GENERATION: derive system actions from the fresh Gold data ──
    // (Release 2, item 2.2). Idempotent on natural_key; fire-and-forget.
    generateSystemActions(sql)
      .then((r) => {
        console.log(
          `[${SERVICE_NAME}] POST-GOLD ACTIONS — generated=${r.generated} resolved=${r.resolved} ` +
          `by_type=${JSON.stringify(r.by_type)}`
        );
      })
      .catch((err) => {
        console.error(`[${SERVICE_NAME}] generateSystemActions failed (non-fatal):`, err);
      });

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

// ── GET /validation/logs ──────────────────────────────────────────────────────
// Returns the most recent pipeline log entries for monitoring dashboards.
app.get("/validation/logs", async (_req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();
    const logs = await sql<{
      id: string;
      logged_at: Date;
      stage: string;
      report_type: string;
      row_count: number;
      anomaly_count: number;
      validation_status: string;
      detail: Record<string, unknown>;
    }[]>`
      SELECT id, logged_at, stage, report_type, row_count, anomaly_count, validation_status, detail
      FROM pipeline_logs
      ORDER BY logged_at DESC
      LIMIT 100
    `;
    res.status(200).json({ success: true, count: logs.length, logs });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    res.status(500).json({ success: false, error: message });
  } finally {
    if (sql) await sql.end();
  }
});

// ── POST /actions/generate ────────────────────────────────────────────────
// On-demand system-action regeneration from current Gold, independent of the
// promotion queue. Idempotent; safe to call anytime.
app.post("/actions/generate", async (_req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();
    const result = await generateSystemActions(sql);
    res.status(200).json({ success: true, ...result });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    res.status(500).json({ success: false, error: message });
  } finally {
    if (sql) await sql.end();
  }
});

// ── GET /validation/integrity ─────────────────────────────────────────────────
// Runs an on-demand integrity check and returns the full report.
app.get("/validation/integrity", async (_req: Request, res: Response) => {
  let sql: postgres.Sql | null = null;
  try {
    sql = getDb();
    const report = await runIntegrityChecks(sql);
    await logIntegrityReport(sql, report).catch(() => {});
    res.status(report.all_passed ? 200 : 207).json({
      success: true,
      all_passed: report.all_passed,
      report,
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    res.status(500).json({ success: false, error: message });
  } finally {
    if (sql) await sql.end();
  }
});

// ── GET /strategies ───────────────────────────────────────────────────────────
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
    validation: {
      silver_validator: "active",
      gold_guard: "active",
      integrity_checker: "active",
      pipeline_logger: "active",
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
  console.log(`[${SERVICE_NAME}] registered strategies: ${getSupportedTypes().join(", ")}`);
  console.log(`[${SERVICE_NAME}] validation layer: silverValidator + goldGuard + integrityChecker + pipelineLogger`);
  await checkDatabaseConnectivity();
});

export default app;
