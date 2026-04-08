// ── Pipeline Logger ───────────────────────────────────────────────────────────
//
// Writes structured validation results to the pipeline_logs table.
// Also emits structured JSON to stdout for Railway log aggregation.
//
// Log schema (pipeline_logs table):
//   id              UUID PRIMARY KEY DEFAULT gen_random_uuid()
//   logged_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
//   stage           TEXT NOT NULL  -- 'silver' | 'gold' | 'integrity'
//   report_type     TEXT
//   bronze_report_id UUID
//   row_count       INTEGER
//   anomaly_count   INTEGER
//   validation_status TEXT  -- 'passed' | 'warned' | 'failed'
//   detail          JSONB  -- full ValidationResult or IntegrityReport

import postgres from "postgres";
import { ValidationResult } from "./silverValidator";
import { IntegrityReport } from "./integrityChecker";

const SERVICE_NAME = "cynthiaos-transform-worker";

// ── DB migration: create pipeline_logs if it doesn't exist ───────────────────

export async function ensurePipelineLogsTable(sql: postgres.Sql): Promise<void> {
  await sql`
    CREATE TABLE IF NOT EXISTS pipeline_logs (
      id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      logged_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      stage             TEXT NOT NULL,
      report_type       TEXT,
      bronze_report_id  UUID,
      row_count         INTEGER,
      anomaly_count     INTEGER,
      validation_status TEXT NOT NULL DEFAULT 'passed',
      detail            JSONB
    )
  `;
  // Index for fast querying by date and status
  await sql`
    CREATE INDEX IF NOT EXISTS pipeline_logs_logged_at_idx
    ON pipeline_logs (logged_at DESC)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS pipeline_logs_status_idx
    ON pipeline_logs (validation_status)
    WHERE validation_status != 'passed'
  `;
}

// ── Log a Silver validation result ───────────────────────────────────────────

export async function logSilverValidation(
  sql: postgres.Sql,
  bronzeReportId: string,
  result: ValidationResult
): Promise<void> {
  // Structured stdout log for Railway
  const logEntry = {
    level: result.validation_status === "passed" ? "info" : "warn",
    service: SERVICE_NAME,
    stage: "silver",
    report_type: result.report_type,
    bronze_report_id: bronzeReportId,
    row_count: result.row_count,
    anomaly_count: result.anomaly_count,
    validation_status: result.validation_status,
    anomalies: result.anomalies,
    validated_at: result.validated_at,
  };

  if (result.validation_status === "passed") {
    console.log(`[PIPELINE_LOG] ${JSON.stringify(logEntry)}`);
  } else {
    console.warn(`[PIPELINE_LOG] ${JSON.stringify(logEntry)}`);
  }

  // Persist to DB (non-blocking — failure here must not break the pipeline)
  try {
    await sql`
      INSERT INTO pipeline_logs
        (stage, report_type, bronze_report_id, row_count, anomaly_count, validation_status, detail)
      VALUES (
        'silver',
        ${result.report_type},
        ${bronzeReportId}::uuid,
        ${result.row_count},
        ${result.anomaly_count},
        ${result.validation_status},
        ${sql.json(result as any)}
      )
    `;
  } catch (err) {
    console.error(
      `[${SERVICE_NAME}] pipelineLogger.logSilverValidation — DB write failed (non-fatal):`,
      err instanceof Error ? err.message : String(err)
    );
  }
}

// ── Log a Gold promotion result ───────────────────────────────────────────────

export async function logGoldPromotion(
  sql: postgres.Sql,
  bronzeReportId: string,
  reportType: string,
  goldRowCount: number,
  skipped: boolean,
  skipReason?: string
): Promise<void> {
  const status = skipped ? "warned" : goldRowCount === 0 ? "warned" : "passed";
  const logEntry = {
    level: status === "passed" ? "info" : "warn",
    service: SERVICE_NAME,
    stage: "gold",
    report_type: reportType,
    bronze_report_id: bronzeReportId,
    gold_row_count: goldRowCount,
    skipped,
    skip_reason: skipReason ?? null,
    validation_status: status,
  };

  if (status === "passed") {
    console.log(`[PIPELINE_LOG] ${JSON.stringify(logEntry)}`);
  } else {
    console.warn(`[PIPELINE_LOG] ${JSON.stringify(logEntry)}`);
  }

  try {
    await sql`
      INSERT INTO pipeline_logs
        (stage, report_type, bronze_report_id, row_count, anomaly_count, validation_status, detail)
      VALUES (
        'gold',
        ${reportType},
        ${bronzeReportId}::uuid,
        ${goldRowCount},
        ${skipped ? 1 : 0},
        ${status},
        ${sql.json(logEntry as any)}
      )
    `;
  } catch (err) {
    console.error(
      `[${SERVICE_NAME}] pipelineLogger.logGoldPromotion — DB write failed (non-fatal):`,
      err instanceof Error ? err.message : String(err)
    );
  }
}

// ── Log an integrity report ───────────────────────────────────────────────────

export async function logIntegrityReport(
  sql: postgres.Sql,
  report: IntegrityReport
): Promise<void> {
  const failedChecks = report.checks.filter((c) => !c.passed);
  const status: "passed" | "warned" | "failed" =
    report.all_passed ? "passed" : failedChecks.some((c) => c.check === "row_count" && c.detail.startsWith("CRITICAL")) ? "failed" : "warned";

  const logEntry = {
    level: status === "passed" ? "info" : status === "failed" ? "error" : "warn",
    service: SERVICE_NAME,
    stage: "integrity",
    all_passed: report.all_passed,
    failed_check_count: failedChecks.length,
    failed_checks: failedChecks,
    validation_status: status,
    run_at: report.run_at,
  };

  if (status === "passed") {
    console.log(`[PIPELINE_LOG] ${JSON.stringify(logEntry)}`);
  } else if (status === "failed") {
    console.error(`[PIPELINE_LOG] ${JSON.stringify(logEntry)}`);
  } else {
    console.warn(`[PIPELINE_LOG] ${JSON.stringify(logEntry)}`);
  }

  try {
    await sql`
      INSERT INTO pipeline_logs
        (stage, report_type, row_count, anomaly_count, validation_status, detail)
      VALUES (
        'integrity',
        'all_tables',
        ${report.checks.length},
        ${failedChecks.length},
        ${status},
        ${sql.json(report as any)}
      )
    `;
  } catch (err) {
    console.error(
      `[${SERVICE_NAME}] pipelineLogger.logIntegrityReport — DB write failed (non-fatal):`,
      err instanceof Error ? err.message : String(err)
    );
  }
}
