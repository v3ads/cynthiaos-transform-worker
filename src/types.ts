// ── Shared types for the CynthiaOS transform worker ──────────────────────────

import postgres from "postgres";

// ── Raw database row types ────────────────────────────────────────────────────

export interface BronzeAppfolioReport {
  id: string;
  report_type: string;
  report_date: string;
  raw_data: Record<string, unknown>;
  ingested_at: Date;
}

export interface SilverAppfolioReport {
  id: string;
  bronze_report_id: string;
  report_type: string;
  report_date: string;
  normalized_data: Record<string, unknown>;
  transformed_at: Date;
}

export interface PipelineMetadata {
  id: string;
  bronze_report_id: string | null;
  stage: string;
  status: string;
  created_at: Date;
  updated_at: Date;
}

export interface GoldLeaseExpiration {
  id: string;
  bronze_report_id: string | null;
  tenant_id: string;
  unit_id: string;
  lease_start_date: string | null;
  lease_end_date: string | null;
  days_until_expiration: number | null;
  created_at: Date;
}

// ── Strategy pattern interfaces ───────────────────────────────────────────────

/**
 * Context passed to every strategy method.
 * Provides the database client and the source Bronze record.
 */
export interface TransformContext {
  sql: postgres.Sql;
  bronze: BronzeAppfolioReport;
  reportDate: string | null; // YYYY-MM-DD derived from bronze.report_date
}

/**
 * Result returned from a Silver normalisation strategy.
 * `normalized_data` is stored verbatim in silver_appfolio_reports.
 */
export interface SilverNormalizeResult {
  normalized_data: Record<string, unknown>;
}

/**
 * Result returned from a Gold promotion strategy.
 * `gold_ids` is the list of UUIDs inserted (may be empty if all rows conflicted).
 * `skipped` is true when the strategy intentionally produces no Gold rows
 * (e.g., a financial report type that has no Gold table yet).
 */
export interface GoldPromoteResult {
  gold_ids: string[];
  skipped: boolean;
  skip_reason?: string;
}

/**
 * A TransformStrategy encapsulates the Silver normalisation and Gold promotion
 * logic for a single report type.
 *
 * - `normalizeSilver`: transforms raw Bronze data into a structured Silver payload.
 * - `promoteGold`: reads normalized Silver data and writes to the appropriate Gold table(s).
 */
export interface TransformStrategy {
  /**
   * Normalise the Bronze payload into a Silver-layer structure.
   * Must be deterministic and idempotent — the same Bronze input always
   * produces the same Silver output.
   */
  normalizeSilver(ctx: TransformContext): SilverNormalizeResult;

  /**
   * Promote a Silver record to the Gold layer.
   * Must be idempotent — duplicate calls for the same bronze_report_id must
   * not create duplicate Gold rows (enforce via ON CONFLICT DO NOTHING).
   */
  promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult>;
}
