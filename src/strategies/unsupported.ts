// ── Strategy: unsupported ─────────────────────────────────────────────────────
//
// Fallback strategy for any report_type that does not yet have a registered
// handler. It safely normalises the Bronze payload into a minimal Silver record
// (preserving the raw data) and skips Gold promotion entirely.
//
// This prevents data corruption and pipeline stalls when new report types are
// ingested before their handlers are implemented.

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";

export const unsupportedStrategy: TransformStrategy = {
  // ── Silver normalisation ──────────────────────────────────────────────────
  // Wraps the raw Bronze payload verbatim — no field extraction is attempted.
  // The Silver record is still created so the pipeline tracks the record and
  // does not attempt to re-process it on every run.
  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    return {
      normalized_data: {
        source: "appfolio",
        report_type: ctx.bronze.report_type,
        report_date: ctx.bronze.report_date,
        bronze_report_id: ctx.bronze.id,
        transformed_at: new Date().toISOString(),
        unsupported: true,
        note: `No Silver normalisation handler registered for report_type='${ctx.bronze.report_type}'. Raw data preserved.`,
        raw_data: ctx.bronze.raw_data,
      },
    };
  },

  // ── Gold promotion ────────────────────────────────────────────────────────
  // Always skips — there is no Gold table for unsupported report types.
  async promoteGold(
    _ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    return {
      gold_ids: [],
      skipped: true,
      skip_reason: `No Gold promotion handler registered for report_type='${_ctx.bronze.report_type}'`,
    };
  },
};
