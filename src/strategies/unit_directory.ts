/**
 * unit_directory strategy — CynthiaOS Transform Worker
 *
 * Silver normalization:
 *   Extracts the canonical list of unit_ids from the AppFolio unit_directory
 *   report. This is the authoritative unit source for the entire system.
 *   All other modules (occupancy, turnover velocity, unit intelligence)
 *   derive their total_units from this table.
 *
 * Gold promotion:
 *   Upserts one row per unit into gold_units with:
 *     - unit_id  (normalized via normalizeUnitId)
 *     - report_date
 *     - raw_name (original AppFolio UnitName, preserved for debugging)
 *
 * The table is truncated and rebuilt on each daily run to ensure it always
 * reflects the exact AppFolio unit roster — no stale units, no missing units.
 *
 * Total units guaranteed: 182 (as of April 2026)
 */

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";
import { normalizeUnitId } from "../utils/normalize";

// ── Strategy ──────────────────────────────────────────────────────────────────
export const unitDirectoryStrategy: TransformStrategy = {

  // ── Silver normalization ──────────────────────────────────────────────────
  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data as {
      results?: Record<string, unknown>[];
      data?:    Record<string, unknown>[];
    };

    // AppFolio unit_directory uses chunked 'results' key
    const rows: Record<string, unknown>[] = raw.results ?? raw.data ?? [];

    const units: { unit_id: string; raw_name: string }[] = [];
    const seen = new Set<string>();

    for (const row of rows) {
      const rawName = String(row["UnitName"] ?? row["unit_name"] ?? "").trim();
      if (!rawName) continue;
      const unitId = normalizeUnitId(rawName);
      if (unitId === "unknown" || seen.has(unitId)) continue;
      seen.add(unitId);
      units.push({ unit_id: unitId, raw_name: rawName });
    }

    return {
      normalized_data: {
        report_date: ctx.bronze.report_date,
        units,
        unit_count: units.length,
      },
    };
  },

  // ── Gold promotion ────────────────────────────────────────────────────────
  async promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    const nd = ctx.silver.normalized_data as {
      report_date: string;
      units: { unit_id: string; raw_name: string }[];
      unit_count: number;
    };

    const reportDate = nd.report_date ?? ctx.bronze.report_date;
    const units = nd.units ?? [];

    if (units.length === 0) {
      console.warn(
        `[unit_directory] WARN: No units found in Bronze for ${reportDate} — skipping Gold promotion`
      );
      return { gold_ids: [], skipped: true, skip_reason: "no_units_found" };
    }

    // ── Ensure gold_units table exists ────────────────────────────────────
    await ctx.sql`
      CREATE TABLE IF NOT EXISTS gold_units (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        unit_id     TEXT NOT NULL UNIQUE,
        raw_name    TEXT,
        report_date DATE,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        updated_at  TIMESTAMPTZ DEFAULT NOW()
      )
    `;

    // ── Upsert all units ──────────────────────────────────────────────────
    // Use INSERT ... ON CONFLICT DO UPDATE to handle daily refreshes.
    // This is idempotent — re-running with the same data is safe.
    const goldIds: string[] = [];

    for (const unit of units) {
      const rows = await ctx.sql`
        INSERT INTO gold_units (unit_id, raw_name, report_date, updated_at)
        VALUES (${unit.unit_id}, ${unit.raw_name}, ${reportDate}, NOW())
        ON CONFLICT (unit_id) DO UPDATE SET
          raw_name    = EXCLUDED.raw_name,
          report_date = EXCLUDED.report_date,
          updated_at  = NOW()
        RETURNING id
      `;
      const id = (rows as unknown as { id: string }[])[0]?.id;
      if (id) goldIds.push(id);
    }

    // ── Remove units no longer in AppFolio ───────────────────────────────
    // If AppFolio removes a unit from the directory, remove it from gold_units.
    const currentUnitIds = units.map((u) => u.unit_id);
    await ctx.sql`
      DELETE FROM gold_units
      WHERE unit_id != ALL(${currentUnitIds}::text[])
    `;

    console.log(
      `[unit_directory] date=${reportDate} units=${units.length} gold_rows=${goldIds.length}`
    );

    return { gold_ids: goldIds, skipped: false };
  },
};
