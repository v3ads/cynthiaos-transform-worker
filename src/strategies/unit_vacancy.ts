/**
 * unit_vacancy strategy — CynthiaOS Transform Worker
 *
 * Silver normalization:
 *   Counts vacant and notice units from the AppFolio unit_vacancy report.
 *   The report only sends non-occupied units, so total_units is derived
 *   dynamically from the unit_directory Bronze records (preferred) or falls
 *   back to COUNT(DISTINCT unit_id) from gold_lease_expirations.
 *
 * Gold promotion:
 *   Upserts a daily snapshot into gold_occupancy_snapshots with:
 *     - total_units  = dynamic (unit_directory count, not hardcoded)
 *     - occupied_units = total_units - vacant_units - notice_units
 *     - occupancy_rate = occupied_units / total_units
 *
 * Change detection:
 *   If the derived total_units differs from the previous snapshot by more
 *   than 5 units, a WARNING is logged so the team can investigate.
 */

import { TransformStrategy, TransformContext, GoldPromoteResult } from "../types";
import { normalizeUnitId } from "../utils/normalize";
import { SilverAppfolioReport } from "../types";
import * as crypto from "crypto";

// ── Change-detection threshold ────────────────────────────────────────────────
const UNIT_COUNT_CHANGE_THRESHOLD = 5;

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Classify an AppFolio UnitStatus string into one of three buckets.
 * The unit_vacancy report only sends vacant and notice units.
 */
function classifyStatus(status: string): "vacant" | "notice" | null {
  const s = (status ?? "").toLowerCase();
  if (s.startsWith("vacant")) return "vacant";
  if (s.startsWith("notice")) return "notice";
  return null; // occupied units are not present in this report
}

/**
 * Derive total_units dynamically from the database.
 *
 * Priority order:
 *   1. COUNT of distinct active UnitName values across all unit_directory
 *      Bronze chunks for the same report_date (most authoritative).
 *   2. COUNT(DISTINCT unit_id) from gold_lease_expirations (Gold fallback —
 *      only counts leased units, so may undercount vacant units).
 *   3. Previous gold_occupancy_snapshots total_units value (historical fallback).
 *
 * Returns { total: number, source: string }
 */
async function deriveTotalUnits(
  ctx: TransformContext,
  reportDate: string
): Promise<{ total: number; source: string }> {
  // ── Source 1: unit_directory Bronze chunks ──────────────────────────────
  try {
    const rows = await ctx.sql`
      SELECT SUM(jsonb_array_length(raw_data->'results')) AS cnt
      FROM bronze_appfolio_reports
      WHERE report_type = 'unit_directory'
        AND report_date = ${reportDate}
    `;
    const cnt = Number((rows as any)[0]?.cnt ?? 0);
    if (cnt > 0) {
      return { total: cnt, source: "unit_directory_bronze" };
    }
  } catch (_) {
    // fall through
  }

  // ── Source 2: gold_lease_expirations distinct unit_ids ──────────────────
  try {
    const rows = await ctx.sql`
      SELECT COUNT(DISTINCT unit_id) AS cnt
      FROM gold_lease_expirations
      WHERE unit_id IS NOT NULL AND unit_id != 'unknown'
    `;
    const cnt = Number((rows as any)[0]?.cnt ?? 0);
    if (cnt > 0) {
      return { total: cnt, source: "gold_lease_expirations_distinct_units" };
    }
  } catch (_) {
    // fall through
  }

  // ── Source 3: previous snapshot ─────────────────────────────────────────
  try {
    const rows = await ctx.sql`
      SELECT total_units
      FROM gold_occupancy_snapshots
      ORDER BY report_date DESC
      LIMIT 1
    `;
    const cnt = Number((rows as any)[0]?.total_units ?? 0);
    if (cnt > 0) {
      return { total: cnt, source: "previous_snapshot" };
    }
  } catch (_) {
    // fall through
  }

  // ── No source available — return 0 so the caller can skip ───────────────
  return { total: 0, source: "none" };
}

// ── Strategy ──────────────────────────────────────────────────────────────────
export const unitVacancyStrategy: TransformStrategy = {
  // ── Silver normalization ─────────────────────────────────────────────────
  normalize(ctx: TransformContext): Record<string, unknown> {
    const raw = ctx.bronze.raw_data as {
      results?: Record<string, unknown>[];
      data?:    Record<string, unknown>[];
      [key: string]: unknown;
    };

    // AppFolio unit_vacancy uses chunked 'results' key
    const rows: Record<string, unknown>[] =
      raw.results ?? raw.data ?? [];

    let vacantUnits = 0;
    let noticeUnits = 0;

    for (const row of rows) {
      const status = String(row["UnitStatus"] ?? row["unit_status"] ?? "");
      const bucket = classifyStatus(status);
      if (bucket === "vacant") vacantUnits++;
      else if (bucket === "notice") noticeUnits++;
    }

    return {
      report_date:  ctx.bronze.report_date,
      // total_units is intentionally omitted here — it is resolved
      // dynamically at Gold promotion time from the database.
      vacant_units: vacantUnits,
      notice_units: noticeUnits,
      row_count:    rows.length,
    };
  },

  // ── Gold promotion ────────────────────────────────────────────────────────
  async promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    const nd = ctx.silver.normalized_data as {
      report_date:  string;
      vacant_units: number;
      notice_units: number;
      row_count:    number;
    };

    const reportDate  = nd.report_date;
    const vacantUnits = Number(nd.vacant_units ?? 0);
    const noticeUnits = Number(nd.notice_units ?? 0);

    // ── Dynamically resolve total_units ──────────────────────────────────
    const { total: totalUnits, source: totalSource } =
      await deriveTotalUnits(ctx, reportDate);

    if (totalUnits === 0) {
      console.warn(
        `[unit_vacancy] WARN: Could not derive total_units for ${reportDate} — skipping Gold promotion`
      );
      return { gold_ids: [], skipped: true };
    }

    // ── Change-detection warning ──────────────────────────────────────────
    try {
      const prev = await ctx.sql`
        SELECT total_units FROM gold_occupancy_snapshots
        ORDER BY report_date DESC LIMIT 1
      `;
      const prevTotal = Number((prev as any)[0]?.total_units ?? 0);
      if (prevTotal > 0) {
        const delta = Math.abs(totalUnits - prevTotal);
        if (delta > UNIT_COUNT_CHANGE_THRESHOLD) {
          console.warn(
            `[unit_vacancy] WARN: total_units changed significantly: ` +
            `${prevTotal} → ${totalUnits} (Δ${delta > 0 ? "+" : ""}${totalUnits - prevTotal}) ` +
            `source=${totalSource} date=${reportDate}`
          );
        }
      }
    } catch (_) {
      // Non-critical — do not block promotion
    }

    // ── Calculate occupancy metrics ───────────────────────────────────────
    // Occupancy = all non-vacant units (occupied + notice)
    const nonVacantUnits = totalUnits - vacantUnits;
    const occupiedUnits  = Math.max(0, nonVacantUnits);
    const occupancyRate  = parseFloat((occupiedUnits / totalUnits).toFixed(4));
    const vacancyRate    = parseFloat((vacantUnits   / totalUnits).toFixed(4));

    console.log(
      `[unit_vacancy] date=${reportDate} total=${totalUnits}(${totalSource}) ` +
      `vacant=${vacantUnits} notice=${noticeUnits} occupied=${occupiedUnits} ` +
      `rate=${(occupancyRate * 100).toFixed(1)}%`
    );

    const contentHash = crypto
      .createHash("md5")
      .update(
        `${reportDate}|${totalUnits}|${occupiedUnits}|${vacantUnits}|${noticeUnits}`
      )
      .digest("hex");

    const rows = await ctx.sql`
      INSERT INTO gold_occupancy_snapshots (
        bronze_report_id,
        report_date,
        total_units,
        occupied_units,
        vacant_units,
        occupancy_rate,
        vacancy_rate,
        content_hash
      ) VALUES (
        ${ctx.bronze.id},
        ${reportDate},
        ${totalUnits},
        ${occupiedUnits},
        ${vacantUnits},
        ${occupancyRate},
        ${vacancyRate},
        ${contentHash}
      )
      ON CONFLICT (report_date, content_hash)
      DO UPDATE SET
        bronze_report_id = EXCLUDED.bronze_report_id,
        total_units      = EXCLUDED.total_units,
        occupied_units   = EXCLUDED.occupied_units,
        vacant_units     = EXCLUDED.vacant_units,
        occupancy_rate   = EXCLUDED.occupancy_rate,
        vacancy_rate     = EXCLUDED.vacancy_rate
      RETURNING id
    `;

    const goldIds = (rows as unknown as { id: string }[]).map((r) => r.id);
    return { gold_ids: goldIds, skipped: false };
  },
};
