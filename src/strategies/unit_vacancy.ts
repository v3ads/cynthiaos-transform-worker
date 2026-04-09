/**
 * unit_vacancy strategy — CynthiaOS Transform Worker
 *
 * Silver normalization:
 *   Counts vacant and notice units from the AppFolio unit_vacancy report.
 *   The report only sends non-occupied units, so total_units is resolved
 *   dynamically at Gold promotion time.
 *
 * Gold promotion:
 *   Upserts a daily snapshot into gold_occupancy_snapshots with:
 *     - total_units  = dynamic (from unit_directory Bronze chunks, then Gold fallback)
 *     - occupied_units = total_units - vacant_units - notice_units
 *     - occupancy_rate = occupied_units / total_units  (non-vacant / total)
 *
 * Change detection:
 *   If total_units changes by more than 5 vs the previous snapshot, a WARNING
 *   is logged so the team can investigate before the next daily run.
 */

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";
import * as crypto from "crypto";

// ── Threshold for significant unit-count change ───────────────────────────────
const UNIT_COUNT_CHANGE_THRESHOLD = 5;

// ── Helpers ───────────────────────────────────────────────────────────────────

function classifyStatus(status: string): "vacant" | "notice" | null {
  const s = (status ?? "").toLowerCase();
  if (s.startsWith("vacant")) return "vacant";
  if (s.startsWith("notice")) return "notice";
  return null;
}

/**
 * Derive total_units from the canonical gold_units table.
 *
 * Priority:
 *   1. COUNT(*) from gold_units — the canonical unit roster populated by
 *      the unit_directory transform strategy. This is the authoritative
 *      source for the entire system.
 *   2. SUM of jsonb_array_length(raw_data->'results') across all unit_directory
 *      Bronze chunks for the same report_date (fallback if gold_units not yet
 *      populated, e.g. first pipeline run).
 *   3. Previous gold_occupancy_snapshots total_units (historical fallback).
 *
 * Returns { total: number, source: string }
 */
async function deriveTotalUnits(
  sql: TransformContext["sql"],
  reportDate: string
): Promise<{ total: number; source: string }> {
  // Source 1: gold_units — canonical unit roster (preferred)
  try {
    const rows = await sql`
      SELECT COUNT(*)::int AS cnt FROM gold_units
    `;
    const cnt = Number((rows as any)[0]?.cnt ?? 0);
    if (cnt > 0) {
      return { total: cnt, source: "gold_units" };
    }
  } catch (err) {
    console.warn(`[unit_vacancy] gold_units query failed: ${err}`);
  }

  // Source 2: unit_directory Bronze chunks for same report_date (bootstrap fallback)
  try {
    const rows = await sql`
      SELECT COALESCE(SUM(jsonb_array_length(raw_data->'results')), 0) AS cnt
      FROM bronze_appfolio_reports
      WHERE report_type = 'unit_directory'
        AND report_date = ${reportDate}
    `;
    const cnt = Number((rows as any)[0]?.cnt ?? 0);
    if (cnt > 0) {
      return { total: cnt, source: "unit_directory_bronze" };
    }
  } catch (err) {
    console.warn(`[unit_vacancy] unit_directory bronze fallback failed: ${err}`);
  }

  // Source 3: previous snapshot
  try {
    const rows = await sql`
      SELECT total_units FROM gold_occupancy_snapshots
      ORDER BY report_date DESC LIMIT 1
    `;
    const cnt = Number((rows as any)[0]?.total_units ?? 0);
    if (cnt > 0) {
      return { total: cnt, source: "previous_snapshot" };
    }
  } catch (err) {
    console.warn(`[unit_vacancy] previous snapshot fallback failed: ${err}`);
  }

  return { total: 0, source: "none" };
}

// ── Strategy ──────────────────────────────────────────────────────────────────
export const unitVacancyStrategy: TransformStrategy = {

  // ── Silver normalization ──────────────────────────────────────────────────
  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data as {
      results?: Record<string, unknown>[];
      data?:    Record<string, unknown>[];
    };

    // AppFolio unit_vacancy uses chunked 'results' key
    const rows: Record<string, unknown>[] = raw.results ?? raw.data ?? [];

    let vacantUnits = 0;
    let noticeUnits = 0;

    for (const row of rows) {
      const status = String(row["UnitStatus"] ?? row["unit_status"] ?? "");
      const bucket = classifyStatus(status);
      if (bucket === "vacant") vacantUnits++;
      else if (bucket === "notice") noticeUnits++;
    }

    return {
      normalized_data: {
        report_date:  ctx.bronze.report_date,
        vacant_units: vacantUnits,
        notice_units: noticeUnits,
        row_count:    rows.length,
      },
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
    };

    const reportDate  = nd.report_date ?? ctx.bronze.report_date;
    const vacantUnits = Number(nd.vacant_units ?? 0);
    const noticeUnits = Number(nd.notice_units ?? 0);

    // ── Dynamically resolve total_units ──────────────────────────────────
    const { total: totalUnits, source: totalSource } =
      await deriveTotalUnits(ctx.sql, reportDate);

    if (totalUnits === 0) {
      console.warn(
        `[unit_vacancy] WARN: Could not derive total_units for ${reportDate} — skipping Gold promotion`
      );
      return { gold_ids: [], skipped: true, skip_reason: "total_units_unresolvable" };
    }

    // ── Change-detection warning ──────────────────────────────────────────
    try {
      const prev = await ctx.sql`
        SELECT total_units FROM gold_occupancy_snapshots
        ORDER BY report_date DESC LIMIT 1
      `;
      const prevTotal = Number((prev as any)[0]?.total_units ?? 0);
      if (prevTotal > 0) {
        const delta = totalUnits - prevTotal;
        if (Math.abs(delta) > UNIT_COUNT_CHANGE_THRESHOLD) {
          console.warn(
            `[unit_vacancy] WARN: total_units changed significantly: ` +
            `${prevTotal} → ${totalUnits} (Δ${delta > 0 ? "+" : ""}${delta}) ` +
            `source=${totalSource} date=${reportDate}`
          );
        }
      }
    } catch (_) {
      // Non-critical — do not block promotion
    }

    // ── Occupancy metrics ─────────────────────────────────────────────────
    // Occupancy = all non-vacant units (occupied + notice)
    const occupiedUnits = Math.max(0, totalUnits - vacantUnits);
    const occupancyRate = parseFloat((occupiedUnits / totalUnits).toFixed(4));
    const vacancyRate   = parseFloat((vacantUnits   / totalUnits).toFixed(4));

    console.log(
      `[unit_vacancy] date=${reportDate} total=${totalUnits}(${totalSource}) ` +
      `vacant=${vacantUnits} notice=${noticeUnits} occupied=${occupiedUnits} ` +
      `rate=${(occupancyRate * 100).toFixed(1)}%`
    );

    const contentHash = crypto
      .createHash("md5")
      .update(`${reportDate}|${totalUnits}|${occupiedUnits}|${vacantUnits}|${noticeUnits}`)
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
      ON CONFLICT (bronze_report_id)
      DO UPDATE SET
        total_units    = EXCLUDED.total_units,
        occupied_units = EXCLUDED.occupied_units,
        vacant_units   = EXCLUDED.vacant_units,
        occupancy_rate = EXCLUDED.occupancy_rate,
        vacancy_rate   = EXCLUDED.vacancy_rate,
        content_hash   = EXCLUDED.content_hash
      RETURNING id
    `;

    const goldIds = (rows as unknown as { id: string }[]).map((r) => r.id);
    return { gold_ids: goldIds, skipped: false };
  },
};
