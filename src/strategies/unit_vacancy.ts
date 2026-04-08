// ── unit_vacancy strategy ─────────────────────────────────────────────────────
//
// Handles the AppFolio "unit_vacancy" report type.
//
// IMPORTANT: The AppFolio unit_vacancy report ONLY sends vacant and notice
// units. It does NOT include occupied units. The total unit count (182) is
// a fixed property-level constant derived from the unit_directory report.
//
// AppFolio sends one row per vacant/notice unit with PascalCase fields:
//   { Unit, UnitId, UnitStatus, Property, PropertyId, SqFt, ... }
//
// UnitStatus values observed in this report:
//   "Vacant-Unrented", "Vacant-Rented", "Notice-Unrented", "Notice-Rented"
//
// Occupancy definition (per business requirement):
//   occupied_units = TOTAL_UNITS - vacant_units - notice_units
//   occupancy_rate = occupied_units / TOTAL_UNITS
//   vacancy_rate   = (vacant_units + notice_units) / TOTAL_UNITS
//
// Silver: counts vacant/notice rows → derives occupied from total constant.
// Gold:   promotes one snapshot row per report into gold_occupancy_snapshots.
//         Idempotent via ON CONFLICT (report_date, content_hash) DO UPDATE.

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";

// ── Property constant ─────────────────────────────────────────────────────────
// Total rentable units across all properties managed in AppFolio.
// Source: unit_directory report (19 property records, 182 distinct UnitName values).
// Update this constant only if the property portfolio changes.
const TOTAL_UNITS = 182;

// ── Date extraction helper ────────────────────────────────────────────────────
function toDateStr(val: unknown, fallback: string): string {
  if (!val) return fallback;
  if (val instanceof Date) return val.toISOString().slice(0, 10);
  const s = String(val).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) {
    const [m, d, y] = s.split("/");
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) return s.slice(0, 10);
  return fallback;
}

// ── Classify a UnitStatus string ──────────────────────────────────────────────
// The unit_vacancy report only sends vacant/notice units, but we classify
// defensively in case AppFolio ever includes other statuses.
function classifyStatus(status: string): "vacant" | "notice" | "other" {
  const s = status.toLowerCase().trim();
  if (s.startsWith("vacant")) return "vacant";
  if (s.startsWith("notice")) return "notice";
  return "other"; // occupied, model, down — not expected in this report
}

// ── Strategy ─────────────────────────────────────────────────────────────────
export const unitVacancyStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────
  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw   = ctx.bronze.raw_data;
    const today = new Date().toISOString().slice(0, 10);
    const reportDate =
      ctx.reportDate ||
      toDateStr(
        ctx.bronze.report_date ?? raw.report_date ?? raw.period_end ?? raw.as_of_date,
        today
      );

    // AppFolio sends rows under raw.results
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];

    let vacantUnits = 0;
    let noticeUnits = 0;

    for (const row of rows) {
      const status = String(row.UnitStatus ?? row.unit_status ?? row.status ?? "");
      const cat = classifyStatus(status);
      if (cat === "vacant") vacantUnits++;
      else if (cat === "notice") noticeUnits++;
      // "other" rows (occupied, model, down) are ignored — not expected here
    }

    // Occupied = all units not reported as vacant or notice
    const occupiedUnits = TOTAL_UNITS - vacantUnits - noticeUnits;
    const occupancyRate = occupiedUnits / TOTAL_UNITS;
    const vacancyRate   = (vacantUnits + noticeUnits) / TOTAL_UNITS;

    return {
      normalized_data: {
        source:           "appfolio",
        report_type:      ctx.bronze.report_type,
        report_date:      reportDate,
        bronze_report_id: ctx.bronze.id,
        transformed_at:   new Date().toISOString(),
        total_units:      TOTAL_UNITS,
        occupied_units:   occupiedUnits,
        vacant_units:     vacantUnits,
        notice_units:     noticeUnits,
        occupancy_rate:   occupancyRate,
        vacancy_rate:     vacancyRate,
      },
    };
  },

  // ── Gold promotion ────────────────────────────────────────────────────────
  async promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    const nd = ctx.silver.normalized_data as {
      report_date:    string;
      total_units:    number;
      occupied_units: number;
      vacant_units:   number;
      notice_units:   number;
      occupancy_rate: number;
      vacancy_rate:   number;
    };

    const contentHash = require("crypto")
      .createHash("md5")
      .update(
        `${nd.report_date}|${nd.total_units}|${nd.occupied_units}|${nd.vacant_units}|${nd.notice_units}`
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
        ${nd.report_date},
        ${nd.total_units},
        ${nd.occupied_units},
        ${nd.vacant_units},
        ${nd.occupancy_rate},
        ${nd.vacancy_rate},
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
