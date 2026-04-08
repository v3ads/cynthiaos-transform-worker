// ── unit_vacancy strategy ─────────────────────────────────────────────────────
//
// Handles the AppFolio "unit_vacancy" report type.
//
// AppFolio sends one row per unit with PascalCase fields:
//   { Unit, UnitId, UnitStatus, Property, PropertyId, SqFt, UnitType,
//     SchdRent, AdvertisedRent, DaysVacant, RentReady, AvailableOn, ... }
//
// UnitStatus values observed: "Occupied", "Vacant-Unrented", "Vacant-Rented",
//   "Notice-Unrented", "Notice-Rented", "Model", "Down"
//
// Silver: counts units by status category → { total_units, occupied_units,
//         vacant_units, notice_units, occupancy_rate, vacancy_rate }
//
// Gold:   promotes one snapshot row per report into gold_occupancy_snapshots.
//         Idempotent via ON CONFLICT (report_date, content_hash) DO UPDATE.

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";

// ── Numeric helpers ───────────────────────────────────────────────────────────

function toInt(val: unknown, fallback = 0): number {
  if (val === null || val === undefined) return fallback;
  const n = Number(val);
  return Number.isFinite(n) ? Math.round(n) : fallback;
}

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
//
// Returns "occupied", "vacant", or "notice" based on the UnitStatus value.

function classifyStatus(status: string): "occupied" | "vacant" | "notice" | "other" {
  const s = status.toLowerCase().trim();
  if (s.startsWith("occupied") || s === "rented" || s === "leased") return "occupied";
  if (s.startsWith("vacant"))  return "vacant";
  if (s.startsWith("notice"))  return "notice";
  return "other"; // model, down, etc.
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

    let occupiedUnits = 0;
    let vacantUnits   = 0;
    let noticeUnits   = 0;

    for (const row of rows) {
      // AppFolio PascalCase: UnitStatus; also support legacy snake_case
      const status = String(row.UnitStatus ?? row.unit_status ?? row.status ?? "");
      const cat = classifyStatus(status);
      if (cat === "occupied") occupiedUnits++;
      else if (cat === "vacant") vacantUnits++;
      else if (cat === "notice") noticeUnits++;
      // "other" (model, down) not counted in totals
    }

    const totalUnits    = occupiedUnits + vacantUnits + noticeUnits;
    const occupancyRate = totalUnits > 0 ? occupiedUnits / totalUnits : null;
    const vacancyRate   = totalUnits > 0 ? vacantUnits  / totalUnits : null;

    return {
      normalized_data: {
        source:         "appfolio",
        report_type:    ctx.bronze.report_type,
        report_date:    reportDate,
        bronze_report_id: ctx.bronze.id,
        transformed_at: new Date().toISOString(),
        total_units:    totalUnits,
        occupied_units: occupiedUnits,
        vacant_units:   vacantUnits,
        notice_units:   noticeUnits,
        occupancy_rate: occupancyRate,
        vacancy_rate:   vacancyRate,
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
      occupancy_rate: number | null;
      vacancy_rate:   number | null;
    };

    const contentHash = require("crypto")
      .createHash("md5")
      .update(`${nd.report_date}|${nd.total_units}|${nd.occupied_units}|${nd.vacant_units}`)
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
