// ── occupancy_summary strategy ────────────────────────────────────────────────
//
// Handles the AppFolio "Occupancy Summary" report.
// Normalises unit counts and derives occupancy_rate / vacancy_rate.
//
// Supported payload shapes:
//   1. Summary object: { total_units, occupied_units, vacant_units, occupancy_rate? }
//   2. Rows array: [{ status: "Occupied"|"Vacant", count: N }, ...]
//
// Gold table: gold_occupancy_snapshots (one row per report)
// Idempotency: ON CONFLICT (bronze_report_id) DO NOTHING

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types.js";

// ── Numeric helpers ───────────────────────────────────────────────────────────

function toInt(val: unknown, fallback = 0): number {
  if (val === null || val === undefined) return fallback;
  const n = Number(val);
  return Number.isFinite(n) ? Math.round(n) : fallback;
}

function toFloat(val: unknown): number | null {
  if (val === null || val === undefined) return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

// ── Date extraction helper ────────────────────────────────────────────────────

function toDateStr(val: unknown, fallback: string): string {
  if (!val) return fallback;
  // Handle Date objects returned by the postgres driver for DATE columns
  if (val instanceof Date) return val.toISOString().slice(0, 10);
  const s = String(val).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) {
    const [m, d, y] = s.split("/");
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }
  // Handle ISO datetime strings like "2025-09-30T00:00:00.000Z"
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) return s.slice(0, 10);
  return fallback;
}

// ── Extract summary from rows array ──────────────────────────────────────────
//
// AppFolio sometimes exports occupancy as a list of status rows, e.g.:
//   [{ status: "Occupied", count: 48 }, { status: "Vacant", count: 4 }]

function extractFromRows(rows: Record<string, unknown>[]): {
  totalUnits: number;
  occupiedUnits: number;
  vacantUnits: number;
} {
  let occupiedUnits = 0;
  let vacantUnits = 0;

  for (const row of rows) {
    const status = String(row.status ?? row.unit_status ?? row.type ?? "").toLowerCase().trim();
    const count = toInt(row.count ?? row.unit_count ?? row.units ?? 0);

    if (status.includes("occupied") || status === "leased" || status === "rented") {
      occupiedUnits += count;
    } else if (status.includes("vacant") || status === "available" || status === "empty") {
      vacantUnits += count;
    }
  }

  return {
    totalUnits: occupiedUnits + vacantUnits,
    occupiedUnits,
    vacantUnits,
  };
}

// ── Rate derivation ───────────────────────────────────────────────────────────

function deriveRates(
  occupiedUnits: number,
  vacantUnits: number,
  totalUnits: number,
  providedOccupancyRate: number | null
): { occupancyRate: number | null; vacancyRate: number | null } {
  if (totalUnits === 0) return { occupancyRate: null, vacancyRate: null };

  const occupancyRate =
    providedOccupancyRate !== null
      ? providedOccupancyRate
      : occupiedUnits / totalUnits;

  const vacancyRate = vacantUnits / totalUnits;

  return { occupancyRate, vacancyRate };
}

// ── Strategy ─────────────────────────────────────────────────────────────────

export const occupancySummaryStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────

  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw   = ctx.bronze.raw_data;
    const today = new Date().toISOString().slice(0, 10);

    // Use ctx.reportDate (pre-computed YYYY-MM-DD) as primary source
    const reportDate =
      ctx.reportDate ||
      toDateStr(
        ctx.bronze.report_date ?? raw.report_date ?? raw.period_end ?? raw.as_of_date,
        today
      );

    let totalUnits: number;
    let occupiedUnits: number;
    let vacantUnits: number;
    let providedOccupancyRate: number | null = null;

    const rows = Array.isArray(raw.rows) ? (raw.rows as Record<string, unknown>[]) : [];

    if (rows.length > 0) {
      // Shape 2: status rows array
      const extracted = extractFromRows(rows);
      totalUnits    = extracted.totalUnits;
      occupiedUnits = extracted.occupiedUnits;
      vacantUnits   = extracted.vacantUnits;
    } else {
      // Shape 1: flat summary object
      const summary = (raw.summary ?? raw) as Record<string, unknown>;

      totalUnits = toInt(
        summary.total_units ?? summary.unit_count ?? summary.units
      );
      occupiedUnits = toInt(
        summary.occupied_units ?? summary.occupied ?? summary.leased_units
      );
      vacantUnits = toInt(
        summary.vacant_units ?? summary.vacant ?? summary.available_units
      );

      // If total_units not provided, derive from occupied + vacant
      if (totalUnits === 0 && (occupiedUnits + vacantUnits) > 0) {
        totalUnits = occupiedUnits + vacantUnits;
      }

      // If vacant_units not provided, derive from total - occupied
      if (vacantUnits === 0 && totalUnits > 0 && occupiedUnits > 0) {
        vacantUnits = totalUnits - occupiedUnits;
      }

      // If occupied_units not provided, derive from total - vacant
      if (occupiedUnits === 0 && totalUnits > 0 && vacantUnits > 0) {
        occupiedUnits = totalUnits - vacantUnits;
      }

      // Provided occupancy rate (may be a decimal 0.95 or a percentage 95)
      const rawRate = toFloat(
        summary.occupancy_rate ?? summary.occupancy_pct ?? summary.occupancy_percent
      );
      if (rawRate !== null) {
        // Normalise to decimal: if > 1, assume percentage
        providedOccupancyRate = rawRate > 1 ? rawRate / 100 : rawRate;
      }
    }

    const { occupancyRate, vacancyRate } = deriveRates(
      occupiedUnits, vacantUnits, totalUnits, providedOccupancyRate
    );

    return {
      normalized_data: {
        report_date:    reportDate,
        total_units:    totalUnits,
        occupied_units: occupiedUnits,
        vacant_units:   vacantUnits,
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

    // Compute a stable content hash for idempotency across re-uploads
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
      ON CONFLICT (report_date, content_hash) DO NOTHING
      RETURNING id
    `;

    const goldIds = (rows as unknown as { id: string }[]).map((r) => r.id);

    return { gold_ids: goldIds, skipped: false };
  },
};
