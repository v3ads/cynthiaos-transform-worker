// ── income_statement strategy ─────────────────────────────────────────────────
//
// Silver: normalises an AppFolio income statement report into a single summary
//         row per report:
//         { report_date, total_income, rental_income, other_income,
//           total_expenses, operating_expenses, net_operating_income }
//
// Gold:   promotes one row per report into gold_income_statements.
//         Derives profit_margin = net_operating_income / total_income.
//         Idempotent via UNIQUE constraint on bronze_report_id.
//
// Note:   AppFolio income statements are property-level summaries, not
//         per-tenant rows. The payload may arrive as a flat summary object
//         OR as a rows array where each row is a line-item category.
//         Both shapes are handled.

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";

// ── Gold row interface ────────────────────────────────────────────────────────

interface GoldIncomeStatement {
  id: string;
  bronze_report_id: string | null;
  report_date: Date;
  total_income: string;
  rental_income: string;
  other_income: string;
  total_expenses: string;
  operating_expenses: string;
  net_operating_income: string;
  profit_margin: string | null;
  created_at: Date;
}

// ── Numeric extraction helper ─────────────────────────────────────────────────

function toNum(val: unknown): number {
  if (typeof val === "number") return val;
  if (typeof val === "string") {
    const n = parseFloat(val.replace(/[^0-9.-]/g, ""));
    return isNaN(n) ? 0 : n;
  }
  return 0;
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
  // Handle ISO datetime strings like "2025-03-31T00:00:00.000Z"
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) return s.slice(0, 10);
  return fallback;
}

// ── Profit margin derivation ──────────────────────────────────────────────────

function deriveProfitMargin(noi: number, totalIncome: number): number | null {
  if (totalIncome === 0) return null;
  return noi / totalIncome;
}

// ── Extract summary from rows array (line-item format) ────────────────────────
//
// AppFolio sometimes exports income statements as a list of line items, e.g.:
//   [{ category: "Rental Income", amount: 45000 }, ...]
// This function aggregates those into the summary fields we need.

function extractFromRows(rows: Record<string, unknown>[]): {
  totalIncome: number;
  rentalIncome: number;
  otherIncome: number;
  totalExpenses: number;
  operatingExpenses: number;
  noi: number;
} {
  let rentalIncome = 0;
  let otherIncome = 0;
  let totalExpenses = 0;
  let operatingExpenses = 0;

  for (const row of rows) {
    const cat = String(row.category ?? row.account ?? row.line_item ?? row.description ?? "").toLowerCase();
    const amt = toNum(row.amount ?? row.value ?? row.total ?? 0);

    if (cat.includes("rental") || cat.includes("rent income") || cat.includes("base rent")) {
      rentalIncome += amt;
    } else if (cat.includes("income") || cat.includes("revenue")) {
      otherIncome += amt;
    } else if (cat.includes("operating") && cat.includes("expense")) {
      operatingExpenses += amt;
    } else if (cat.includes("expense") || cat.includes("cost")) {
      totalExpenses += amt;
    }
  }

  // If operating_expenses not broken out, treat all expenses as operating
  if (operatingExpenses === 0) operatingExpenses = totalExpenses;
  // Ensure total_expenses >= operating_expenses
  if (totalExpenses < operatingExpenses) totalExpenses = operatingExpenses;

  const totalIncome = rentalIncome + otherIncome;
  const noi = totalIncome - totalExpenses;

  return { totalIncome, rentalIncome, otherIncome, totalExpenses, operatingExpenses, noi };
}

// ── Strategy ─────────────────────────────────────────────────────────────────

export const incomeStatementStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────

  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw     = ctx.bronze.raw_data;
    const today   = new Date().toISOString().slice(0, 10);
    // ctx.reportDate is the pre-computed YYYY-MM-DD string from the transform worker
    // (derived from bronze.report_date before the strategy is called).
    // Fall back to raw payload fields, then today as last resort.
    const reportDate =
      ctx.reportDate ||
      toDateStr(
        ctx.bronze.report_date ?? raw.report_date ?? raw.period_end ?? raw.as_of_date,
        today
      );

    // Support two payload shapes:
    //   1. Flat summary: { total_income, rental_income, ... }
    //   2. Rows array:   [{ category, amount }, ...]
    let totalIncome: number;
    let rentalIncome: number;
    let otherIncome: number;
    let totalExpenses: number;
    let operatingExpenses: number;
    let noi: number;

    const rows = Array.isArray(raw.rows) ? (raw.rows as Record<string, unknown>[]) : [];

    if (rows.length > 0) {
      // Shape 2: line-item rows
      const extracted = extractFromRows(rows);
      totalIncome       = extracted.totalIncome;
      rentalIncome      = extracted.rentalIncome;
      otherIncome       = extracted.otherIncome;
      totalExpenses     = extracted.totalExpenses;
      operatingExpenses = extracted.operatingExpenses;
      noi               = extracted.noi;
    } else {
      // Shape 1: flat summary object (may be nested under raw.summary or raw directly)
      const src = (raw.summary ?? raw) as Record<string, unknown>;
      rentalIncome      = toNum(src.rental_income      ?? src.rent_income     ?? src.gross_rent      ?? 0);
      otherIncome       = toNum(src.other_income       ?? src.misc_income     ?? src.other_revenue   ?? 0);
      totalIncome       = toNum(src.total_income       ?? src.gross_income    ?? src.total_revenue   ?? 0)
                          || (rentalIncome + otherIncome);
      operatingExpenses = toNum(src.operating_expenses ?? src.opex            ?? src.total_opex      ?? 0);
      totalExpenses     = toNum(src.total_expenses     ?? src.expenses        ?? src.total_costs     ?? 0)
                          || operatingExpenses;
      noi               = toNum(src.net_operating_income ?? src.noi           ?? src.net_income      ?? 0)
                          || (totalIncome - totalExpenses);
    }

    const profitMargin = deriveProfitMargin(noi, totalIncome);

    const normalized_data: Record<string, unknown> = {
      source:           "appfolio",
      report_type:      ctx.bronze.report_type,
      report_date:      reportDate,
      bronze_report_id: ctx.bronze.id,
      transformed_at:   new Date().toISOString(),
      // Income statement is a single summary — no row array
      summary: {
        report_date:          reportDate,
        total_income:         totalIncome,
        rental_income:        rentalIncome,
        other_income:         otherIncome,
        total_expenses:       totalExpenses,
        operating_expenses:   operatingExpenses,
        net_operating_income: noi,
        profit_margin:        profitMargin,
      },
    };

    return { normalized_data };
  },

  // ── Gold promotion ────────────────────────────────────────────────────────

  async promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    const { sql, bronze, silver } = ctx;
    const nd = silver.normalized_data as any;
    const s  = nd.summary as Record<string, unknown>;

    if (!s) {
      return {
        gold_ids:    [],
        skipped:     true,
        skip_reason: `Silver record ${silver.id} has no summary in normalized_data`,
      };
    }

    const reportDate      = String(s.report_date ?? nd.report_date ?? new Date().toISOString().slice(0, 10));
    const totalIncome     = toNum(s.total_income);
    const rentalIncome    = toNum(s.rental_income);
    const otherIncome     = toNum(s.other_income);
    const totalExpenses   = toNum(s.total_expenses);
    const opex            = toNum(s.operating_expenses);
    const noi             = toNum(s.net_operating_income);
    const profitMargin    = typeof s.profit_margin === "number" ? s.profit_margin
                          : deriveProfitMargin(noi, totalIncome);

    const goldRows = await sql<GoldIncomeStatement[]>`
      INSERT INTO gold_income_statements
        (bronze_report_id, report_date, total_income, rental_income, other_income,
         total_expenses, operating_expenses, net_operating_income, profit_margin, created_at)
      VALUES (
        ${bronze.id},
        ${reportDate}::date,
        ${totalIncome},
        ${rentalIncome},
        ${otherIncome},
        ${totalExpenses},
        ${opex},
        ${noi},
        ${profitMargin},
        NOW()
      )
      ON CONFLICT (bronze_report_id) DO NOTHING
      RETURNING *
    `;

    return {
      gold_ids: goldRows.length > 0 ? [goldRows[0].id] : [],
      skipped:  false,
    };
  },
};
