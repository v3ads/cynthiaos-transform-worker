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
// AppFolio exports income statements as a list of account line items:
//   [{ AccountName: "Rent", AccountNumber: "4530", MonthToDate: "238,147.00",
//      YearToDate: "1,015,600.32", ... }, ...]
//
// Income accounts: AccountNumber 4xxx (positive = income)
// Expense accounts: AccountNumber 6xxx (positive = expense)
// Summary rows: AccountName starts with "Total" (no AccountNumber)
//
// FIX (2026-04-08): Added AppFolio PascalCase field support.
// FIX (2026-04-11): Use YearToDate as the primary amount (YTD gross revenue).

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
  let totalIncomeFromSummary = 0;
  let totalExpenseFromSummary = 0;
  let hasSummaryRows = false;

  for (const row of rows) {
    // AppFolio PascalCase; also support legacy snake_case
    const accountName   = String(row.AccountName   ?? row.category ?? row.account ?? row.line_item ?? row.description ?? "");
    const accountNumber = String(row.AccountNumber ?? row.account_number ?? "");
    // Use YearToDate as the primary amount (YTD gross revenue); fall back to MonthToDate then legacy field names
    const amt = toNum(
      row.YearToDate ?? row.year_to_date ?? row.MonthToDate ?? row.month_to_date ?? row.amount ?? row.value ?? row.total ?? 0
    );

    const nameLower = accountName.toLowerCase();

    // Summary rows (no account number, name starts with "Total")
    if (!accountNumber && nameLower.startsWith("total income")) {
      totalIncomeFromSummary = amt;
      hasSummaryRows = true;
      continue;
    }
    if (!accountNumber && nameLower.startsWith("total expense")) {
      totalExpenseFromSummary = Math.abs(amt);
      hasSummaryRows = true;
      continue;
    }
    if (!accountNumber) continue; // skip other summary rows

    const acctNum = parseInt(accountNumber, 10);

    // Income accounts: 4000–4999 (AppFolio convention)
    if (acctNum >= 4000 && acctNum < 5000) {
      const absAmt = Math.abs(amt); // some income rows are negative (concessions, prepaid)
      // Account 4530 = Rent (primary rental income)
      if (acctNum === 4530 || nameLower.includes("rent") && !nameLower.includes("expense")) {
        rentalIncome += absAmt;
      } else {
        otherIncome += absAmt;
      }
    }
    // Expense accounts: 6000–6999 (AppFolio convention)
    else if (acctNum >= 6000 && acctNum < 7000) {
      totalExpenses += Math.abs(amt);
    }
  }

  // Prefer the "Total Income" / "Total Expense" summary rows if present
  const finalTotalIncome   = hasSummaryRows && totalIncomeFromSummary > 0
    ? totalIncomeFromSummary
    : rentalIncome + otherIncome;
  const finalTotalExpenses = hasSummaryRows && totalExpenseFromSummary > 0
    ? totalExpenseFromSummary
    : totalExpenses;

  // If we used summary rows, back-calculate rental vs other income split
  if (hasSummaryRows && totalIncomeFromSummary > 0 && rentalIncome === 0) {
    rentalIncome = finalTotalIncome;
  }

  const operatingExpenses = finalTotalExpenses; // all AppFolio expenses are operating
  const noi = finalTotalIncome - finalTotalExpenses;

  return {
    totalIncome:       finalTotalIncome,
    rentalIncome,
    otherIncome,
    totalExpenses:     finalTotalExpenses,
    operatingExpenses,
    noi,
  };
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

    // Support both AppFolio native format (raw.results) and legacy format (raw.rows)
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];

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

    // Compute a stable content hash so that re-uploading identical data for the
    // same period does not create duplicate Gold rows, regardless of bronze_report_id.
    const contentHash = require("crypto")
      .createHash("md5")
      .update(`${reportDate}|${totalIncome}|${totalExpenses}|${noi}`)
      .digest("hex");

    const goldRows = await sql<GoldIncomeStatement[]>`
      INSERT INTO gold_income_statements
        (bronze_report_id, report_date, total_income, rental_income, other_income,
         total_expenses, operating_expenses, net_operating_income, profit_margin,
         content_hash, created_at)
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
        ${contentHash},
        NOW()
      )
      ON CONFLICT (report_date, content_hash)
      DO UPDATE SET
        bronze_report_id      = EXCLUDED.bronze_report_id,
        total_income          = EXCLUDED.total_income,
        rental_income         = EXCLUDED.rental_income,
        other_income          = EXCLUDED.other_income,
        total_expenses        = EXCLUDED.total_expenses,
        operating_expenses    = EXCLUDED.operating_expenses,
        net_operating_income  = EXCLUDED.net_operating_income,
        profit_margin         = EXCLUDED.profit_margin
      RETURNING *
    `;

    return {
      gold_ids: goldRows.length > 0 ? [goldRows[0].id] : [],
      skipped:  false,
    };
  },
};
