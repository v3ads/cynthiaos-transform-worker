// ── income_statement strategy ─────────────────────────────────────────────────
//
// Silver: normalises an AppFolio income statement report into a single summary
//         row per report with BOTH YearToDate and MonthToDate figures:
//         { report_date,
//           total_income_ytd, rental_income_ytd, other_income_ytd,
//           total_expenses_ytd, operating_expenses_ytd, noi_ytd,
//           total_income_mtd, rental_income_mtd, other_income_mtd,
//           total_expenses_mtd, operating_expenses_mtd, noi_mtd }
//
// Gold:   promotes one row per report into gold_income_statements.
//         Stores both YTD and MTD columns.
//         Derives profit_margin from YTD figures.
//         Idempotent via UNIQUE constraint on (report_date, content_hash).
//
// Note:   AppFolio income statements are property-level summaries, not
//         per-tenant rows. The payload arrives as a flat list of account rows
//         where each row has both MonthToDate and YearToDate columns.

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
  // YTD
  total_income: string;
  rental_income: string;
  other_income: string;
  total_expenses: string;
  operating_expenses: string;
  net_operating_income: string;
  profit_margin: string | null;
  // MTD
  total_income_mtd: string;
  rental_income_mtd: string;
  other_income_mtd: string;
  total_expenses_mtd: string;
  operating_expenses_mtd: string;
  net_operating_income_mtd: string;
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

// ── Profit margin derivation ──────────────────────────────────────────────────

function deriveProfitMargin(noi: number, totalIncome: number): number | null {
  if (totalIncome === 0) return null;
  return noi / totalIncome;
}

// ── Extract summary from rows array (AppFolio line-item format) ───────────────
//
// AppFolio exports income statements as a flat list of account line items:
//   [{ AccountName: "Rent", AccountNumber: "4530",
//      MonthToDate: "247,847.00", YearToDate: "1,025,300.32", ... }, ...]
//
// Summary rows have no AccountNumber:
//   { AccountName: "Total Income", AccountNumber: null,
//     MonthToDate: "145,536.50", YearToDate: "918,254.74" }
//
// This function extracts BOTH YTD and MTD figures in a single pass.

interface IncomeExtract {
  ytd: { totalIncome: number; rentalIncome: number; otherIncome: number; totalExpenses: number; operatingExpenses: number; noi: number; };
  mtd: { totalIncome: number; rentalIncome: number; otherIncome: number; totalExpenses: number; operatingExpenses: number; noi: number; };
}

function extractFromRows(rows: Record<string, unknown>[]): IncomeExtract {
  // Accumulators for YTD
  let ytdRental = 0, ytdOther = 0, ytdExpenses = 0;
  let ytdTotalFromSummary = 0, ytdExpenseFromSummary = 0, ytdHasSummary = false;

  // Accumulators for MTD
  let mtdRental = 0, mtdOther = 0, mtdExpenses = 0;
  let mtdTotalFromSummary = 0, mtdExpenseFromSummary = 0, mtdHasSummary = false;

  const parseAmt = (val: unknown): number =>
    toNum(String(val ?? "0").replace(/,/g, ""));

  for (const row of rows) {
    const accountName   = String(row.AccountName   ?? row.category ?? row.account ?? row.description ?? "").trim();
    const accountNumber = String(row.AccountNumber ?? row.account_number ?? "").trim();
    const nameLower     = accountName.toLowerCase();

    const ytd = parseAmt(row.YearToDate  ?? row.year_to_date  ?? row.amount ?? 0);
    const mtd = parseAmt(row.MonthToDate ?? row.month_to_date ?? row.amount ?? 0);

    // ── Summary rows (no AccountNumber) ──────────────────────────────────────
    if (!accountNumber || accountNumber === "null") {
      if (nameLower === "total income" || nameLower === "total revenue") {
        ytdTotalFromSummary = ytd;
        mtdTotalFromSummary = mtd;
        ytdHasSummary = mtdHasSummary = true;
      } else if (nameLower === "total expense" || nameLower === "total expenses") {
        ytdExpenseFromSummary = Math.abs(ytd);
        mtdExpenseFromSummary = Math.abs(mtd);
        ytdHasSummary = mtdHasSummary = true;
      }
      continue;
    }

    // ── Individual account rows ───────────────────────────────────────────────
    const acctInt = parseInt(accountNumber, 10);
    if (isNaN(acctInt)) continue;

    if (acctInt >= 4000 && acctInt < 5000) {
      // Income accounts (4xxx)
      if (acctInt === 4530 || (nameLower.includes("rent") && !nameLower.includes("expense"))) {
        // Primary Rent account
        ytdRental += ytd;
        mtdRental += mtd;
      } else {
        // Other income (fees, laundry, misc, etc.)
        ytdOther += ytd;
        mtdOther += mtd;
      }
    } else if (acctInt >= 6000 && acctInt < 7000) {
      // Expense accounts (6xxx)
      ytdExpenses += Math.abs(ytd);
      mtdExpenses += Math.abs(mtd);
    }
  }

  // IMPORTANT: Do NOT use AppFolio's "Total Income" summary row for total_income.
  // AppFolio includes Prepaid Rent (account 2300, a liability) in its summary
  // "Total Income" figure, inflating it by ~$85K. Always derive total_income
  // by summing 4xxx account rows only (rental_income + other_income).
  // Use AppFolio's "Total Expenses" summary row for expenses — it is accurate.
  const ytdTotalIncome   = ytdRental + ytdOther;
  const ytdTotalExpenses = ytdHasSummary && ytdExpenseFromSummary !== 0 ? ytdExpenseFromSummary : ytdExpenses;
  const mtdTotalIncome   = mtdRental + mtdOther;
  const mtdTotalExpenses = mtdHasSummary && mtdExpenseFromSummary !== 0 ? mtdExpenseFromSummary : mtdExpenses;

  return {
    ytd: {
      totalIncome:       ytdTotalIncome,
      rentalIncome:      ytdRental,
      otherIncome:       ytdOther,
      totalExpenses:     ytdTotalExpenses,
      operatingExpenses: ytdTotalExpenses,
      noi:               ytdTotalIncome - ytdTotalExpenses,
    },
    mtd: {
      totalIncome:       mtdTotalIncome,
      rentalIncome:      mtdRental,
      otherIncome:       mtdOther,
      totalExpenses:     mtdTotalExpenses,
      operatingExpenses: mtdTotalExpenses,
      noi:               mtdTotalIncome - mtdTotalExpenses,
    },
  };
}

// ── Strategy ─────────────────────────────────────────────────────────────────

export const incomeStatementStrategy: TransformStrategy = {

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

    // Support both AppFolio native format (raw.results) and legacy format (raw.rows)
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];

    let ytd: IncomeExtract["ytd"];
    let mtd: IncomeExtract["mtd"];

    if (rows.length > 0) {
      const extracted = extractFromRows(rows);
      ytd = extracted.ytd;
      mtd = extracted.mtd;
    } else {
      // Flat summary object fallback (legacy shape)
      const src = (raw.summary ?? raw) as Record<string, unknown>;
      const ti  = toNum(src.total_income ?? src.gross_income ?? src.total_revenue ?? 0);
      const ri  = toNum(src.rental_income ?? src.rent_income ?? src.gross_rent ?? 0);
      const oi  = toNum(src.other_income ?? src.misc_income ?? src.other_revenue ?? 0);
      const te  = toNum(src.total_expenses ?? src.expenses ?? src.total_costs ?? 0);
      const oe  = toNum(src.operating_expenses ?? src.opex ?? te);
      const n   = toNum(src.net_operating_income ?? src.noi ?? src.net_income ?? 0) || (ti - te);
      ytd = { totalIncome: ti, rentalIncome: ri, otherIncome: oi, totalExpenses: te, operatingExpenses: oe, noi: n };
      mtd = { totalIncome: 0, rentalIncome: 0, otherIncome: 0, totalExpenses: 0, operatingExpenses: 0, noi: 0 };
    }

    const profitMarginYtd = deriveProfitMargin(ytd.noi, ytd.totalIncome);
    const profitMarginMtd = deriveProfitMargin(mtd.noi, mtd.totalIncome);

    const normalized_data: Record<string, unknown> = {
      source:           "appfolio",
      report_type:      ctx.bronze.report_type,
      report_date:      reportDate,
      bronze_report_id: ctx.bronze.id,
      transformed_at:   new Date().toISOString(),
      summary: {
        report_date: reportDate,
        // YTD
        total_income:         ytd.totalIncome,
        rental_income:        ytd.rentalIncome,
        other_income:         ytd.otherIncome,
        total_expenses:       ytd.totalExpenses,
        operating_expenses:   ytd.operatingExpenses,
        net_operating_income: ytd.noi,
        profit_margin:        profitMarginYtd,
        // MTD
        total_income_mtd:         mtd.totalIncome,
        rental_income_mtd:        mtd.rentalIncome,
        other_income_mtd:         mtd.otherIncome,
        total_expenses_mtd:       mtd.totalExpenses,
        operating_expenses_mtd:   mtd.operatingExpenses,
        net_operating_income_mtd: mtd.noi,
        profit_margin_mtd:        profitMarginMtd,
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

    const reportDate  = String(s.report_date ?? nd.report_date ?? new Date().toISOString().slice(0, 10));

    // YTD figures
    const totalIncome     = toNum(s.total_income);

    // Skip blank records — a zero total_income means the Bronze report had no data
    if (totalIncome === 0) {
      return {
        gold_ids:    [],
        skipped:     true,
        skip_reason: `Skipping income statement with zero total_income for ${reportDate} (blank report)`,
      };
    }
    const rentalIncome    = toNum(s.rental_income);
    const otherIncome     = toNum(s.other_income);
    const totalExpenses   = toNum(s.total_expenses);
    const opex            = toNum(s.operating_expenses);
    const noi             = toNum(s.net_operating_income);
    const profitMargin    = typeof s.profit_margin === "number" ? s.profit_margin
                          : deriveProfitMargin(noi, totalIncome);

    // MTD figures
    const totalIncomeMtd   = toNum(s.total_income_mtd);
    const rentalIncomeMtd  = toNum(s.rental_income_mtd);
    const otherIncomeMtd   = toNum(s.other_income_mtd);
    const totalExpensesMtd = toNum(s.total_expenses_mtd);
    const opexMtd          = toNum(s.operating_expenses_mtd);
    const noiMtd           = toNum(s.net_operating_income_mtd);

    // Content hash for idempotency (based on YTD figures which are the primary values)
    const contentHash = require("crypto")
      .createHash("md5")
      .update(`${reportDate}|${totalIncome}|${totalExpenses}|${noi}`)
      .digest("hex");

    const goldRows = await sql<GoldIncomeStatement[]>`
      INSERT INTO gold_income_statements
        (bronze_report_id, report_date,
         total_income, rental_income, other_income,
         total_expenses, operating_expenses, net_operating_income, profit_margin,
         total_income_mtd, rental_income_mtd, other_income_mtd,
         total_expenses_mtd, operating_expenses_mtd, net_operating_income_mtd,
         content_hash, created_at)
      VALUES (
        ${bronze.id},
        ${reportDate}::date,
        ${totalIncome}, ${rentalIncome}, ${otherIncome},
        ${totalExpenses}, ${opex}, ${noi}, ${profitMargin},
        ${totalIncomeMtd}, ${rentalIncomeMtd}, ${otherIncomeMtd},
        ${totalExpensesMtd}, ${opexMtd}, ${noiMtd},
        ${contentHash},
        NOW()
      )
      ON CONFLICT (report_date, content_hash)
      DO UPDATE SET
        bronze_report_id          = EXCLUDED.bronze_report_id,
        total_income              = EXCLUDED.total_income,
        rental_income             = EXCLUDED.rental_income,
        other_income              = EXCLUDED.other_income,
        total_expenses            = EXCLUDED.total_expenses,
        operating_expenses        = EXCLUDED.operating_expenses,
        net_operating_income      = EXCLUDED.net_operating_income,
        profit_margin             = EXCLUDED.profit_margin,
        total_income_mtd          = EXCLUDED.total_income_mtd,
        rental_income_mtd         = EXCLUDED.rental_income_mtd,
        other_income_mtd          = EXCLUDED.other_income_mtd,
        total_expenses_mtd        = EXCLUDED.total_expenses_mtd,
        operating_expenses_mtd    = EXCLUDED.operating_expenses_mtd,
        net_operating_income_mtd  = EXCLUDED.net_operating_income_mtd
      RETURNING *
    `;

    return {
      gold_ids: goldRows.length > 0 ? [goldRows[0].id] : [],
      skipped:  false,
    };
  },
};
