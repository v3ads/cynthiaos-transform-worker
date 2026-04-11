// ── Strategy: delinquency ─────────────────────────────────────────────────────
//
// Handles the AppFolio Delinquency report type.
// Silver: normalises rows into { tenant_id, unit_id, balance_due, days_overdue,
//                                last_payment_date, risk_level }
// Gold:   promotes each row into gold_delinquency_records with a derived risk_level
//
// FIX (2026-04-08): Gold promotion now reads `unit_id ?? unit` and
//   `balance_due ?? total_balance` to handle both the current Silver schema
//   (which stores `unit` + `total_balance`) and the new schema going forward.
//   Silver normalization updated to store `unit_id` + `balance_due` consistently.
//
// FIX (2026-04-11): days_overdue was incorrectly storing dollar amounts from
//   AppFolio's aging bucket fields (30Plus, 60Plus, 90Plus are DOLLAR amounts,
//   not day counts). Corrected to derive actual calendar days from which bucket
//   has a non-zero balance. Also added 5-day grace period logic: balances with
//   0To30 > 0 after the 5th of the month are treated as 6 days overdue.

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";
import { normalizeTenantId, normalizeUnitId } from "../utils/normalize";

// ── Gold row interface ────────────────────────────────────────────────────────

interface GoldDelinquencyRecord {
  id: string;
  bronze_report_id: string | null;
  tenant_id: string;
  unit_id: string;
  balance_due: string; // NUMERIC returns as string from postgres driver
  days_overdue: number | null;
  risk_level: string;
  created_at: Date;
}

// ── Numeric helper ────────────────────────────────────────────────────────────

function toNum(v: unknown): number {
  if (typeof v === "number") return v;
  if (typeof v === "string") {
    const n = parseFloat(v.replace(/[^0-9.-]/g, ""));
    return isNaN(n) ? 0 : n;
  }
  return 0;
}

// ── Days overdue derivation ───────────────────────────────────────────────────
//
// AppFolio delinquency report aging buckets are DOLLAR amounts, not day counts:
//   0To30   = balance aged 0–30 days (dollars)
//   30Plus  = balance aged 30+ days (dollars)
//   30To60  = balance aged 30–60 days (dollars)
//   60Plus  = balance aged 60+ days (dollars)
//   60To90  = balance aged 60–90 days (dollars)
//   90Plus  = balance aged 90+ days (dollars)
//
// We derive actual days overdue from which buckets are non-zero:
//   90Plus > 0  → 90 days overdue
//   60Plus > 0  → 60 days overdue
//   30Plus > 0  → 30 days overdue
//   0To30  > 0  → within 0–30 days; apply grace period logic below
//   all zero    → 0 (not yet due or current)
//
// Grace period: Cynthia Gardens allows until the 5th to pay rent.
// If today is after the 5th and 0To30 > 0 but 30Plus = 0, treat as 6 days overdue.

function deriveDaysOverdue(
  row: Record<string, unknown>,
  reportDate: string | null
): number {
  const ninetyPlus = toNum(row["90Plus"] ?? 0);
  const sixtyPlus  = toNum(row["60Plus"]  ?? 0);
  const thirtyPlus = toNum(row["30Plus"]  ?? 0);
  const zeroToThirty = toNum(row["0To30"] ?? 0);

  // Determine the reference date: use report_date if available, else today
  const refDate = reportDate ? new Date(reportDate) : new Date();
  const dayOfMonth = refDate.getDate();
  const GRACE_PERIOD_END = 5; // tenants have until the 5th

  if (ninetyPlus > 0) return 90;
  if (sixtyPlus > 0)  return 60;
  if (thirtyPlus > 0) return 30;
  if (zeroToThirty > 0) {
    // Within the 0–30 day bucket. Apply grace period:
    // If today is after the 5th, this balance is late (treat as 6 days overdue).
    // If on or before the 5th, it may still be within grace (treat as 0).
    return dayOfMonth > GRACE_PERIOD_END ? 6 : 0;
  }
  return 0;
}

// ── Risk level derivation ─────────────────────────────────────────────────────
//
// Based on actual days overdue (after grace period logic):
//   0          → low  (current or within grace period)
//   1–30       → low  (recently late, within first month)
//   31–90      → medium
//   91+        → high

function deriveRiskLevel(daysOverdue: number): "low" | "medium" | "high" {
  if (daysOverdue >= 91) return "high";
  if (daysOverdue >= 31) return "medium";
  return "low";
}

// ── Strategy ─────────────────────────────────────────────────────────────────

export const delinquencyStrategy: TransformStrategy = {
  // ── Silver normalisation ──────────────────────────────────────────────────
  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data;
    // Support both AppFolio native format (raw.results) and legacy format (raw.rows)
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];
    const summary = (raw.summary ?? {}) as Record<string, unknown>;

    const reportDate = ctx.bronze.report_date ?? null;

    const normalizedRows = rows.map((r) => {
      // AppFolio delinquency PascalCase fields:
      //   Name, Unit, AmountReceivable, 0To30, 30Plus, 30To60, 60Plus, 60To90, 90Plus, LastPayment

      // Derive actual calendar days overdue (NOT dollar amounts)
      const daysOverdue = deriveDaysOverdue(r, reportDate);

      // Balance: AppFolio uses AmountReceivable; also support legacy field names
      const balanceDue = toNum(
        r.AmountReceivable ?? r.balance_due ?? r.total_balance ?? r.amount_owed ?? r.balance ?? 0
      );

      const lastPaymentDate =
        typeof r.LastPayment === "string" && r.LastPayment ? r.LastPayment
        : typeof r.last_payment_date === "string" ? r.last_payment_date
        : typeof r.last_paid === "string" ? r.last_paid
        : null;

      // AppFolio delinquency: Name = "LastName, FirstName", Unit = unit number
      const rawName = String(r.Name ?? r.tenant ?? r.tenant_id ?? r.tenant_name ?? r.name ?? r.resident ?? "");
      const rawUnit = String(r.Unit ?? r.unit   ?? r.unit_id   ?? r.unit_number ?? "");

      return {
        tenant_id:         normalizeTenantId(rawName, rawUnit),
        unit_id:           normalizeUnitId(rawUnit),
        balance_due:       balanceDue,
        days_overdue:      daysOverdue,
        last_payment_date: lastPaymentDate,
        risk_level:        deriveRiskLevel(daysOverdue),
      };
    });

    const totalBalance = normalizedRows.reduce((acc, r) => acc + r.balance_due, 0);

    const normalized_data: Record<string, unknown> = {
      source:           "appfolio",
      report_type:      ctx.bronze.report_type,
      report_date:      ctx.bronze.report_date,
      bronze_report_id: ctx.bronze.id,
      transformed_at:   new Date().toISOString(),
      row_count:        normalizedRows.length,
      rows:             normalizedRows,
      summary: {
        total_delinquent_tenants: summary.total_delinquent_tenants ?? normalizedRows.length,
        total_balance_due:        summary.total_balance_due ?? totalBalance,
        high_risk_count:   normalizedRows.filter((r) => r.risk_level === "high").length,
        medium_risk_count: normalizedRows.filter((r) => r.risk_level === "medium").length,
        low_risk_count:    normalizedRows.filter((r) => r.risk_level === "low").length,
      },
    };

    return { normalized_data };
  },

  // ── Gold promotion ────────────────────────────────────────────────────────
  async promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    const { sql, bronze, silver } = ctx;
    const normalized_data = silver.normalized_data;

    const rows = Array.isArray((normalized_data as any).rows)
      ? ((normalized_data as any).rows as Record<string, unknown>[])
      : [];

    if (rows.length === 0) {
      return {
        gold_ids:    [],
        skipped:     true,
        skip_reason: `Silver record ${silver.id} has no rows in normalized_data`,
      };
    }

    const goldIds: string[] = [];

    for (const row of rows) {
      const tenantId = String(row.tenant_id ?? "unknown");

      // FIX: Silver may store unit as `unit_id` (new) or `unit` (legacy Silver records)
      const rawUnit  = String(row.unit_id ?? row.unit ?? "unknown");
      const unitId   = rawUnit === "unknown" ? "unknown" : normalizeUnitId(rawUnit);

      // FIX: Silver may store balance as `balance_due` (new) or `total_balance` (legacy)
      const balanceDue  = toNum(row.balance_due ?? row.total_balance ?? 0);

      // days_overdue is now a real calendar day count (0–365), never a dollar amount
      const daysOverdue = typeof row.days_overdue === "number" ? row.days_overdue : 0;
      const riskLevel   = deriveRiskLevel(daysOverdue);

      // Use UPSERT so re-running Gold promotion corrects existing bad records
      const goldRows = await sql<GoldDelinquencyRecord[]>`
        INSERT INTO gold_delinquency_records
          (bronze_report_id, tenant_id, unit_id, balance_due, days_overdue, risk_level, created_at)
        VALUES (
          ${bronze.id},
          ${tenantId},
          ${unitId},
          ${balanceDue},
          ${daysOverdue},
          ${riskLevel},
          NOW()
        )
        ON CONFLICT (bronze_report_id, tenant_id, unit_id)
        DO UPDATE SET
          balance_due   = EXCLUDED.balance_due,
          days_overdue  = EXCLUDED.days_overdue,
          risk_level    = EXCLUDED.risk_level
        RETURNING *
      `;

      if (goldRows.length > 0) {
        goldIds.push(goldRows[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
