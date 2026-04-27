// ── Strategy: delinquency ─────────────────────────────────────────────────────
//
// Handles the AppFolio Delinquency report type.
// Silver: normalises rows into { tenant_id, unit_id, balance_due, total_outstanding,
//                                days_overdue, last_payment_date, risk_level, tenant_status }
// Gold:   promotes each row into gold_delinquency_records with a derived risk_level
//
// FIX (2026-04-22): CRITICAL DATA DEFINITION CORRECTION
//   balance_due was incorrectly set to AmountReceivable (total outstanding balance),
//   which includes current-month charges not yet overdue and past-tenant carry-overs.
//   Corrected to use 30Plus (balance aged 30+ days) as the true "overdue" amount.
//   Added total_outstanding field to store AmountReceivable for reference.
//
//   Data definitions:
//     balance_due       = 30Plus (dollars overdue 30+ days — the actionable collection amount)
//     total_outstanding = AmountReceivable (total open balance including current charges)
//
//   Why this matters:
//     - AmountReceivable includes this month's rent (not yet overdue) for current tenants
//     - AmountReceivable includes carry-over balances from past tenants (moved out)
//     - 30Plus is the correct metric for "how much is actually overdue and collectible"
//
// FIX (2026-04-20): Added tenant_status field sourced from AppFolio's TenantStatus
//   field ('Current' | 'Past'). Past-tenant balances are carry-overs from prior lease
//   terms and must NOT inflate the current tenant's risk score in unit-intelligence.
//   The unit-intelligence CTE splits delinquency_balance into current_delinquency_balance
//   (tenant_status = 'current') and prior_term_balance (tenant_status = 'past').
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
  balance_due: string;        // NUMERIC — 30Plus (truly overdue 30+ days)
  total_outstanding: string;  // NUMERIC — AmountReceivable (total open balance)
  days_overdue: number | null;
  risk_level: string;
  tenant_status: string; // 'current' | 'past' — sourced from AppFolio TenantStatus
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
//   31–89      → medium
//   90+        → high  (the 90Plus AppFolio bucket maps to exactly 90 days;
//                       using >= 90 ensures these tenants are correctly flagged)

function deriveRiskLevel(daysOverdue: number): "low" | "medium" | "high" {
  if (daysOverdue >= 90) return "high";
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

      // balance_due = 30Plus (truly overdue, 30+ days aged)
      // This is the actionable collection amount — what is actually past due.
      const balanceDue = toNum(r["30Plus"] ?? r.balance_due ?? 0);

      // total_outstanding = AmountReceivable (total open balance including current charges)
      // This includes current-month rent not yet overdue and all aging buckets.
      const totalOutstanding = toNum(
        r.AmountReceivable ?? r.total_outstanding ?? r.balance ?? 0
      );

      const lastPaymentDate =
        typeof r.LastPayment === "string" && r.LastPayment ? r.LastPayment
        : typeof r.last_payment_date === "string" ? r.last_payment_date
        : typeof r.last_paid === "string" ? r.last_paid
        : null;

      // AppFolio delinquency: Name = "LastName, FirstName", Unit = unit number
      const rawName = String(r.Name ?? r.tenant ?? r.tenant_id ?? r.tenant_name ?? r.name ?? r.resident ?? "");
      const rawUnit = String(r.Unit ?? r.unit   ?? r.unit_id   ?? r.unit_number ?? "");

      // AppFolio TenantStatus: 'Current' | 'Past' | 'Future'
      // Normalise to lowercase; default to 'current' if missing
      const rawTenantStatus = String(r.TenantStatus ?? r.tenant_status ?? 'Current').toLowerCase();
      const tenantStatus = rawTenantStatus === 'past' ? 'past' : 'current';

      return {
        tenant_id:         normalizeTenantId(rawName, rawUnit),
        unit_id:           normalizeUnitId(rawUnit),
        balance_due:       balanceDue,
        total_outstanding: totalOutstanding,
        days_overdue:      daysOverdue,
        last_payment_date: lastPaymentDate,
        risk_level:        deriveRiskLevel(daysOverdue),
        tenant_status:     tenantStatus,
      };
    });

    const totalBalanceDue = normalizedRows.reduce((acc, r) => acc + r.balance_due, 0);
    const totalOutstanding = normalizedRows.reduce((acc, r) => acc + r.total_outstanding, 0);

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
        total_balance_due:        summary.total_balance_due ?? totalBalanceDue,
        total_outstanding:        totalOutstanding,
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

      // balance_due = 30Plus (truly overdue 30+ days)
      // Legacy Silver records stored AmountReceivable here — correct on re-promotion.
      const balanceDue = toNum(row.balance_due ?? 0);

      // total_outstanding = AmountReceivable (total open balance)
      // May be absent in legacy Silver records — default to balanceDue as fallback.
      const totalOutstanding = toNum(row.total_outstanding ?? row.balance_due ?? 0);

      // days_overdue is now a real calendar day count (0–365), never a dollar amount
      const daysOverdue = typeof row.days_overdue === "number" ? row.days_overdue : 0;
      const riskLevel   = deriveRiskLevel(daysOverdue);

      // tenant_status: 'current' | 'past' — from AppFolio TenantStatus field.
      // IMPORTANT: AppFolio sometimes marks transferred tenants as 'past' even when
      // they have an active lease in a new unit. Cross-check gold_tenants: if the
      // tenant has lease_status = 'active', override to 'current' regardless of
      // what AppFolio reports. This prevents unit-transfer tenants from being
      // misclassified as past tenants in the collections-risk panel.
      let tenantStatus = String((row as any).tenant_status ?? 'current');
      if (tenantStatus === 'past') {
        const activeCheck = await sql<{ count: string }[]>`
          SELECT COUNT(*) AS count FROM gold_tenants
          WHERE tenant_id = ${tenantId} AND lease_status = 'active'
        `;
        if (parseInt(activeCheck[0]?.count ?? '0', 10) > 0) {
          tenantStatus = 'current';
        }
      }

      // UPSERT on (tenant_id, unit_id) — one delinquency record per tenant+unit.
      const goldRows = await sql<GoldDelinquencyRecord[]>`
        INSERT INTO gold_delinquency_records
          (bronze_report_id, tenant_id, unit_id, balance_due, total_outstanding,
           days_overdue, risk_level, tenant_status, created_at)
        VALUES (
          ${bronze.id},
          ${tenantId},
          ${unitId},
          ${balanceDue},
          ${totalOutstanding},
          ${daysOverdue},
          ${riskLevel},
          ${tenantStatus},
          NOW()
        )
        ON CONFLICT (tenant_id, unit_id)
        DO UPDATE SET
          bronze_report_id  = EXCLUDED.bronze_report_id,
          balance_due       = EXCLUDED.balance_due,
          total_outstanding = EXCLUDED.total_outstanding,
          days_overdue      = EXCLUDED.days_overdue,
          risk_level        = EXCLUDED.risk_level,
          tenant_status     = EXCLUDED.tenant_status
        RETURNING *
      `;

      if (goldRows.length > 0) {
        goldIds.push(goldRows[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
