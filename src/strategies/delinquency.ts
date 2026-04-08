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

// ── Risk level derivation ─────────────────────────────────────────────────────

function deriveRiskLevel(daysOverdue: number | null): "low" | "medium" | "high" {
  if (daysOverdue === null || daysOverdue === undefined) return "low";
  if (daysOverdue > 30) return "high";
  if (daysOverdue >= 15) return "medium";
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

    const normalizedRows = rows.map((r) => {
      // AppFolio delinquency PascalCase fields:
      //   Name, Unit, AmountReceivable, 30Plus, 60Plus, 90Plus, LastPayment
      const thirtyPlus  = toNum(r["30Plus"]  ?? r.days_overdue ?? r.days_past_due ?? 0);
      const sixtyPlus   = toNum(r["60Plus"]  ?? 0);
      const ninetyPlus  = toNum(r["90Plus"]  ?? 0);

      // Derive days_overdue from aging buckets
      const daysOverdue: number | null =
        ninetyPlus > 0 ? 90
        : sixtyPlus > 0 ? 60
        : thirtyPlus > 0 ? 30
        : typeof r.days_overdue === "number" ? r.days_overdue
        : typeof r.days_past_due === "number" ? r.days_past_due
        : null;

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
      const daysOverdue = typeof row.days_overdue === "number" ? row.days_overdue : null;
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
