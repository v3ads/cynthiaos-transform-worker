// ── Strategy: delinquency ─────────────────────────────────────────────────────
//
// Handles the AppFolio Delinquency report type.
// Silver: normalises rows into { tenant_id, unit_id, balance_due, days_overdue,
//                                last_payment_date }
// Gold:   promotes each row into gold_delinquency_records with a derived risk_level

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";

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
    const rows = Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];
    const summary = (raw.summary ?? {}) as Record<string, unknown>;

    const normalizedRows = rows.map((r) => {
      const daysOverdue =
        typeof r.days_overdue === "number" ? r.days_overdue
        : typeof r.days_past_due === "number" ? r.days_past_due
        : null;

      const balanceDue =
        typeof r.balance_due === "number" ? r.balance_due
        : typeof r.amount_owed === "number" ? r.amount_owed
        : typeof r.balance === "number" ? r.balance
        : 0;

      const lastPaymentDate =
        typeof r.last_payment_date === "string" ? r.last_payment_date
        : typeof r.last_paid === "string" ? r.last_paid
        : null;

      return {
        tenant_id: String(r.tenant ?? r.tenant_id ?? r.tenant_name ?? "unknown"),
        unit_id:   String(r.unit   ?? r.unit_id   ?? r.unit_number  ?? "unknown"),
        balance_due: balanceDue,
        days_overdue: daysOverdue,
        last_payment_date: lastPaymentDate,
        risk_level: deriveRiskLevel(daysOverdue),
      };
    });

    const totalBalance = normalizedRows.reduce((acc, r) => acc + r.balance_due, 0);

    const normalized_data: Record<string, unknown> = {
      source: "appfolio",
      report_type: ctx.bronze.report_type,
      report_date: ctx.bronze.report_date,
      bronze_report_id: ctx.bronze.id,
      transformed_at: new Date().toISOString(),
      row_count: normalizedRows.length,
      rows: normalizedRows,
      summary: {
        total_delinquent_tenants: summary.total_delinquent_tenants ?? normalizedRows.length,
        total_balance_due: summary.total_balance_due ?? totalBalance,
        high_risk_count: normalizedRows.filter((r) => r.risk_level === "high").length,
        medium_risk_count: normalizedRows.filter((r) => r.risk_level === "medium").length,
        low_risk_count: normalizedRows.filter((r) => r.risk_level === "low").length,
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
        gold_ids: [],
        skipped: true,
        skip_reason: `Silver record ${silver.id} has no rows in normalized_data`,
      };
    }

    const goldIds: string[] = [];

    for (const row of rows) {
      const tenantId   = String(row.tenant_id ?? "unknown");
      const unitId     = String(row.unit_id   ?? "unknown");
      const balanceDue = typeof row.balance_due === "number" ? row.balance_due : 0;
      const daysOverdue = typeof row.days_overdue === "number" ? row.days_overdue : null;
      const riskLevel  = deriveRiskLevel(daysOverdue);

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
        ON CONFLICT (bronze_report_id, tenant_id, unit_id) DO NOTHING
        RETURNING *
      `;

      if (goldRows.length > 0) {
        goldIds.push(goldRows[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
