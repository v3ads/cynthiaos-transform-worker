// ── delinquency strategy ──────────────────────────────────────────────────────
//
// Handles the AppFolio "Delinquency" report.
// Normalises delinquency amounts and derives risk levels.
//
// Silver: normalises each row into a compact delinquency record with snake_case
//   fields, parsed amounts, and calculated risk level.
//
// Gold:   upserts each delinquency record into gold_delinquency_records.
//         Deduplicates by unit_id to handle tenant name variants.
import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";
import { normalizeTenantId, normalizeUnitId } from "../utils/normalize";

// ── Helpers ───────────────────────────────────────────────────────────────────

function toNum(v: unknown): number {
  if (v == null || v === "" || v === "--" || v === "None") return 0;
  if (typeof v === "number") return v;
  const n = parseFloat(String(v).replace(/[^0-9.-]/g, ""));
  return isNaN(n) ? 0 : n;
}

function deriveRiskLevel(totalOutstanding: number): "low" | "medium" | "high" | "critical" {
  if (totalOutstanding <= 0) return "low";
  if (totalOutstanding < 500) return "low";
  if (totalOutstanding < 2000) return "medium";
  if (totalOutstanding < 5000) return "high";
  return "critical";
}

// ── Strategy ──────────────────────────────────────────────────────────────────

export const delinquencyStrategy: TransformStrategy = {

  // ── Silver normalisation ────────────────────────────────────────────────────
  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data;
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];

    const normalizedRows = rows.map((r) => {
      const balanceDue = toNum(r.BalanceDue ?? r.balance_due);
      const totalOutstanding = toNum(r.TotalOutstanding ?? r.total_outstanding);
      const tenantName = String(r.TenantName ?? r.tenant_name ?? "");
      const unitName = String(r.UnitName ?? r.unit_name ?? "");

      return {
        tenant_id: normalizeTenantId(tenantName),
        unit_id: normalizeUnitId(unitName),
        balance_due: balanceDue,
        total_outstanding: totalOutstanding,
        days_overdue: Math.max(0, parseInt(String(r.DaysOverdue ?? r.days_overdue ?? 0), 10)),
        risk_level: deriveRiskLevel(totalOutstanding),
        tenant_status: String(r.TenantStatus ?? r.tenant_status ?? "current").toLowerCase(),
      };
    });

    return { normalized_data: { rows: normalizedRows } };
  },

  // ── Gold promotion ──────────────────────────────────────────────────────────
  async promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    const { sql, bronze, silver } = ctx;
    const nd = silver.normalized_data as any;
    const rows: Record<string, unknown>[] = Array.isArray(nd.rows) ? nd.rows : [];

    if (rows.length === 0) {
      return {
        gold_ids: [],
        skipped: true,
        skip_reason: `Silver record ${silver.id} has no rows in normalized_data`,
      };
    }

    const goldIds: string[] = [];

    for (const row of rows) {
      const tenantId = row.tenant_id as string;
      const unitId = row.unit_id as string;
      const balanceDue = row.balance_due as number;
      const totalOutstanding = row.total_outstanding as number;
      const daysOverdue = row.days_overdue as number;
      const riskLevel = row.risk_level as string;
      const tenantStatus = row.tenant_status as string;

      // UPSERT on unit_id — only one delinquency record per unit.
      // This handles tenant ID variants (e.g., "powell_wonson" vs "powellwonson")
      // by keeping the latest data for the unit.
      const goldRows = await sql<any[]>`
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
        ON CONFLICT (unit_id)
        DO UPDATE SET
          bronze_report_id  = EXCLUDED.bronze_report_id,
          tenant_id         = EXCLUDED.tenant_id,
          balance_due       = EXCLUDED.balance_due,
          total_outstanding = EXCLUDED.total_outstanding,
          days_overdue      = EXCLUDED.days_overdue,
          risk_level        = EXCLUDED.risk_level,
          tenant_status     = EXCLUDED.tenant_status,
          created_at        = NOW()
        RETURNING id
      `;

      if (goldRows.length > 0) {
        goldIds.push(goldRows[0].id);
      }
    }

    // PURGE GHOST/PAST RECORDS:
    // Delete delinquency records where tenant_status = 'past' IF there is 
    // a 'current' tenant record for the same unit.
    await sql`
      DELETE FROM gold_delinquency_records g1
      WHERE g1.tenant_status = 'past'
      AND EXISTS (
        SELECT 1 FROM gold_delinquency_records g2
        WHERE g2.unit_id = g1.unit_id
        AND g2.tenant_status = 'current'
      )
    `;

    // Also purge $0 records for past tenants
    await sql`
      DELETE FROM gold_delinquency_records
      WHERE tenant_status = 'past' AND total_outstanding <= 0
    `;

    return { gold_ids: goldIds, skipped: false };
  },
};
