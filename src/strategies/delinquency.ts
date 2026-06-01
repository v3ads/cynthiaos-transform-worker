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

function firstNonEmpty(...values: unknown[]): string {
  for (const value of values) {
    if (value == null) continue;
    const text = String(value).trim();
    if (text !== "") return text;
  }
  return "";
}

function deriveRiskLevel(totalOutstanding: number): "low" | "medium" | "high" | "critical" {
  if (totalOutstanding <= 0) return "low";
  if (totalOutstanding < 500) return "low";
  if (totalOutstanding < 2000) return "medium";
  if (totalOutstanding < 5000) return "high";
  return "critical";
}

function deriveDaysOverdue(r: Record<string, unknown>): number {
  const explicit = parseInt(String(r.DaysOverdue ?? r.days_overdue ?? ""), 10);
  if (!Number.isNaN(explicit)) return Math.max(0, explicit);

  // AppFolio Delinquency rows provide aging buckets rather than a single days-overdue field.
  if (toNum(r["90Plus"] ?? r["90_plus"] ?? r.over_90) > 0) return 90;
  if (toNum(r["60Plus"] ?? r["60_plus"]) > 0 || toNum(r["60To90"] ?? r["61_90"] ?? r.days_61_90) > 0) return 60;
  if (toNum(r["30Plus"] ?? r["30_plus"]) > 0 || toNum(r["30To60"] ?? r["31_60"] ?? r.days_31_60) > 0) return 30;
  if (toNum(r["0To30"] ?? r["0_30"] ?? r.current) > 0) return 1;
  return 0;
}

function normalizeTenantStatus(value: unknown): "current" | "past" {
  const status = String(value ?? "Current").trim().toLowerCase();
  return status === "past" ? "past" : "current";
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
      // Live AppFolio delinquency uses Name/Unit/DelinquentRent/AmountReceivable.
      // Keep legacy aliases so older Bronze payloads can still be reprocessed safely.
      const tenantName = firstNonEmpty(
        r.TenantName,
        r.tenant_name,
        r.tenant,
        r.Name,
        r.name,
        r.PayerName,
        r.resident
      );
      const unitName = firstNonEmpty(
        r.UnitName,
        r.unit_name,
        r.unit,
        r.Unit,
        r.unit_id,
        r.unit_number
      );
      const balanceDue = toNum(
        r.BalanceDue ??
        r.balance_due ??
        r.DelinquentRent ??
        r.delinquent_rent ??
        r.AmountReceivable ??
        r.amount_receivable
      );
      const totalOutstanding = toNum(
        r.TotalOutstanding ??
        r.total_outstanding ??
        r.AmountReceivable ??
        r.amount_receivable ??
        r.DelinquentRent ??
        r.delinquent_rent
      );

      return {
        tenant_id: normalizeTenantId(tenantName),
        unit_id: normalizeUnitId(unitName),
        balance_due: balanceDue,
        total_outstanding: totalOutstanding,
        days_overdue: deriveDaysOverdue(r),
        risk_level: deriveRiskLevel(totalOutstanding),
        tenant_status: normalizeTenantStatus(r.TenantStatus ?? r.tenant_status),
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
    let skippedSentinelRows = 0;

    for (const row of rows) {
      const tenantId = String(row.tenant_id ?? "unknown");
      const unitId = String(row.unit_id ?? row.unit ?? "unknown");
      const balanceDue = toNum(row.balance_due);
      const totalOutstanding = toNum(row.total_outstanding);
      const daysOverdue = Math.max(0, parseInt(String(row.days_overdue ?? 0), 10) || 0);
      const riskLevel = String(row.risk_level ?? deriveRiskLevel(totalOutstanding));
      const tenantStatus = normalizeTenantStatus(row.tenant_status);

      // Never write sentinel identity rows into Gold; they break joins and integrity checks.
      if (tenantId === "unknown" || unitId === "unknown") {
        skippedSentinelRows += 1;
        continue;
      }

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

    // Remove stale sentinel rows left by prior bad mappings.
    await sql`
      DELETE FROM gold_delinquency_records
      WHERE tenant_id = 'unknown' OR unit_id = 'unknown'
    `;

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

    return {
      gold_ids: goldIds,
      skipped: false,
      ...(skippedSentinelRows > 0 ? { warnings: [`Skipped ${skippedSentinelRows} delinquency row(s) with unresolved tenant_id or unit_id`] } : {}),
    } as GoldPromoteResult;
  },
};
