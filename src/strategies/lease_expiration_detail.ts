// ── lease_expiration_detail strategy ─────────────────────────────────────────
//
// Silver: normalises AppFolio lease_expiration_detail report rows into:
//         { tenant_id, unit_id, lease_start_date, lease_end_date }
//
// Gold:   UPSERTS into gold_lease_expirations — the canonical lease expiry layer.
//         days_until_expiration is computed at promotion time from lease_end_date.
//         If a record already exists (matched on tenant_id) → update.
//
// Added 2026-04-26: Provides daily refresh of gold_lease_expirations so that
//   expiry countdowns stay accurate without manual pipeline triggers.

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";
import { normalizeTenantId, normalizeUnitId } from "../utils/normalize";

// ── Gold row interface ────────────────────────────────────────────────────────

interface GoldLeaseExpiration {
  id: string;
  bronze_report_id: string | null;
  tenant_id: string;
  unit_id: string;
  lease_start_date: Date | null;
  lease_end_date: Date | null;
  days_until_expiration: number | null;
  created_at: Date;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function toDateStr(val: unknown): string | null {
  if (!val) return null;
  const s = String(val).trim();
  if (!s || s === "null" || s === "undefined") return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) {
    const [m, d, y] = s.split("/");
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }
  return null;
}

function daysUntil(dateStr: string | null): number | null {
  if (!dateStr) return null;
  const now = new Date();
  const target = new Date(dateStr);
  if (isNaN(target.getTime())) return null;
  return Math.round((target.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
}

// ── Strategy ─────────────────────────────────────────────────────────────────

export const leaseExpirationDetailStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────

  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw  = ctx.bronze.raw_data;
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];

    const normalizedRows = rows.map((r) => {
      const tenantName = String(
        r.TenantName ?? r.Tenant ?? r.tenant_name ?? r.name ?? "unknown"
      ).trim();
      const unitId = String(
        r.Unit ?? r.unit ?? r.unit_id ?? r.unit_number ?? "unknown"
      ).trim();

      return {
        tenant_id:        normalizeTenantId(tenantName, unitId),
        unit_id:          normalizeUnitId(unitId),
        lease_start_date: toDateStr(r.MoveIn ?? r.LeaseSignDate ?? r.RenewalStartDate ?? r.lease_start_date),
        lease_end_date:   toDateStr(r.LeaseExpires ?? r.MoveOut ?? r.lease_end_date),
      };
    });

    const normalized_data: Record<string, unknown> = {
      source:           "appfolio",
      report_type:      ctx.bronze.report_type,
      report_date:      ctx.bronze.report_date,
      bronze_report_id: ctx.bronze.id,
      transformed_at:   new Date().toISOString(),
      row_count:        normalizedRows.length,
      rows:             normalizedRows,
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
      const tenantId   = String(row.tenant_id ?? "unknown");
      const unitId     = String(row.unit_id   ?? "unknown");
      const leaseStart = typeof row.lease_start_date === "string" ? row.lease_start_date : null;
      const leaseEnd   = typeof row.lease_end_date   === "string" ? row.lease_end_date   : null;
      const days       = daysUntil(leaseEnd);

      const goldRows = await sql<GoldLeaseExpiration[]>`
        INSERT INTO gold_lease_expirations
          (bronze_report_id, tenant_id, unit_id,
           lease_start_date, lease_end_date, days_until_expiration,
           created_at)
        VALUES (
          ${bronze.id},
          ${tenantId},
          ${unitId},
          ${leaseStart}::date,
          ${leaseEnd}::date,
          ${days},
          NOW()
        )
        ON CONFLICT (unit_id) DO UPDATE SET
          bronze_report_id      = EXCLUDED.bronze_report_id,
          tenant_id             = EXCLUDED.tenant_id,
          lease_start_date      = COALESCE(EXCLUDED.lease_start_date, gold_lease_expirations.lease_start_date),
          lease_end_date        = COALESCE(EXCLUDED.lease_end_date,   gold_lease_expirations.lease_end_date),
          days_until_expiration = EXCLUDED.days_until_expiration,
          created_at            = NOW()
        RETURNING *
      `;

      if (goldRows.length > 0) {
        goldIds.push(goldRows[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
