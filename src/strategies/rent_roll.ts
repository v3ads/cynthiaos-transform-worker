// ── Strategy: rent_roll ───────────────────────────────────────────────────────
//
// Handles the AppFolio Rent Roll report type.
// Silver: normalises rows into { property_id, unit, tenant, rent, status }
// Gold:   promotes each row into gold_lease_expirations

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
  GoldLeaseExpiration,
} from "../types";
import { normalizeTenantId, normalizeUnitId } from "../utils/normalize";

export const rentRollStrategy: TransformStrategy = {
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

    const toNum = (v: unknown) => {
      if (typeof v === "number") return v;
      if (typeof v === "string") { const n = parseFloat(v.replace(/[^0-9.-]/g, "")); return isNaN(n) ? 0 : n; }
      return 0;
    };

    const normalized_data: Record<string, unknown> = {
      source: "appfolio",
      report_type: ctx.bronze.report_type,
      report_date: ctx.bronze.report_date,
      bronze_report_id: ctx.bronze.id,
      transformed_at: new Date().toISOString(),
      row_count: rows.length,
      rows: rows.map((r) => ({
        // AppFolio PascalCase fields; also support legacy snake_case
        property_id:     r.PropertyId    ?? r.property_id    ?? null,
        unit:            r.Unit          ?? r.unit           ?? null,
        tenant:          r.Tenant        ?? r.tenant         ?? null,
        rent:            r.Rent != null  ? toNum(r.Rent)     : typeof r.rent === "number" ? r.rent : null,
        status:          r.Status        ?? r.status         ?? null,
        lease_start_date: r.LeaseFrom    ?? r.MoveIn         ?? r.lease_start_date ?? r.lease_start ?? null,
        lease_end_date:   r.LeaseTo      ?? r.MoveOut        ?? r.lease_end_date   ?? r.lease_end   ?? null,
      })),
      summary: {
        total_units: summary.total_units ?? rows.length,
        total_rent:
          summary.total_rent ??
          rows.reduce((acc, r) => acc + toNum(r.Rent ?? r.rent ?? 0), 0),
        occupancy_rate: summary.occupancy_rate ?? null,
      },
    };

    return { normalized_data };
  },

  // ── Gold promotion ────────────────────────────────────────────────────────
  async promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    const { sql, bronze, silver, reportDate } = ctx;
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

    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);
    const goldIds: string[] = [];

    for (const row of rows) {
      const rawName  = String(row.tenant ?? row.tenant_id ?? row.name ?? row.resident ?? "");
      const rawUnit  = String(row.unit   ?? row.unit_id   ?? "");
      const tenantId = normalizeTenantId(rawName, rawUnit);
      const unitId   = normalizeUnitId(rawUnit);

      // Derive lease dates: prefer explicit fields, fall back to report_date
      const leaseStart: string | null =
        typeof row.lease_start_date === "string" ? row.lease_start_date
        : typeof row.lease_start === "string"    ? row.lease_start
        : reportDate ?? null;

      const leaseEnd: string | null =
        typeof row.lease_end_date === "string" ? row.lease_end_date
        : typeof row.lease_end === "string"    ? row.lease_end
        : (() => {
            if (!reportDate) return null;
            const d = new Date(reportDate);
            d.setFullYear(d.getFullYear() + 1);
            return d.toISOString().slice(0, 10);
          })();

      let daysUntilExpiration: number | null = null;
      if (leaseEnd) {
        const endDate = new Date(leaseEnd);
        endDate.setUTCHours(0, 0, 0, 0);
        daysUntilExpiration = Math.round(
          (endDate.getTime() - today.getTime()) / (1000 * 60 * 60 * 24)
        );
      }

      const goldRows = await sql<GoldLeaseExpiration[]>`
        INSERT INTO gold_lease_expirations
          (bronze_report_id, tenant_id, unit_id, lease_start_date, lease_end_date, days_until_expiration, created_at)
        VALUES (
          ${bronze.id},
          ${tenantId},
          ${unitId},
          ${leaseStart},
          ${leaseEnd},
          ${daysUntilExpiration},
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
