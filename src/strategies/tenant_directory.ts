// ── tenant_directory strategy ─────────────────────────────────────────────────
//
// Silver: normalises AppFolio tenant directory report rows into consistent shape:
//         { tenant_id, full_name, unit_id, email, phone,
//           lease_start_date, lease_end_date, lease_status }
//
// Gold:   UPSERTS into gold_tenants — the canonical identity layer.
//         If a tenant already exists (matched on normalised tenant_id) → update.
//         If not → insert.
//
// Identity rules:
//   tenant_id is derived from full_name + unit_id, trimmed, lowercased, and
//   de-noised (special chars → underscores) to produce a stable key.
//   This allows cross-referencing with delinquency and aged_receivables records
//   that currently store tenant names as free-text strings.

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";

// ── Gold row interface ────────────────────────────────────────────────────────

interface GoldTenant {
  id: string;
  bronze_report_id: string | null;
  tenant_id: string;
  full_name: string;
  unit_id: string;
  email: string | null;
  phone: string | null;
  lease_start_date: Date | null;
  lease_end_date: Date | null;
  lease_status: string | null;
  created_at: Date;
  updated_at: Date;
}

// ── Identity normalisation ────────────────────────────────────────────────────

/**
 * Derive a stable, normalised tenant_id from name + unit.
 * Rules:
 *   1. Trim whitespace
 *   2. Lowercase
 *   3. Replace any run of non-alphanumeric chars with a single underscore
 *   4. Strip leading/trailing underscores
 *
 * Example: "  Maria Santos " + "101" → "maria_santos_101"
 */
function normalizeTenantId(name: string, unit: string): string {
  const raw = `${name.trim()}_${unit.trim()}`;
  return raw
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function normalizeLeaseStatus(raw: unknown): string {
  if (!raw) return "unknown";
  const s = String(raw).trim().toLowerCase();
  if (s.includes("current") || s.includes("active")) return "active";
  if (s.includes("past") || s.includes("former") || s.includes("vacated")) return "past";
  if (s.includes("future") || s.includes("pending") || s.includes("upcoming")) return "future";
  if (s.includes("notice") || s.includes("evict")) return "notice";
  return s || "unknown";
}

function toDateStr(val: unknown): string | null {
  if (!val) return null;
  const s = String(val).trim();
  if (!s || s === "null" || s === "undefined") return null;
  // Accept YYYY-MM-DD or MM/DD/YYYY
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) {
    const [m, d, y] = s.split("/");
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }
  return null;
}

function cleanPhone(val: unknown): string | null {
  if (!val) return null;
  const digits = String(val).replace(/\D/g, "");
  if (digits.length < 7) return null;
  return digits.length === 10
    ? `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`
    : digits;
}

function cleanEmail(val: unknown): string | null {
  if (!val) return null;
  const s = String(val).trim().toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s) ? s : null;
}

// ── Strategy ─────────────────────────────────────────────────────────────────

export const tenantDirectoryStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────

  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw  = ctx.bronze.raw_data;
    const rows = Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];
    const summary = (raw.summary ?? {}) as Record<string, unknown>;

    const normalizedRows = rows.map((r) => {
      const fullName = String(
        r.full_name ?? r.name ?? r.tenant_name ?? r.tenant ?? "unknown"
      ).trim();
      const unitId = String(
        r.unit ?? r.unit_id ?? r.unit_number ?? "unknown"
      ).trim();

      return {
        tenant_id:        normalizeTenantId(fullName, unitId),
        full_name:        fullName,
        unit_id:          unitId,
        email:            cleanEmail(r.email ?? r.email_address ?? r.contact_email),
        phone:            cleanPhone(r.phone ?? r.phone_number ?? r.mobile ?? r.cell),
        lease_start_date: toDateStr(r.lease_start_date ?? r.move_in_date ?? r.start_date),
        lease_end_date:   toDateStr(r.lease_end_date   ?? r.move_out_date ?? r.end_date),
        lease_status:     normalizeLeaseStatus(r.lease_status ?? r.status ?? r.tenancy_status),
      };
    });

    const statusCounts = normalizedRows.reduce<Record<string, number>>((acc, r) => {
      acc[r.lease_status] = (acc[r.lease_status] ?? 0) + 1;
      return acc;
    }, {});

    const normalized_data: Record<string, unknown> = {
      source:           "appfolio",
      report_type:      ctx.bronze.report_type,
      report_date:      ctx.bronze.report_date,
      bronze_report_id: ctx.bronze.id,
      transformed_at:   new Date().toISOString(),
      row_count:        normalizedRows.length,
      rows:             normalizedRows,
      summary: {
        total_tenants:   summary.total_tenants ?? normalizedRows.length,
        status_counts:   statusCounts,
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
      const tenantId       = String(row.tenant_id ?? "unknown");
      const fullName       = String(row.full_name  ?? "unknown");
      const unitId         = String(row.unit_id    ?? "unknown");
      const email          = typeof row.email  === "string" ? row.email  : null;
      const phone          = typeof row.phone  === "string" ? row.phone  : null;
      const leaseStart     = typeof row.lease_start_date === "string" ? row.lease_start_date : null;
      const leaseEnd       = typeof row.lease_end_date   === "string" ? row.lease_end_date   : null;
      const leaseStatus    = typeof row.lease_status     === "string" ? row.lease_status     : null;

      // UPSERT: update all mutable fields if tenant already exists.
      // created_at is preserved on conflict (only updated_at changes).
      const goldRows = await sql<GoldTenant[]>`
        INSERT INTO gold_tenants
          (bronze_report_id, tenant_id, full_name, unit_id,
           email, phone, lease_start_date, lease_end_date, lease_status,
           created_at, updated_at)
        VALUES (
          ${bronze.id},
          ${tenantId},
          ${fullName},
          ${unitId},
          ${email},
          ${phone},
          ${leaseStart}::date,
          ${leaseEnd}::date,
          ${leaseStatus},
          NOW(),
          NOW()
        )
        ON CONFLICT (tenant_id) DO UPDATE SET
          bronze_report_id = EXCLUDED.bronze_report_id,
          full_name        = EXCLUDED.full_name,
          unit_id          = EXCLUDED.unit_id,
          email            = COALESCE(EXCLUDED.email,  gold_tenants.email),
          phone            = COALESCE(EXCLUDED.phone,  gold_tenants.phone),
          lease_start_date = COALESCE(EXCLUDED.lease_start_date, gold_tenants.lease_start_date),
          lease_end_date   = COALESCE(EXCLUDED.lease_end_date,   gold_tenants.lease_end_date),
          lease_status     = EXCLUDED.lease_status,
          updated_at       = NOW()
        RETURNING *
      `;

      if (goldRows.length > 0) {
        goldIds.push(goldRows[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
