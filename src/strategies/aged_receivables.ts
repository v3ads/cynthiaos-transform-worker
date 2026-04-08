// ── aged_receivables strategy ─────────────────────────────────────────────────
//
// Silver: normalises AppFolio aged receivables report rows into consistent shape:
//         { tenant_id, unit_id, total_balance,
//           bucket_0_30, bucket_31_60, bucket_61_90, bucket_90_plus }
//
// Gold:   promotes each row into gold_aged_receivables with derived fields:
//         dominant_bucket — the aging bucket with the highest dollar amount
//         risk_score      — weighted sum: 0-30→1x, 31-60→2x, 61-90→3x, 90+→5x
//
// FIX (2026-04-08): Gold promotion now reads `unit_id ?? unit` to handle
//   legacy Silver records that stored the unit number under the key `unit`.
//   Also switched ON CONFLICT to DO UPDATE so re-running Gold promotion
//   corrects existing records with unit_id='unknown'.

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";
import { normalizeTenantId, normalizeUnitId } from "../utils/normalize";

// ── Gold row interface ────────────────────────────────────────────────────────

interface GoldAgedReceivableRecord {
  id: string;
  bronze_report_id: string | null;
  tenant_id: string;
  unit_id: string;
  total_balance: string;    // NUMERIC returns as string from postgres driver
  bucket_0_30: string;
  bucket_31_60: string;
  bucket_61_90: string;
  bucket_90_plus: string;
  dominant_bucket: string;
  risk_score: string;
  created_at: Date;
}

// ── Derivation helpers ────────────────────────────────────────────────────────

type BucketKey = "0_30" | "31_60" | "61_90" | "90_plus";

function deriveDominantBucket(
  b0_30: number,
  b31_60: number,
  b61_90: number,
  b90plus: number
): BucketKey {
  const buckets: [BucketKey, number][] = [
    ["0_30",    b0_30],
    ["31_60",   b31_60],
    ["61_90",   b61_90],
    ["90_plus", b90plus],
  ];
  // Sort descending by amount; on tie, prefer the older bucket (higher weight)
  buckets.sort((a, b) => b[1] - a[1]);
  return buckets[0][0];
}

function deriveRiskScore(
  b0_30: number,
  b31_60: number,
  b61_90: number,
  b90plus: number
): number {
  return (b0_30 * 1) + (b31_60 * 2) + (b61_90 * 3) + (b90plus * 5);
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

// ── Strategy ─────────────────────────────────────────────────────────────────

export const agedReceivablesStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────

  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw  = ctx.bronze.raw_data;
    // Support both AppFolio native format (raw.results) and legacy format (raw.rows)
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];
    const summary = (raw.summary ?? {}) as Record<string, unknown>;

    // AppFolio aged_receivables_detail has one row per invoice line, not per tenant.
    // Aggregate by tenant (PayerName + UnitName) before normalising.
    const tenantMap = new Map<string, { rawName: string; rawUnit: string; b0_30: number; b31_60: number; b61_90: number; b90plus: number; }>();
    for (const r of rows) {
      // AppFolio PascalCase fields; also support legacy snake_case
      const rawName = String(r.PayerName ?? r.tenant ?? r.tenant_id ?? r.tenant_name ?? r.name ?? r.resident ?? "");
      const rawUnit = String(r.UnitName  ?? r.unit   ?? r.unit_id   ?? r.unit_number  ?? "");
      const key = `${rawName}||${rawUnit}`;
      const existing = tenantMap.get(key) ?? { rawName, rawUnit, b0_30: 0, b31_60: 0, b61_90: 0, b90plus: 0 };
      existing.b0_30   += toNum(r["0To30"]   ?? r.bucket_0_30   ?? r["0_30"]   ?? r.current    ?? 0);
      existing.b31_60  += toNum(r["30To60"]  ?? r.bucket_31_60  ?? r["31_60"]  ?? r.days_31_60 ?? 0);
      existing.b61_90  += toNum(r["60To90"]  ?? r.bucket_61_90  ?? r["61_90"]  ?? r.days_61_90 ?? 0);
      existing.b90plus += toNum(r["90Plus"]  ?? r.bucket_90_plus ?? r["90_plus"] ?? r.over_90  ?? 0);
      tenantMap.set(key, existing);
    }

    const normalizedRows = Array.from(tenantMap.values()).map(({ rawName, rawUnit, b0_30, b31_60, b61_90, b90plus }) => {
      const tenantId = normalizeTenantId(rawName, rawUnit);
      const unitId   = normalizeUnitId(rawUnit);
      const totalBalance = b0_30 + b31_60 + b61_90 + b90plus;

      return {
        tenant_id:      tenantId,
        unit_id:        unitId,
        total_balance:  totalBalance,
        bucket_0_30:    b0_30,
        bucket_31_60:   b31_60,
        bucket_61_90:   b61_90,
        bucket_90_plus: b90plus,
        dominant_bucket: deriveDominantBucket(b0_30, b31_60, b61_90, b90plus),
        risk_score:     deriveRiskScore(b0_30, b31_60, b61_90, b90plus),
      };
    });

    const totalAR = normalizedRows.reduce((acc, r) => acc + r.total_balance, 0);

    const normalized_data: Record<string, unknown> = {
      source:           "appfolio",
      report_type:      ctx.bronze.report_type,
      report_date:      ctx.bronze.report_date,
      bronze_report_id: ctx.bronze.id,
      transformed_at:   new Date().toISOString(),
      row_count:        normalizedRows.length,
      rows:             normalizedRows,
      summary: {
        total_tenants:       summary.total_tenants       ?? normalizedRows.length,
        total_ar_balance:    summary.total_ar_balance    ?? totalAR,
        high_risk_count:     normalizedRows.filter((r) => r.dominant_bucket === "90_plus").length,
        medium_risk_count:   normalizedRows.filter((r) => r.dominant_bucket === "61_90").length,
        low_risk_count:      normalizedRows.filter((r) =>
          r.dominant_bucket === "0_30" || r.dominant_bucket === "31_60"
        ).length,
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

      const totalBalance  = toNum(row.total_balance);
      const b0_30         = toNum(row.bucket_0_30);
      const b31_60        = toNum(row.bucket_31_60);
      const b61_90        = toNum(row.bucket_61_90);
      const b90plus       = toNum(row.bucket_90_plus);
      const dominantBucket = String(
        row.dominant_bucket ?? deriveDominantBucket(b0_30, b31_60, b61_90, b90plus)
      );
      const riskScore     = typeof row.risk_score === "number"
        ? row.risk_score
        : deriveRiskScore(b0_30, b31_60, b61_90, b90plus);

      const goldRows = await sql<GoldAgedReceivableRecord[]>`
        INSERT INTO gold_aged_receivables
          (bronze_report_id, tenant_id, unit_id,
           total_balance, bucket_0_30, bucket_31_60, bucket_61_90, bucket_90_plus,
           dominant_bucket, risk_score, created_at)
        VALUES (
          ${bronze.id},
          ${tenantId},
          ${unitId},
          ${totalBalance},
          ${b0_30},
          ${b31_60},
          ${b61_90},
          ${b90plus},
          ${dominantBucket},
          ${riskScore},
          NOW()
        )
        ON CONFLICT (bronze_report_id, tenant_id, unit_id)
        DO UPDATE SET
          unit_id         = EXCLUDED.unit_id,
          total_balance   = EXCLUDED.total_balance,
          bucket_0_30     = EXCLUDED.bucket_0_30,
          bucket_31_60    = EXCLUDED.bucket_31_60,
          bucket_61_90    = EXCLUDED.bucket_61_90,
          bucket_90_plus  = EXCLUDED.bucket_90_plus,
          dominant_bucket = EXCLUDED.dominant_bucket,
          risk_score      = EXCLUDED.risk_score
        RETURNING *
      `;

      if (goldRows.length > 0) {
        goldIds.push(goldRows[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
