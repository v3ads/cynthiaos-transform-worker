// ── Gold Row Guard ────────────────────────────────────────────────────────────
//
// Pre-insert validation applied before any Gold table write.
// Rejects rows that would corrupt the Gold layer with known-bad values.
//
// Rules:
//  1. tenant_id must not be null/empty/unknown (except turnover — unit-centric)
//  2. unit_id must not be null/empty/unknown
//  3. balance_due must not be 0 when a delinquency record already exists
//     (prevents zero-balance ghost records — legitimate zero-balance is allowed
//      only on first insert, not on update of an existing non-zero record)
//
// Design: returns a RejectionResult instead of throwing, so the caller
// can log the rejection and continue processing other rows.

export interface RejectionResult {
  rejected: boolean;
  reasons: string[];
}

export type GoldTableName =
  | "gold_delinquency_records"
  | "gold_aged_receivables"
  | "gold_rent_roll"
  | "gold_lease_expirations"
  | "gold_tenants"
  | "gold_occupancy_snapshots"
  | "gold_unit_turnover"
  | "gold_income_statements";

// ── Report types that are unit-centric (tenant_id is optional) ────────────────
const UNIT_CENTRIC_TABLES = new Set<GoldTableName>([
  "gold_unit_turnover",
  "gold_occupancy_snapshots",
  "gold_income_statements",
]);

// ── Report types that require a positive balance_due ─────────────────────────
const BALANCE_TABLES = new Set<GoldTableName>([
  "gold_delinquency_records",
  "gold_aged_receivables",
]);

// ── Main guard function ───────────────────────────────────────────────────────

export function guardGoldRow(
  table: GoldTableName,
  row: Record<string, unknown>
): RejectionResult {
  const reasons: string[] = [];

  // ── Rule 1: tenant_id must be valid (unless unit-centric table) ───────────
  if (!UNIT_CENTRIC_TABLES.has(table)) {
    const tenantId = row.tenant_id;
    if (
      tenantId === null ||
      tenantId === undefined ||
      String(tenantId).trim() === "" ||
      String(tenantId).toLowerCase() === "unknown"
    ) {
      reasons.push(
        `tenant_id is invalid: "${tenantId}" — row would corrupt Gold JOIN. ` +
        `Check normalizeTenantId() in normalize.ts and the Silver strategy for this report type.`
      );
    }
  }

  // ── Rule 2: unit_id must be valid ─────────────────────────────────────────
  const unitId = row.unit_id;
  if (
    unitId !== undefined && // only check if field is present in this row
    (unitId === null ||
     String(unitId).trim() === "" ||
     String(unitId).toLowerCase() === "unknown")
  ) {
    reasons.push(
      `unit_id is invalid: "${unitId}" — check normalizeUnitId() in normalize.ts ` +
      `and the Silver field mapping for this report type.`
    );
  }

  // ── Rule 3: balance_due must not be zero for balance tables ──────────────
  // NOTE: We allow zero-balance on first insert (legitimate AppFolio data).
  // This guard only fires if the caller explicitly passes existingBalance > 0,
  // meaning we're about to overwrite a real balance with zero.
  if (BALANCE_TABLES.has(table)) {
    const balanceDue = row.balance_due ?? row.total_balance;
    const existingBalance = row._existing_balance as number | undefined;
    if (
      existingBalance !== undefined &&
      existingBalance > 0 &&
      balanceDue !== undefined &&
      Number(balanceDue) === 0
    ) {
      reasons.push(
        `balance_due is 0 but existing record has balance ${existingBalance} — ` +
        `would overwrite real balance with zero. Check Silver field mapping for total_balance.`
      );
    }
  }

  return {
    rejected: reasons.length > 0,
    reasons,
  };
}

// ── Convenience: log a rejection ─────────────────────────────────────────────

export function logRejection(
  table: GoldTableName,
  row: Record<string, unknown>,
  result: RejectionResult,
  serviceName: string
): void {
  if (!result.rejected) return;
  console.warn(
    `[${serviceName}] GOLD_GUARD REJECTED row for ${table}:`,
    JSON.stringify({
      table,
      tenant_id: row.tenant_id ?? null,
      unit_id: row.unit_id ?? null,
      reasons: result.reasons,
    })
  );
}
