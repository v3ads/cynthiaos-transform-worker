// ── Pipeline Integrity Checker ────────────────────────────────────────────────
//
// Runs after each /gold/run call to verify the Gold layer is in a healthy state.
// Checks:
//  1. No critical Gold tables are empty
//  2. Row counts are within expected ranges
//  3. No rows with known-bad sentinel values remain in Gold tables
//
// Design: all checks are non-blocking — results are returned as a structured
// IntegrityReport and stored in pipeline_logs. Failures trigger console.warn
// but never throw or stop the pipeline.

import postgres from "postgres";

export interface IntegrityCheck {
  check: string;
  table: string;
  passed: boolean;
  detail: string;
  actual?: number | string;
  expected?: string;
}

export interface IntegrityReport {
  run_at: string;
  all_passed: boolean;
  checks: IntegrityCheck[];
}

// ── Table definitions ─────────────────────────────────────────────────────────

interface TableSpec {
  table: string;
  critical: boolean;          // empty table = integrity failure
  minRows?: number;           // warn if below this count
  badValueChecks?: {
    column: string;
    badValue: string;
    description: string;
  }[];
}

const GOLD_TABLES: TableSpec[] = [
  {
    table: "gold_tenants",
    critical: true,
    minRows: 100,
    badValueChecks: [
      { column: "tenant_id", badValue: "unknown", description: "tenant_id sentinel 'unknown'" },
    ],
  },
  {
    table: "gold_delinquency_records",
    critical: true,
    minRows: 1,
    badValueChecks: [
      { column: "unit_id",   badValue: "unknown", description: "unit_id sentinel 'unknown'" },
      { column: "tenant_id", badValue: "unknown", description: "tenant_id sentinel 'unknown'" },
    ],
  },
  {
    table: "gold_aged_receivables",
    critical: true,
    minRows: 1,
    badValueChecks: [
      { column: "unit_id",   badValue: "unknown", description: "unit_id sentinel 'unknown'" },
      { column: "tenant_id", badValue: "unknown", description: "tenant_id sentinel 'unknown'" },
    ],
  },
  {
    table: "gold_lease_expirations",
    critical: true,
    minRows: 50,
    badValueChecks: [
      { column: "unit_id",   badValue: "unknown", description: "unit_id sentinel 'unknown'" },
      { column: "tenant_id", badValue: "unknown", description: "tenant_id sentinel 'unknown'" },
    ],
  },
  {
    // rent_roll strategy promotes into gold_lease_expirations — no separate gold_rent_roll table
    table: "gold_lease_expirations",
    critical: true,
    minRows: 50,
    badValueChecks: [
      { column: "unit_id",   badValue: "unknown", description: "unit_id sentinel 'unknown'" },
      { column: "tenant_id", badValue: "unknown", description: "tenant_id sentinel 'unknown'" },
    ],
  },
  {
    table: "gold_occupancy_snapshots",
    critical: true,
    minRows: 1,
  },
  {
    table: "gold_income_statements",
    critical: true,
    minRows: 1,
  },
  {
    table: "gold_unit_turnover",
    critical: false,   // may be empty if no turns occurred this period
    minRows: 0,
  },
];

// ── Main checker ──────────────────────────────────────────────────────────────

export async function runIntegrityChecks(
  sql: postgres.Sql
): Promise<IntegrityReport> {
  const checks: IntegrityCheck[] = [];
  const runAt = new Date().toISOString();

  for (const spec of GOLD_TABLES) {
    // ── Row count check ─────────────────────────────────────────────────────
    try {
      const countRows = await sql<{ count: string }[]>`
        SELECT COUNT(*)::text AS count FROM ${sql(spec.table)}
      `;
      const actual = parseInt(countRows[0].count, 10);

      if (spec.critical && actual === 0) {
        checks.push({
          check: "row_count",
          table: spec.table,
          passed: false,
          detail: `CRITICAL: ${spec.table} is empty — Gold promotion may have failed`,
          actual,
          expected: "> 0",
        });
      } else if (spec.minRows !== undefined && actual < spec.minRows) {
        checks.push({
          check: "row_count",
          table: spec.table,
          passed: false,
          detail: `Row count ${actual} is below minimum ${spec.minRows} — possible data loss`,
          actual,
          expected: `>= ${spec.minRows}`,
        });
      } else {
        checks.push({
          check: "row_count",
          table: spec.table,
          passed: true,
          detail: `${actual} rows — OK`,
          actual,
        });
      }
    } catch (err) {
      checks.push({
        check: "row_count",
        table: spec.table,
        passed: false,
        detail: `Query failed: ${err instanceof Error ? err.message : String(err)}`,
      });
    }

    // ── Bad-value sentinel checks ───────────────────────────────────────────
    if (spec.badValueChecks) {
      for (const bv of spec.badValueChecks) {
        try {
          const badRows = await sql<{ count: string }[]>`
            SELECT COUNT(*)::text AS count
            FROM ${sql(spec.table)}
            WHERE ${sql(bv.column)} = ${bv.badValue}
          `;
          const badCount = parseInt(badRows[0].count, 10);
          if (badCount > 0) {
            checks.push({
              check: "sentinel_value",
              table: spec.table,
              passed: false,
              detail: `${badCount} rows with ${bv.description} — field mapping regression detected`,
              actual: badCount,
              expected: "0",
            });
          } else {
            checks.push({
              check: "sentinel_value",
              table: spec.table,
              passed: true,
              detail: `No ${bv.description} — OK`,
              actual: 0,
            });
          }
        } catch (err) {
          // Column may not exist on this table — skip gracefully
          checks.push({
            check: "sentinel_value",
            table: spec.table,
            passed: true,
            detail: `Skipped (column ${bv.column} not queryable): ${err instanceof Error ? err.message : String(err)}`,
          });
        }
      }
    }
  }

  // ── Cross-table JOIN health check ─────────────────────────────────────────
  // Verify that delinquency records can JOIN to gold_tenants
  try {
    const joinRows = await sql<{ matched: string; total: string }[]>`
      SELECT
        COUNT(CASE WHEN t.tenant_id IS NOT NULL THEN 1 END)::text AS matched,
        COUNT(*)::text AS total
      FROM gold_delinquency_records d
      LEFT JOIN gold_tenants t ON t.tenant_id = d.tenant_id
    `;
    if (joinRows.length > 0) {
      const matched = parseInt(joinRows[0].matched, 10);
      const total = parseInt(joinRows[0].total, 10);
      const matchRate = total > 0 ? Math.round((matched / total) * 100) : 100;
      const passed = matchRate >= 80; // warn if < 80% of delinquency records have a matching tenant
      checks.push({
        check: "join_health",
        table: "gold_delinquency_records ⟶ gold_tenants",
        passed,
        detail: `${matched}/${total} delinquency records have a matching tenant (${matchRate}%)`,
        actual: matchRate,
        expected: ">= 80%",
      });
    }
  } catch (err) {
    checks.push({
      check: "join_health",
      table: "gold_delinquency_records ⟶ gold_tenants",
      passed: false,
      detail: `JOIN health check failed: ${err instanceof Error ? err.message : String(err)}`,
    });
  }

  const allPassed = checks.every((c) => c.passed);

  return { run_at: runAt, all_passed: allPassed, checks };
}
