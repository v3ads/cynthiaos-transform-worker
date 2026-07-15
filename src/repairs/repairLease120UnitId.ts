import postgres from "postgres";

const MALFORMED_UNIT_ID = "120-120-a";
const CANONICAL_UNIT_ID = "120-a";
const EXPECTED_TENANT_ID = "julianna_da_silva";
const EXPECTED_LEASE_END_DATE = "2026-12-31";

interface LeaseExpirationRow {
  id: string;
  bronze_report_id: string | null;
  tenant_id: string;
  unit_id: string;
  lease_start_date: Date | null;
  lease_end_date: Date | null;
  days_until_expiration: number | null;
  created_at: Date;
}

export interface Lease120RepairResult {
  repaired: boolean;
  reason: "repaired" | "already_repaired" | "source_not_found";
  unit_id: string | null;
  lease_end_date: string | null;
}

function toDateOnly(value: Date | string | null): string | null {
  if (value === null) return null;
  return value instanceof Date
    ? value.toISOString().slice(0, 10)
    : String(value).slice(0, 10);
}

/**
 * One-time, idempotent repair for the known doubled-prefix lease row.
 *
 * The transaction is deliberately restricted to the exact malformed ID,
 * expected tenant, and expected current lease end date. It never scans or
 * rewrites other unit IDs and it does not modify v_lease_population.
 */
export async function repairLease120UnitId(
  sql: postgres.Sql
): Promise<Lease120RepairResult> {
  return sql.begin(async (tx) => {
    // postgres.js transaction typings omit the callable tag signature even
    // though the runtime transaction object is the same parameterized SQL tag.
    const transaction = tx as unknown as postgres.Sql;

    const malformedRows = await transaction<LeaseExpirationRow[]>`
      SELECT
        id,
        bronze_report_id,
        tenant_id,
        unit_id,
        lease_start_date,
        lease_end_date,
        days_until_expiration,
        created_at
      FROM gold_lease_expirations
      WHERE unit_id = ${MALFORMED_UNIT_ID}
      FOR UPDATE
    `;

    if (malformedRows.length === 0) {
      const canonicalRows = await transaction<LeaseExpirationRow[]>`
        SELECT
          id,
          bronze_report_id,
          tenant_id,
          unit_id,
          lease_start_date,
          lease_end_date,
          days_until_expiration,
          created_at
        FROM gold_lease_expirations
        WHERE unit_id = ${CANONICAL_UNIT_ID}
          AND tenant_id = ${EXPECTED_TENANT_ID}
          AND lease_end_date = ${EXPECTED_LEASE_END_DATE}::date
        LIMIT 1
      `;

      if (canonicalRows.length === 1) {
        return {
          repaired: false,
          reason: "already_repaired",
          unit_id: CANONICAL_UNIT_ID,
          lease_end_date: toDateOnly(canonicalRows[0].lease_end_date),
        };
      }

      return {
        repaired: false,
        reason: "source_not_found",
        unit_id: null,
        lease_end_date: null,
      };
    }

    if (malformedRows.length !== 1) {
      throw new Error(
        `Expected exactly one ${MALFORMED_UNIT_ID} row; found ${malformedRows.length}`
      );
    }

    const source = malformedRows[0];
    const sourceLeaseEnd = toDateOnly(source.lease_end_date);

    if (
      source.tenant_id !== EXPECTED_TENANT_ID ||
      sourceLeaseEnd !== EXPECTED_LEASE_END_DATE
    ) {
      throw new Error(
        `Refusing ${MALFORMED_UNIT_ID} repair: expected tenant=${EXPECTED_TENANT_ID} ` +
          `and lease_end=${EXPECTED_LEASE_END_DATE}, found tenant=${source.tenant_id} ` +
          `and lease_end=${sourceLeaseEnd}`
      );
    }

    const repairedRows = await transaction<LeaseExpirationRow[]>`
      INSERT INTO gold_lease_expirations (
        bronze_report_id,
        tenant_id,
        unit_id,
        lease_start_date,
        lease_end_date,
        days_until_expiration,
        created_at
      )
      VALUES (
        ${source.bronze_report_id},
        ${source.tenant_id},
        ${CANONICAL_UNIT_ID},
        ${source.lease_start_date},
        ${source.lease_end_date},
        (${source.lease_end_date}::date - CURRENT_DATE),
        NOW()
      )
      ON CONFLICT (unit_id) DO UPDATE SET
        bronze_report_id      = EXCLUDED.bronze_report_id,
        tenant_id             = EXCLUDED.tenant_id,
        lease_start_date      = EXCLUDED.lease_start_date,
        lease_end_date        = EXCLUDED.lease_end_date,
        days_until_expiration = EXCLUDED.days_until_expiration,
        created_at            = NOW()
      RETURNING *
    `;

    await transaction`
      DELETE FROM gold_lease_expirations
      WHERE unit_id = ${MALFORMED_UNIT_ID}
        AND tenant_id = ${EXPECTED_TENANT_ID}
        AND lease_end_date = ${EXPECTED_LEASE_END_DATE}::date
    `;

    const remainingRows = await transaction<LeaseExpirationRow[]>`
      SELECT
        id,
        bronze_report_id,
        tenant_id,
        unit_id,
        lease_start_date,
        lease_end_date,
        days_until_expiration,
        created_at
      FROM gold_lease_expirations
      WHERE unit_id LIKE '120%'
      ORDER BY unit_id
    `;

    if (
      remainingRows.length !== 1 ||
      remainingRows[0].unit_id !== CANONICAL_UNIT_ID ||
      toDateOnly(remainingRows[0].lease_end_date) !== EXPECTED_LEASE_END_DATE
    ) {
      throw new Error(
        `Post-repair verification failed for 120% rows: ${JSON.stringify(
          remainingRows.map((row) => ({
            unit_id: row.unit_id,
            lease_end_date: toDateOnly(row.lease_end_date),
          }))
        )}`
      );
    }

    return {
      repaired: true,
      reason: "repaired",
      unit_id: repairedRows[0]?.unit_id ?? CANONICAL_UNIT_ID,
      lease_end_date: toDateOnly(
        repairedRows[0]?.lease_end_date ?? source.lease_end_date
      ),
    };
  });
}
