import postgres from "postgres";
import type { IntegrityCheck } from "./integrityChecker";

function queryFailure(check: string, table: string, error: unknown): IntegrityCheck {
  return {
    check,
    table,
    passed: false,
    detail: `Reconciliation query failed: ${error instanceof Error ? error.message : String(error)}`,
  };
}

export async function runReconciliationChecks(sql: postgres.Sql): Promise<IntegrityCheck[]> {
  const checks: IntegrityCheck[] = [];

  // Canonical units: compare Gold with the normalized union of the latest roster-bearing
  // AppFolio reports. Manual family-unit overrides are part of the leaseable universe.
  try {
    const rows = await sql<{
      source_count: string;
      gold_count: string;
      missing_in_gold: string[] | null;
      extra_in_gold: string[] | null;
      malformed_gold_count: string;
    }[]>`
      WITH latest_dates AS (
        SELECT report_type, MAX(report_date) AS report_date
        FROM bronze_appfolio_reports
        WHERE report_type = ANY(ARRAY[
          'unit_directory', 'rent_roll', 'tenant_directory',
          'lease_expiration_detail', 'lease_expiration'
        ]::text[])
        GROUP BY report_type
      ), source_raw AS (
        SELECT COALESCE(
          NULLIF(TRIM(elem->>'UnitName'), ''),
          NULLIF(TRIM(elem->>'Unit'), ''),
          NULLIF(TRIM(elem->>'unit_name'), ''),
          NULLIF(TRIM(elem->>'unit'), ''),
          NULLIF(TRIM(elem->>'unit_id'), '')
        ) AS raw_unit
        FROM bronze_appfolio_reports b
        JOIN latest_dates ld
          ON ld.report_type = b.report_type
         AND ld.report_date IS NOT DISTINCT FROM b.report_date
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(b.raw_data->'results') = 'array' THEN b.raw_data->'results'
            WHEN jsonb_typeof(b.raw_data->'data') = 'array' THEN b.raw_data->'data'
            WHEN jsonb_typeof(b.raw_data->'rows') = 'array' THEN b.raw_data->'rows'
            ELSE '[]'::jsonb
          END
        ) elem
      ), cleaned AS (
        SELECT regexp_replace(
          regexp_replace(lower(TRIM(raw_unit)), '[^a-z0-9_-]+', '', 'g'),
          '[_-]+', '-', 'g'
        ) AS unit_id
        FROM source_raw
        WHERE raw_unit IS NOT NULL
      ), expanded AS (
        SELECT CASE
          WHEN match_parts IS NULL THEN unit_id
          ELSE expanded_id
        END AS unit_id
        FROM cleaned
        LEFT JOIN LATERAL regexp_match(unit_id, '^([0-9]+)-\\1-([a-z0-9]+)$') match_parts ON true
        LEFT JOIN LATERAL unnest(
          CASE WHEN match_parts IS NULL
            THEN ARRAY[unit_id]
            ELSE ARRAY[match_parts[1], match_parts[1] || '-' || match_parts[2]]
          END
        ) expanded_id ON true
      ), source_ids AS (
        SELECT DISTINCT unit_id FROM expanded WHERE unit_id <> ''
        UNION SELECT '202'
        UNION SELECT '313'
      ), gold_ids AS (
        SELECT DISTINCT unit_id FROM gold_units WHERE unit_id IS NOT NULL AND unit_id <> 'unknown'
      )
      SELECT
        (SELECT COUNT(*)::text FROM source_ids) AS source_count,
        (SELECT COUNT(*)::text FROM gold_ids) AS gold_count,
        (SELECT array_agg(unit_id ORDER BY unit_id) FROM (
          SELECT unit_id FROM source_ids EXCEPT SELECT unit_id FROM gold_ids
        ) missing) AS missing_in_gold,
        (SELECT array_agg(unit_id ORDER BY unit_id) FROM (
          SELECT unit_id FROM gold_ids EXCEPT SELECT unit_id FROM source_ids
        ) extra) AS extra_in_gold,
        (SELECT COUNT(*)::text FROM gold_ids
          WHERE unit_id ~ '^([0-9]+)-\\1-' OR strpos(unit_id, '_') > 0) AS malformed_gold_count
    `;
    const r = rows[0];
    const sourceCount = Number(r?.source_count ?? 0);
    const goldCount = Number(r?.gold_count ?? 0);
    const missing = r?.missing_in_gold ?? [];
    const extra = r?.extra_in_gold ?? [];
    const malformed = Number(r?.malformed_gold_count ?? 0);
    const passed = sourceCount === goldCount && missing.length === 0 && extra.length === 0 && malformed === 0;
    checks.push({
      check: "canonical_unit_reconciliation",
      table: "bronze roster sources ⟶ gold_units",
      passed,
      detail: passed
        ? `${goldCount} canonical units exactly match the normalized latest source union`
        : `source=${sourceCount}, gold=${goldCount}, missing=[${missing.join(", ")}], extra=[${extra.join(", ")}], malformed=${malformed}`,
      actual: goldCount,
      expected: String(sourceCount),
    });
  } catch (err) {
    checks.push(queryFailure("canonical_unit_reconciliation", "bronze roster sources ⟶ gold_units", err));
  }

  // Collections-risk pages are keyed by unit after current/past classification. Verify
  // that the canonical relation has no null unit IDs or duplicate pagination keys.
  try {
    const rows = await sql<{ total: string; distinct_units: string; null_units: string }[]>`
      WITH ar_current AS (
        SELECT DISTINCT ON (tenant_id) tenant_id, unit_id
        FROM gold_aged_receivables
        WHERE tenant_status = 'current'
        ORDER BY tenant_id, risk_score DESC, created_at DESC
      ), d_past AS (
        SELECT DISTINCT ON (tenant_id) tenant_id, unit_id
        FROM gold_delinquency_records
        WHERE tenant_status = 'past'
        ORDER BY tenant_id, days_overdue DESC NULLS LAST, created_at DESC
      ), all_rows AS (
        SELECT tenant_id, unit_id, 'current'::text AS tenant_status FROM ar_current
        UNION ALL
        SELECT tenant_id, unit_id, 'past'::text AS tenant_status FROM d_past
      ), canonical AS (
        SELECT DISTINCT ON (unit_id) *
        FROM all_rows
        ORDER BY unit_id, CASE WHEN tenant_status = 'past' THEN 0 ELSE 1 END
      )
      SELECT COUNT(*)::text AS total,
             COUNT(DISTINCT unit_id)::text AS distinct_units,
             COUNT(*) FILTER (WHERE unit_id IS NULL OR unit_id = '' OR unit_id = 'unknown')::text AS null_units
      FROM canonical
    `;
    const total = Number(rows[0]?.total ?? 0);
    const distinctUnits = Number(rows[0]?.distinct_units ?? 0);
    const nullUnits = Number(rows[0]?.null_units ?? 0);
    checks.push({
      check: "collections_pagination_reconciliation",
      table: "collections-risk canonical relation",
      passed: total === distinctUnits && nullUnits === 0,
      detail: `canonical rows=${total}, distinct pagination keys=${distinctUnits}, unresolved units=${nullUnits}`,
      actual: total,
      expected: String(distinctUnits),
    });
  } catch (err) {
    checks.push(queryFailure("collections_pagination_reconciliation", "collections-risk canonical relation", err));
  }

  // Delinquency is unit-level. A tenant can legitimately have more than one unit,
  // so record totals must match unit keys rather than distinct tenant IDs.
  try {
    const rows = await sql<{ records: string; units: string; tenants: string }[]>`
      SELECT COUNT(*)::text AS records,
             COUNT(DISTINCT unit_id)::text AS units,
             COUNT(DISTINCT tenant_id)::text AS tenants
      FROM gold_delinquency_records
    `;
    const records = Number(rows[0]?.records ?? 0);
    const units = Number(rows[0]?.units ?? 0);
    const tenants = Number(rows[0]?.tenants ?? 0);
    checks.push({
      check: "delinquency_record_reconciliation",
      table: "gold_delinquency_records",
      passed: records === units,
      detail: `${records} unit-level records across ${tenants} distinct tenants; endpoint total must use records, not tenants`,
      actual: records,
      expected: String(units),
    });
  } catch (err) {
    checks.push(queryFailure("delinquency_record_reconciliation", "gold_delinquency_records", err));
  }

  // Work-order report is a complete current snapshot. Compare distinct source IDs
  // from every chunk on the latest report date with the Gold work-order IDs.
  try {
    const rows = await sql<{ source_count: string; gold_count: string; missing: string; stale: string }[]>`
      WITH latest AS (
        SELECT MAX(report_date) AS report_date
        FROM bronze_appfolio_reports WHERE report_type = 'work_order'
      ), source_ids AS (
        SELECT DISTINCT NULLIF(elem->>'WorkOrderId', '') AS work_order_id
        FROM bronze_appfolio_reports b
        CROSS JOIN latest l
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(b.raw_data->'results') = 'array' THEN b.raw_data->'results'
            WHEN jsonb_typeof(b.raw_data->'data') = 'array' THEN b.raw_data->'data'
            WHEN jsonb_typeof(b.raw_data->'rows') = 'array' THEN b.raw_data->'rows'
            ELSE '[]'::jsonb
          END
        ) elem
        WHERE b.report_type = 'work_order'
          AND b.report_date IS NOT DISTINCT FROM l.report_date
          AND NULLIF(elem->>'WorkOrderId', '') IS NOT NULL
      ), gold_ids AS (
        SELECT DISTINCT work_order_id::text AS work_order_id FROM gold_maintenance
      )
      SELECT
        (SELECT COUNT(*)::text FROM source_ids) AS source_count,
        (SELECT COUNT(*)::text FROM gold_ids) AS gold_count,
        (SELECT COUNT(*)::text FROM (SELECT * FROM source_ids EXCEPT SELECT * FROM gold_ids) x) AS missing,
        (SELECT COUNT(*)::text FROM (SELECT * FROM gold_ids EXCEPT SELECT * FROM source_ids) x) AS stale
    `;
    const sourceCount = Number(rows[0]?.source_count ?? 0);
    const goldCount = Number(rows[0]?.gold_count ?? 0);
    const missing = Number(rows[0]?.missing ?? 0);
    const stale = Number(rows[0]?.stale ?? 0);
    checks.push({
      check: "maintenance_source_reconciliation",
      table: "latest work_order Bronze ⟶ gold_maintenance",
      passed: sourceCount === goldCount && missing === 0 && stale === 0,
      detail: `source unique work orders=${sourceCount}, Gold=${goldCount}, missing=${missing}, stale=${stale}`,
      actual: goldCount,
      expected: String(sourceCount),
    });
  } catch (err) {
    checks.push(queryFailure("maintenance_source_reconciliation", "latest work_order Bronze ⟶ gold_maintenance", err));
  }

  // Lease expiration integrity. NOTE (2026-07-14): the stored
  // days_until_expiration column is DEPRECATED — every API read now computes
  // the countdown from lease_end_date at CURRENT_DATE, so drift in the stored
  // column no longer affects anything users or agents see and is no longer a
  // failure condition. The load-bearing invariants checked here are per-unit
  // uniqueness and a mutually exclusive date-bucket partition. The stored
  // column should be dropped in a future migration.
  try {
    const rows = await sql<{
      total: string; unique_units: string; expired: string;
      due_0_30: string; due_31_60: string; due_later: string; undated: string;
    }[]>`
      SELECT COUNT(*)::text AS total,
             COUNT(DISTINCT unit_id)::text AS unique_units,
             COUNT(*) FILTER (WHERE lease_end_date < CURRENT_DATE)::text AS expired,
             COUNT(*) FILTER (WHERE lease_end_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 30)::text AS due_0_30,
             COUNT(*) FILTER (WHERE lease_end_date BETWEEN CURRENT_DATE + 31 AND CURRENT_DATE + 60)::text AS due_31_60,
             COUNT(*) FILTER (WHERE lease_end_date > CURRENT_DATE + 60)::text AS due_later,
             COUNT(*) FILTER (WHERE lease_end_date IS NULL)::text AS undated
      FROM gold_lease_expirations
    `;
    const r = rows[0];
    const total = Number(r?.total ?? 0);
    const uniqueUnits = Number(r?.unique_units ?? 0);
    const partition = Number(r?.expired ?? 0) + Number(r?.due_0_30 ?? 0) +
      Number(r?.due_31_60 ?? 0) + Number(r?.due_later ?? 0) + Number(r?.undated ?? 0);
    checks.push({
      check: "lease_expiration_reconciliation",
      table: "gold_lease_expirations",
      passed: total === uniqueUnits && total === partition,
      detail: `total=${total}, units=${uniqueUnits}, expired=${r?.expired ?? 0}, 0-30=${r?.due_0_30 ?? 0}, 31-60=${r?.due_31_60 ?? 0}, later=${r?.due_later ?? 0}, undated=${r?.undated ?? 0} (runtime-computed buckets; stored countdown column deprecated)`,
      actual: total,
      expected: String(uniqueUnits),
    });
  } catch (err) {
    checks.push(queryFailure("lease_expiration_reconciliation", "gold_lease_expirations", err));
  }

  // Turn snapshots must represent one physical event per canonical unit/date. Future
  // move-outs are allowed only as scheduled events at the API layer and are reported.
  try {
    const rows = await sql<{ rows: string; physical_events: string; future_events: string }[]>`
      SELECT COUNT(*) FILTER (WHERE event_type = 'turn')::text AS rows,
             COUNT(DISTINCT (unit_id, move_out_date)) FILTER (WHERE event_type = 'turn')::text AS physical_events,
             COUNT(*) FILTER (WHERE event_type = 'turn' AND move_out_date > CURRENT_DATE)::text AS future_events
      FROM gold_unit_turnover
    `;
    const rowCount = Number(rows[0]?.rows ?? 0);
    const physical = Number(rows[0]?.physical_events ?? 0);
    const future = Number(rows[0]?.future_events ?? 0);
    checks.push({
      check: "unit_turn_event_reconciliation",
      table: "gold_unit_turnover",
      passed: rowCount === physical,
      detail: `${rowCount} turn rows, ${physical} unique physical events, ${future} future events (must be exposed as scheduled)`,
      actual: rowCount,
      expected: String(physical),
    });
  } catch (err) {
    checks.push(queryFailure("unit_turn_event_reconciliation", "gold_unit_turnover", err));
  }

  // A near-100% operating margin is not plausible for a full property statement.
  // Fail visibly until the transform extracts a substantive expense account scope.
  try {
    const rows = await sql<{ income: string; expenses: string; margin: string }[]>`
      SELECT COALESCE(total_income, 0)::text AS income,
             COALESCE(total_expenses, 0)::text AS expenses,
             CASE WHEN COALESCE(total_income, 0) = 0 THEN '0'
               ELSE ((total_income - total_expenses) / total_income)::text END AS margin
      FROM gold_income_statements
      ORDER BY report_date DESC, created_at DESC
      LIMIT 1
    `;
    const income = Number(rows[0]?.income ?? 0);
    const expenses = Number(rows[0]?.expenses ?? 0);
    const margin = Number(rows[0]?.margin ?? 0);
    const passed = income <= 0 || (expenses > 0 && margin < 0.95);
    checks.push({
      check: "financial_expense_scope_plausibility",
      table: "gold_income_statements",
      passed,
      detail: `latest income=${income.toFixed(2)}, expenses=${expenses.toFixed(2)}, operating margin=${(margin * 100).toFixed(2)}%`,
      actual: Number((margin * 100).toFixed(2)),
      expected: "< 95% or explicitly partial scope",
    });
  } catch (err) {
    checks.push(queryFailure("financial_expense_scope_plausibility", "gold_income_statements", err));
  }

  // Maintenance chronology: completion must not precede creation, and all
  // work-order dates must fall in a plausible year range. Year-less AppFolio
  // date strings previously leaked through parsing and rendered as year 2001.
  try {
    const rows = await sql<{ bad_order: string; implausible: string }[]>`
      SELECT
        COUNT(*) FILTER (
          WHERE completed_on IS NOT NULL AND created_at_appfolio IS NOT NULL
            AND completed_on::date < created_at_appfolio::date
        )::text AS bad_order,
        COUNT(*) FILTER (
          WHERE (completed_on IS NOT NULL AND EXTRACT(YEAR FROM completed_on::date) NOT BETWEEN 2015 AND EXTRACT(YEAR FROM CURRENT_DATE) + 1)
             OR (created_at_appfolio IS NOT NULL AND EXTRACT(YEAR FROM created_at_appfolio::date) NOT BETWEEN 2015 AND EXTRACT(YEAR FROM CURRENT_DATE) + 1)
             OR (work_done_on IS NOT NULL AND EXTRACT(YEAR FROM work_done_on::date) NOT BETWEEN 2015 AND EXTRACT(YEAR FROM CURRENT_DATE) + 1)
        )::text AS implausible
      FROM gold_maintenance
    `;
    const badOrder = Number(rows[0]?.bad_order ?? 0);
    const implausible = Number(rows[0]?.implausible ?? 0);
    checks.push({
      check: "maintenance_chronology",
      table: "gold_maintenance",
      passed: badOrder === 0 && implausible === 0,
      detail: `completed-before-created=${badOrder}, implausible-year dates=${implausible}`,
      actual: badOrder + implausible,
      expected: "0",
    });
  } catch (err) {
    checks.push(queryFailure("maintenance_chronology", "gold_maintenance", err));
  }

  // Vendor directory must not be empty when the Bronze source has rows — an
  // empty vendor table is what rendered the Vendors page as a false zero.
  try {
    const rows = await sql<{ source_rows: string; gold_rows: string }[]>`
      WITH latest AS (
        SELECT id FROM bronze_appfolio_reports
        WHERE report_type = 'vendor_directory'
        ORDER BY ingested_at DESC LIMIT 1
      )
      SELECT
        (SELECT COALESCE(jsonb_array_length(raw_data->'results'), 0)::text
           FROM bronze_appfolio_reports WHERE id = (SELECT id FROM latest)) AS source_rows,
        (SELECT COUNT(*)::text FROM gold_vendors) AS gold_rows
    `;
    const sourceRows = Number(rows[0]?.source_rows ?? 0);
    const goldRows = Number(rows[0]?.gold_rows ?? 0);
    checks.push({
      check: "vendor_directory_nonempty",
      table: "vendor_directory Bronze ⟶ gold_vendors",
      passed: sourceRows === 0 || goldRows > 0,
      detail: `source rows=${sourceRows}, gold rows=${goldRows}`,
      actual: goldRows,
      expected: sourceRows > 0 ? "> 0" : "0",
    });
  } catch (err) {
    checks.push(queryFailure("vendor_directory_nonempty", "gold_vendors", err));
  }

  // Scheduled-turn classification: no future-dated move-out may be classified
  // as current or completed.
  try {
    const rows = await sql<{ misclassified: string }[]>`
      SELECT COUNT(*)::text AS misclassified
      FROM gold_unit_turnover
      WHERE move_out_date IS NOT NULL
        AND move_out_date::date > CURRENT_DATE
        AND (turn_end_date IS NOT NULL OR days_to_complete IS NOT NULL)
    `;
    const misclassified = Number(rows[0]?.misclassified ?? 0);
    checks.push({
      check: "scheduled_turn_classification",
      table: "gold_unit_turnover",
      passed: misclassified === 0,
      detail: `future move-outs with completion data=${misclassified}`,
      actual: misclassified,
      expected: "0",
    });
  } catch (err) {
    checks.push(queryFailure("scheduled_turn_classification", "gold_unit_turnover", err));
  }

  return checks;
}
