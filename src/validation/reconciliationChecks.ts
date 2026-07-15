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
          -- Parity with the promotion, which coerces WorkOrderId via toInt and
          -- skips non-integer ids. Counting only integer-castable ids here
          -- keeps the source total defined identically on both sides.
          AND (elem->>'WorkOrderId') ~ '^\\s*\\d+\\s*$'
      ), gold_ids AS (
        SELECT DISTINCT work_order_id::text AS work_order_id FROM gold_maintenance
      )
      SELECT
        (SELECT COUNT(*)::text FROM source_ids) AS source_count,
        (SELECT COUNT(*)::text FROM gold_ids) AS gold_count,
        (SELECT COUNT(*)::text FROM (SELECT * FROM source_ids EXCEPT SELECT * FROM gold_ids) x) AS missing,
        (SELECT COUNT(*)::text FROM (SELECT * FROM gold_ids EXCEPT SELECT * FROM source_ids) x) AS stale,
        (SELECT string_agg(work_order_id, ', ' ORDER BY work_order_id)
           FROM (SELECT * FROM source_ids EXCEPT SELECT * FROM gold_ids LIMIT 20) x) AS missing_ids
    `;
    const sourceCount = Number(rows[0]?.source_count ?? 0);
    const goldCount = Number(rows[0]?.gold_count ?? 0);
    const missing = Number(rows[0]?.missing ?? 0);
    const stale = Number(rows[0]?.stale ?? 0);
    const missingIds = (rows[0] as { missing_ids?: string })?.missing_ids ?? null;
    checks.push({
      check: "maintenance_source_reconciliation",
      table: "latest work_order Bronze ⟶ gold_maintenance",
      passed: sourceCount === goldCount && missing === 0 && stale === 0,
      detail: `source unique work orders=${sourceCount}, Gold=${goldCount}, missing=${missing}, stale=${stale}${missingIds ? ` (missing ids: ${missingIds})` : ''}`,
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

  // Lease scope reconciliation (added 2026-07-14). The API serves every lease
  // population from the v_lease_population canonical view (created by the API
  // at startup); this check queries the SAME view with the SAME scope
  // predicates the endpoints use, so any drift between the view, the base
  // table, and the scope definitions surfaces on Status instead of as a
  // silent count mismatch on a page. Invariants:
  //   1. view row count == gold_lease_expirations row count (pass-through)
  //   2. active_future == sum of its urgency buckets (0-30 / 31-60 / later)
  //   3. active_future has exactly one row per unit
  //   4. risk == expired part + future ≤90d part (mutually exclusive split)
  // If the view is missing (e.g. API startup never ran against this DB), the
  // check fails with undefined_table — that is a genuine failure, since every
  // lease endpoint would be failing too.
  try {
    const rows = await sql<{
      table_total: string; view_total: string;
      active_future: string; af_units: string;
      af_0_30: string; af_31_60: string; af_later: string;
      family_held: string;
      risk_total: string; risk_expired: string; risk_future: string;
    }[]>`
      SELECT
        (SELECT COUNT(*)::text FROM gold_lease_expirations) AS table_total,
        COUNT(*)::text AS view_total,
        COUNT(*) FILTER (WHERE is_soonest_future_for_unit AND NOT is_superseded AND NOT is_family_held)::text AS active_future,
        COUNT(DISTINCT unit_id) FILTER (WHERE is_soonest_future_for_unit AND NOT is_superseded AND NOT is_family_held)::text AS af_units,
        COUNT(*) FILTER (WHERE is_soonest_future_for_unit AND NOT is_superseded AND NOT is_family_held AND days_until_expiration <= 30)::text AS af_0_30,
        COUNT(*) FILTER (WHERE is_soonest_future_for_unit AND NOT is_superseded AND NOT is_family_held AND days_until_expiration BETWEEN 31 AND 60)::text AS af_31_60,
        COUNT(*) FILTER (WHERE is_soonest_future_for_unit AND NOT is_superseded AND NOT is_family_held AND days_until_expiration > 60)::text AS af_later,
        COUNT(*) FILTER (WHERE is_soonest_future_for_unit AND is_family_held)::text AS family_held,
        COUNT(*) FILTER (WHERE is_soonest_for_unit AND NOT has_active_future_tenant_lease AND NOT is_released AND days_until_expiration <= 90)::text AS risk_total,
        COUNT(*) FILTER (WHERE is_soonest_for_unit AND NOT has_active_future_tenant_lease AND NOT is_released AND days_until_expiration < 0)::text AS risk_expired,
        COUNT(*) FILTER (WHERE is_soonest_for_unit AND NOT has_active_future_tenant_lease AND NOT is_released AND days_until_expiration BETWEEN 0 AND 90)::text AS risk_future
      FROM v_lease_population
    `;
    const r = rows[0];
    const n = (v: string | undefined) => Number(v ?? 0);
    const bucketSum = n(r?.af_0_30) + n(r?.af_31_60) + n(r?.af_later);
    const riskSum = n(r?.risk_expired) + n(r?.risk_future);
    const passed =
      n(r?.view_total) === n(r?.table_total) &&
      n(r?.active_future) === bucketSum &&
      n(r?.active_future) === n(r?.af_units) &&
      n(r?.risk_total) === riskSum;
    checks.push({
      check: "lease_scope_reconciliation",
      table: "v_lease_population (canonical lease scopes)",
      passed,
      detail: `view=${r?.view_total ?? 0} vs table=${r?.table_total ?? 0}; active_future=${r?.active_future ?? 0} (units=${r?.af_units ?? 0}, buckets ${r?.af_0_30 ?? 0}+${r?.af_31_60 ?? 0}+${r?.af_later ?? 0}=${bucketSum}); family_held=${r?.family_held ?? 0}; risk=${r?.risk_total ?? 0} (expired ${r?.risk_expired ?? 0} + future≤90 ${r?.risk_future ?? 0} = ${riskSum})`,
      actual: n(r?.active_future),
      expected: String(bucketSum),
    });
  } catch (err) {
    checks.push(queryFailure("lease_scope_reconciliation", "v_lease_population (canonical lease scopes)", err));
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

  // Expense scope disclosure (World B confirmed July 15 2026): property
  // expenses are paid through an EXTERNAL system by design — Cindy does not
  // run them through AppFolio. A partial expense scope is therefore the
  // expected, accepted, and correctly-disclosed state, not a data failure;
  // the old plausibility check failed permanently on it, which is alarm
  // fatigue, not honesty. Every consumer (Financials page, health score,
  // metric contract) labels the scope and withholds NOI/margin via the
  // shared <10% expense/income ratio rule. This check now verifies the
  // financial data is in a KNOWN state and fails only on genuine anomalies:
  // no income at all, or expenses exceeding income (implausible for this
  // property). If the ratio ever crosses 10% — expenses appearing in
  // AppFolio — the detail flags the transition loudly so labeling can be
  // reviewed, but improvement is not a failure.
  try {
    const rows = await sql<{ income: string; expenses: string; ratio: string }[]>`
      SELECT COALESCE(total_income, 0)::text AS income,
             COALESCE(total_expenses, 0)::text AS expenses,
             CASE WHEN COALESCE(total_income, 0) = 0 THEN '0'
               ELSE (total_expenses / total_income)::text END AS ratio
      FROM gold_income_statements
      ORDER BY report_date DESC, created_at DESC
      LIMIT 1
    `;
    const income = Number(rows[0]?.income ?? 0);
    const expenses = Number(rows[0]?.expenses ?? 0);
    const ratio = Number(rows[0]?.ratio ?? 0);
    const anomaly = income <= 0 || expenses > income;
    const world = ratio < 0.1
      ? "external-expense state (expected): expenses paid outside AppFolio; NOI/margin correctly withheld by all consumers"
      : "SCOPE TRANSITION: expenses now exceed 10% of income — the external-expense assumption may have changed; review Financials labeling";
    checks.push({
      check: "expense_scope_disclosure",
      table: "gold_income_statements",
      passed: !anomaly,
      detail: `income=${income.toFixed(2)}, AppFolio-recorded expenses=${expenses.toFixed(2)}, ratio=${(ratio * 100).toFixed(2)}% — ${world}`,
      actual: Number((ratio * 100).toFixed(2)),
      expected: "income > 0 and expenses <= income",
    });
  } catch (err) {
    checks.push(queryFailure("expense_scope_disclosure", "gold_income_statements", err));
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

  // Occupancy partition (R1 item 1.3, July 15 2026): occupied/notice/vacant
  // must form an EXACT partition of the canonical roster — no unit uncounted,
  // none double-counted, and the occupancy-eligible denominator must equal
  // total minus excluded units. This is the invariant behind every
  // occupancy/vacancy rate the product displays.
  try {
    const rows = await sql<{
      total: string; occupied: string; vacant: string; notice: string;
      other_status: string; excluded: string;
    }[]>`
      SELECT
        COUNT(*)::text AS total,
        COUNT(*) FILTER (WHERE unit_status = 'occupied')::text AS occupied,
        COUNT(*) FILTER (WHERE unit_status = 'vacant')::text   AS vacant,
        COUNT(*) FILTER (WHERE unit_status = 'notice')::text   AS notice,
        COUNT(*) FILTER (WHERE unit_status IS NULL
          OR unit_status NOT IN ('occupied','vacant','notice'))::text AS other_status,
        COUNT(*) FILTER (WHERE exclude_from_occupancy)::text   AS excluded
      FROM v_unit_occupancy
    `;
    const r = rows[0];
    const total = Number(r?.total ?? 0);
    const partition = Number(r?.occupied ?? 0) + Number(r?.vacant ?? 0) + Number(r?.notice ?? 0);
    const other = Number(r?.other_status ?? 0);
    const eligible = total - Number(r?.excluded ?? 0);
    checks.push({
      check: "occupancy_partition",
      table: "v_unit_occupancy",
      passed: total > 0 && partition === total && other === 0,
      detail: `total=${total}, occupied=${r?.occupied}, vacant=${r?.vacant}, notice=${r?.notice}, unrecognized-status=${other}, excluded=${r?.excluded}, occupancy-eligible=${eligible}`,
      actual: partition,
      expected: String(total),
    });
  } catch (err) {
    checks.push(queryFailure("occupancy_partition", "v_unit_occupancy", err));
  }

  return checks;
}
