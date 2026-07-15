import type postgres from "postgres";

// Generates system actions into the shared `actions` table after each Gold
// promotion (Release 2, plan item 2.2). Every action is idempotent on
// natural_key, so re-promotions UPDATE the live facts (impact, due date,
// detail) rather than duplicating. Actions whose underlying condition has
// cleared are auto-resolved: any open system action of a managed type whose
// natural_key is absent from the current generation is marked done with a
// 'condition_cleared' event, so the queue reflects reality without manual
// cleanup.
//
// Owner defaults to Cindy (decision 5 — this is Cindy's system, no roles).

const MANAGED_TYPES = [
  "renewal_due",
  "holdover",
  "stale_closeout",
  "overdue_turn",
  "broken_promise",
] as const;

export interface ActionGenResult {
  generated: number;
  resolved: number;
  by_type: Record<string, number>;
}

export async function generateSystemActions(
  sql: postgres.Sql
): Promise<ActionGenResult> {
  const seen = new Set<string>();
  const byType: Record<string, number> = {};

  // Helper: upsert one system action, tracking its natural_key as still-live.
  const upsert = async (a: {
    natural_key: string;
    type: string;
    entity_type: string;
    entity_id: string;
    title: string;
    detail: string | null;
    priority: string;
    due_at: string | null;
    impact_label: string | null;
    next_action: string | null;
  }) => {
    seen.add(a.natural_key);
    byType[a.type] = (byType[a.type] ?? 0) + 1;
    await sql`
      INSERT INTO actions (natural_key, source, type, entity_type, entity_id,
        title, detail, owner, priority, due_at, impact_label, next_action,
        source_freshness, confidence, updated_at)
      VALUES (${a.natural_key}, 'system', ${a.type}, ${a.entity_type}, ${a.entity_id},
        ${a.title}, ${a.detail}, 'Cindy', ${a.priority}, ${a.due_at},
        ${a.impact_label}, ${a.next_action}, NOW(), 'trusted', NOW())
      ON CONFLICT (natural_key) DO UPDATE SET
        title = EXCLUDED.title,
        detail = EXCLUDED.detail,
        priority = EXCLUDED.priority,
        due_at = EXCLUDED.due_at,
        impact_label = EXCLUDED.impact_label,
        next_action = EXCLUDED.next_action,
        source_freshness = EXCLUDED.source_freshness,
        -- Never resurrect an action the user has resolved: if they marked it
        -- done or dismissed, a still-live condition leaves it resolved.
        status = CASE WHEN actions.status IN ('done','dismissed')
                      THEN actions.status ELSE actions.status END,
        updated_at = NOW()
    `;
  };

  // ── renewal_due: actionable renewals within 90 days ──────────────────────
  const renewals = await sql<{
    unit_id: string; tenant_name: string | null; days: string;
    lease_end_date: string | null; monthly_rent: string | null;
  }[]>`
    SELECT unit_id, tenant_name, days_until_expiration::text AS days,
           lease_end_date::text AS lease_end_date, monthly_rent::text AS monthly_rent
    FROM v_lease_population
    WHERE is_soonest_future_for_unit AND NOT is_superseded
      AND NOT is_released AND NOT is_family_held AND NOT is_employee_held
      AND days_until_expiration <= 90
  `;
  for (const r of renewals) {
    const rent = r.monthly_rent ? parseFloat(r.monthly_rent) : null;
    await upsert({
      natural_key: `renewal_due:${r.unit_id}:${r.lease_end_date}`,
      type: "renewal_due",
      entity_type: "unit",
      entity_id: r.unit_id,
      title: `Renewal decision: unit ${r.unit_id}`,
      detail: `${r.tenant_name ?? "Tenant"} — lease ends ${r.lease_end_date} (${r.days} days).`,
      priority: Number(r.days) <= 30 ? "high" : "normal",
      due_at: r.lease_end_date,
      impact_label: rent ? `$${rent.toLocaleString()}/mo` : null,
      next_action: "Contact tenant to confirm renewal or notice.",
    });
  }

  // ── holdover: expired, still occupied, no renewal/re-lease ────────────────
  const holdovers = await sql<{ unit_id: string; tenant_name: string | null; lease_end_date: string | null }[]>`
    SELECT unit_id, tenant_name, lease_end_date::text AS lease_end_date
    FROM v_lease_population
    WHERE is_holdover AND NOT is_family_held AND NOT is_employee_held
  `;
  for (const h of holdovers) {
    await upsert({
      natural_key: `holdover:${h.unit_id}`,
      type: "holdover",
      entity_type: "unit",
      entity_id: h.unit_id,
      title: `Holdover: unit ${h.unit_id}`,
      detail: `${h.tenant_name ?? "Tenant"} occupying past lease end (${h.lease_end_date}) with no renewal on file.`,
      priority: "high",
      due_at: null,
      impact_label: null,
      next_action: "Confirm month-to-month status or process the renewal.",
    });
  }

  // ── stale_closeout: expired, now vacant, lease record still open ──────────
  const closeouts = await sql<{ unit_id: string; lease_end_date: string | null }[]>`
    SELECT unit_id, lease_end_date::text AS lease_end_date
    FROM v_lease_population
    WHERE is_stale_closeout AND NOT is_family_held AND NOT is_employee_held
  `;
  for (const c of closeouts) {
    await upsert({
      natural_key: `stale_closeout:${c.unit_id}`,
      type: "stale_closeout",
      entity_type: "unit",
      entity_id: c.unit_id,
      title: `Close stale lease: unit ${c.unit_id}`,
      detail: `Lease expired ${c.lease_end_date}; unit now vacant with the lease record still open.`,
      priority: "normal",
      due_at: null,
      impact_label: null,
      next_action: "Close the lease record and route the unit to the turn/vacancy workflow.",
    });
  }

  // ── overdue_turn: turn in progress past its target days ───────────────────
  const overdueTurns = await sql<{ unit_id: string; move_out_date: string | null; target: string | null }[]>`
    SELECT unit_id, move_out_date::text AS move_out_date, target_days::text AS target
    FROM gold_unit_turnover
    WHERE move_out_date IS NOT NULL
      AND move_out_date::date <= CURRENT_DATE
      AND turn_end_date IS NULL AND days_to_complete IS NULL
      AND (CURRENT_DATE - move_out_date::date) > COALESCE(target_days, 10)
  `;
  for (const t of overdueTurns) {
    await upsert({
      natural_key: `overdue_turn:${t.unit_id}:${t.move_out_date}`,
      type: "overdue_turn",
      entity_type: "unit",
      entity_id: t.unit_id,
      title: `Overdue turn: unit ${t.unit_id}`,
      detail: `Moved out ${t.move_out_date}; turn exceeds the ${t.target ?? 10}-day target and is not complete.`,
      priority: "high",
      due_at: null,
      impact_label: "Lost rent accruing",
      next_action: "Check turn status with the vendor; expedite make-ready.",
    });
  }

  // ── Auto-resolve managed-type actions whose condition has cleared ─────────
  // Any open system action of a managed type NOT regenerated this run has had
  // its underlying condition resolve — mark done with an audit event.
  const seenArr = Array.from(seen);
  const stale = await sql<{ action_id: string; natural_key: string }[]>`
    SELECT action_id, natural_key FROM actions
    WHERE source = 'system'
      AND status IN ('open','in_progress','snoozed')
      AND type = ANY(${MANAGED_TYPES as unknown as string[]})
      AND (natural_key IS NULL OR natural_key <> ALL(${seenArr.length ? seenArr : ['__none__']}))
  `;
  for (const s of stale) {
    await sql`UPDATE actions SET status = 'done', completed_at = NOW(), updated_at = NOW() WHERE action_id = ${s.action_id}`;
    await sql`
      INSERT INTO action_events (action_id, from_status, to_status, note, actor)
      VALUES (${s.action_id}, 'open', 'done', 'condition_cleared (auto-resolved by pipeline)', 'system')
    `;
  }

  return { generated: seen.size, resolved: stale.length, by_type: byType };
}
