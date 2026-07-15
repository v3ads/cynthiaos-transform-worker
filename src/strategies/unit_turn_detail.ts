// ── unit_turn_detail strategy ─────────────────────────────────────────────────
//
// Handles the AppFolio "unit_turn_detail" report type.
//
// AppFolio sends one row per unit turn with PascalCase fields:
//   { Unit, UnitId, Property, PropertyId, UnitTurnId,
//     MoveOutDate, TurnEndDate, ExpectedMoveInDate,
//     TotalDaysToComplete, TargetDaysToComplete,
//     TotalBilled, BillablesFromWorkOrders, LaborFromWorkOrders,
//     InventoryFromWorkOrders, PurchaseOrdersFromWorkOrders, Notes }
//
// Silver: normalises each turn into:
//   { unit_id, move_out_date, expected_move_in_date, turn_end_date,
//     days_to_complete, target_days, total_billed }
//
// Gold:   promotes each turn into gold_unit_turnover.
//         Idempotent via ON CONFLICT (bronze_report_id, unit_id, move_out_date).

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";
import { normalizeUnitId } from "../utils/normalize";

// ── Numeric helper ────────────────────────────────────────────────────────────

function toNum(v: unknown): number {
  if (typeof v === "number") return v;
  if (typeof v === "string") {
    const n = parseFloat(v.replace(/[^0-9.-]/g, ""));
    return isNaN(n) ? 0 : n;
  }
  return 0;
}

// ── Date normalisation helper ─────────────────────────────────────────────────
// Converts MM/DD/YYYY or YYYY-MM-DD to YYYY-MM-DD; returns null if unparseable.

function toDateStr(val: unknown): string | null {
  if (!val) return null;
  if (val instanceof Date) return val.toISOString().slice(0, 10);
  const s = String(val).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) {
    const [m, d, y] = s.split("/");
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) return s.slice(0, 10);
  return null;
}

// ── Gold row interface ────────────────────────────────────────────────────────

interface NormalizedUnitTurn {
  unit_id: string;
  move_out_date: string | null;
  expected_move_in_date: string | null;
  turn_end_date: string | null;
  days_to_complete: number | null;
  target_days: number | null;
  total_billed: number;
  status: "scheduled" | "in_progress" | "completed";
}

interface GoldUnitTurnover {
  id: string;
  bronze_report_id: string | null;
  unit_id: string;
  move_out_date: string | null;
  expected_move_in_date: string | null;
  turn_end_date: string | null;
  days_to_complete: number | null;
  target_days: number | null;
  total_billed: number;
  created_at: Date;
}

// ── Strategy ─────────────────────────────────────────────────────────────────

export const unitTurnDetailStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────

  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data;

    // AppFolio sends rows under raw.results
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];

    const reportDate = toDateStr(ctx.bronze.report_date) ?? new Date().toISOString().slice(0, 10);
    const eventMap = new Map<string, NormalizedUnitTurn>();

    for (const r of rows) {
      // AppFolio PascalCase fields; also support legacy snake_case.
      const rawUnit = String(r.Unit ?? r.unit ?? r.unit_id ?? r.unit_number ?? "");
      const unitId = normalizeUnitId(rawUnit);
      const moveOutDate = toDateStr(r.MoveOutDate ?? r.move_out_date ?? r.move_out);
      const expectedMoveInDate = toDateStr(
        r.ExpectedMoveInDate ?? r.expected_move_in_date ?? r.move_in_date
      );
      const turnEndDate = toDateStr(r.TurnEndDate ?? r.turn_end_date ?? r.turn_end);
      const rawDays = r.TotalDaysToComplete ?? r.days_to_complete;
      const parsedDays = rawDays == null ? null : toNum(rawDays);
      const daysToComplete = parsedDays !== null && parsedDays >= 0 ? parsedDays : null;
      const targetDays = r.TargetDaysToComplete != null
        ? toNum(r.TargetDaysToComplete)
        : r.target_days != null
        ? toNum(r.target_days)
        : null;
      const totalBilled = toNum(
        r.TotalBilled ?? r.BillablesFromWorkOrders ?? r.total_billed ?? r.billed ?? 0
      );
      const status = moveOutDate && moveOutDate > reportDate
        ? "scheduled"
        : turnEndDate && turnEndDate <= reportDate
        ? "completed"
        : "in_progress";

      // The source repeats identical physical events across report chunks and
      // daily snapshots. Keep one canonical row per full event signature.
      const eventKey = [
        unitId,
        moveOutDate ?? "",
        expectedMoveInDate ?? "",
        turnEndDate ?? "",
        daysToComplete ?? "",
      ].join("|");
      eventMap.set(eventKey, {
        unit_id: unitId,
        move_out_date: moveOutDate,
        expected_move_in_date: expectedMoveInDate,
        turn_end_date: turnEndDate,
        days_to_complete: daysToComplete,
        target_days: targetDays,
        total_billed: totalBilled,
        status,
      });
    }

    const normalizedRows = Array.from(eventMap.values());

    return {
      normalized_data: {
        source:           "appfolio",
        report_type:      ctx.bronze.report_type,
        report_date:      ctx.bronze.report_date,
        bronze_report_id: ctx.bronze.id,
        transformed_at:   new Date().toISOString(),
        source_row_count: rows.length,
        row_count:        normalizedRows.length,
        duplicate_rows_removed: rows.length - normalizedRows.length,
        rows:             normalizedRows,
        summary: {
          total_turns:          normalizedRows.length,
          avg_days_to_complete: normalizedRows.length > 0
            ? normalizedRows
                .filter((r) => r.days_to_complete !== null)
                .reduce((acc, r) => acc + (r.days_to_complete ?? 0), 0) /
              Math.max(1, normalizedRows.filter((r) => r.days_to_complete !== null).length)
            : null,
          total_billed:         normalizedRows.reduce((acc, r) => acc + r.total_billed, 0),
        },
      },
    };
  },

  // ── Gold promotion ────────────────────────────────────────────────────────

  async promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    const { sql, bronze, silver } = ctx;
    const nd = silver.normalized_data as any;
    const rows: Record<string, unknown>[] = Array.isArray(nd.rows) ? nd.rows : [];

    if (rows.length === 0) {
      return {
        gold_ids:    [],
        skipped:     true,
        skip_reason: `Silver record ${silver.id} has no rows in normalized_data`,
      };
    }

    const goldIds: string[] = [];

    // Constraint parity (July 15 2026): a unique constraint on
    // gold_unit_turnover was created out-of-band as uq_unit_turnover_unit_moveout
    // on (unit_id, move_out_date), which disagrees with this strategy's
    // ON CONFLICT target (bronze_report_id, unit_id, move_out_date). The
    // mismatch made cross-snapshot upserts throw a duplicate-key error that
    // 500ed the entire /gold/run pipeline (blocking the 4 newest work orders
    // and everything else behind them). Normalize the constraint to exactly
    // what the upsert targets. Idempotent; runs once per promotion cheaply.
    try {
      await sql`ALTER TABLE gold_unit_turnover DROP CONSTRAINT IF EXISTS uq_unit_turnover_unit_moveout`;
      await sql`
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'uq_unit_turnover_bronze_unit_moveout'
          ) THEN
            ALTER TABLE gold_unit_turnover
              ADD CONSTRAINT uq_unit_turnover_bronze_unit_moveout
              UNIQUE (bronze_report_id, unit_id, move_out_date);
          END IF;
        END $$;
      `;
    } catch (err) {
      console.warn(`[unit_turn] constraint normalization warning (non-fatal): ${err instanceof Error ? err.message : String(err)}`);
    }

    for (const row of rows) {
      const unitId             = normalizeUnitId(row.unit_id ?? "unknown");
      const moveOutDate        = row.move_out_date         as string | null ?? null;
      const expectedMoveInDate = row.expected_move_in_date as string | null ?? null;
      const turnEndDate        = row.turn_end_date         as string | null ?? null;
      const daysToComplete     = row.days_to_complete      != null ? Number(row.days_to_complete) : null;
      const targetDays         = row.target_days           != null ? Number(row.target_days) : null;
      const totalBilled        = typeof row.total_billed === "number" ? row.total_billed : 0;

      const inserted = await sql<GoldUnitTurnover[]>`
        INSERT INTO gold_unit_turnover (
          bronze_report_id,
          unit_id,
          event_type,
          move_out_date,
          expected_move_in_date,
          turn_end_date,
          days_to_complete,
          target_days,
          total_billed,
          created_at
        ) VALUES (
          ${bronze.id},
          ${unitId},
          ${'turn'},
          ${moveOutDate},
          ${expectedMoveInDate},
          ${turnEndDate},
          ${daysToComplete},
          ${targetDays},
          ${totalBilled},
          NOW()
        )
        ON CONFLICT (bronze_report_id, unit_id, move_out_date)
        DO UPDATE SET
          bronze_report_id      = EXCLUDED.bronze_report_id,
          expected_move_in_date = EXCLUDED.expected_move_in_date,
          turn_end_date         = EXCLUDED.turn_end_date,
          days_to_complete      = EXCLUDED.days_to_complete,
          target_days           = EXCLUDED.target_days,
          total_billed          = EXCLUDED.total_billed
        RETURNING id
      `;

      if (inserted.length > 0) {
        goldIds.push(inserted[0].id);
      }
    }

    // AppFolio's unit-turn report is a complete YTD snapshot. Previous code
    // accumulated every daily snapshot, producing hundreds of repeated physical
    // events. Delete prior `turn` snapshots only after the full current report
    // has been promoted; move-in/move-out event rows are separate and preserved.
    await sql`
      DELETE FROM gold_unit_turnover
      WHERE event_type = 'turn'
        AND bronze_report_id IS DISTINCT FROM ${bronze.id}
    `;

    // PostgreSQL UNIQUE constraints treat NULL values as distinct, so repeatedly
    // promoting the same Bronze report used to append duplicate undated events.
    // Collapse only exact physical signatures; different expected dates, end dates,
    // durations, targets, or billed amounts remain separate legitimate events.
    await sql`
      WITH ranked AS (
        SELECT id,
               ROW_NUMBER() OVER (
                 PARTITION BY bronze_report_id,
                              event_type,
                              unit_id,
                              COALESCE(move_out_date::text, ''),
                              COALESCE(expected_move_in_date::text, ''),
                              COALESCE(turn_end_date::text, ''),
                              COALESCE(days_to_complete::text, ''),
                              COALESCE(target_days::text, ''),
                              COALESCE(total_billed::text, '')
                 ORDER BY created_at DESC, id DESC
               ) AS duplicate_rank
        FROM gold_unit_turnover
        WHERE event_type = 'turn'
          AND bronze_report_id = ${bronze.id}
      )
      DELETE FROM gold_unit_turnover g
      USING ranked r
      WHERE g.id = r.id
        AND r.duplicate_rank > 1
    `;

    return { gold_ids: goldIds, skipped: false };
  },
};
