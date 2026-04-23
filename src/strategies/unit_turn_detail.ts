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

    const normalizedRows = rows.map((r) => {
      // AppFolio PascalCase fields; also support legacy snake_case
      const rawUnit = String(r.Unit ?? r.unit ?? r.unit_id ?? r.unit_number ?? "");

      return {
        unit_id:               normalizeUnitId(rawUnit),
        move_out_date:         toDateStr(r.MoveOutDate         ?? r.move_out_date  ?? r.move_out),
        expected_move_in_date: toDateStr(r.ExpectedMoveInDate  ?? r.expected_move_in_date ?? r.move_in_date),
        turn_end_date:         toDateStr(r.TurnEndDate         ?? r.turn_end_date  ?? r.turn_end),
        days_to_complete:      (() => {
          const raw = r.TotalDaysToComplete ?? r.days_to_complete;
          if (raw == null) return null;
          const n = toNum(raw);
          // Negative means AppFolio used expected_move_in_date (future) — turn still in progress
          return n !== null && n >= 0 ? n : null;
        })(),
        target_days:           r.TargetDaysToComplete != null
          ? toNum(r.TargetDaysToComplete)
          : r.target_days != null
          ? toNum(r.target_days)
          : null,
        total_billed:          toNum(
          r.TotalBilled ?? r.BillablesFromWorkOrders ?? r.total_billed ?? r.billed ?? 0
        ),
      };
    });

    return {
      normalized_data: {
        source:           "appfolio",
        report_type:      ctx.bronze.report_type,
        report_date:      ctx.bronze.report_date,
        bronze_report_id: ctx.bronze.id,
        transformed_at:   new Date().toISOString(),
        row_count:        normalizedRows.length,
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

    for (const row of rows) {
      const unitId             = String(row.unit_id ?? "unknown");
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
        ON CONFLICT (unit_id, move_out_date)
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

    return { gold_ids: goldIds, skipped: false };
  },
};
