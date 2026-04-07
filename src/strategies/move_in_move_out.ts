// ── move_in_move_out strategy ─────────────────────────────────────────────────
//
// Handles the AppFolio "Move In / Move Out" report.
// Normalises tenant movement events and derives event_type.
//
// Supported payload shapes:
//   1. Rows array: [{ tenant_id, unit_id, move_in_date?, move_out_date? }, ...]
//   2. Separate arrays: { move_ins: [...], move_outs: [...] }
//
// Gold table: gold_unit_turnover (one row per event per tenant/unit)
// Idempotency: ON CONFLICT (bronze_report_id, tenant_id, unit_id, event_type) DO NOTHING

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

function toDateStr(val: unknown): string | null {
  if (!val) return null;
  if (val instanceof Date) return val.toISOString().slice(0, 10);
  const s = String(val).trim();
  if (!s || s === "null" || s === "undefined") return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) {
    const [m, d, y] = s.split("/");
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) return s.slice(0, 10);
  return null;
}

function normalizeTenantId(val: unknown): string {
  if (!val) return "unknown";
  return String(val).trim().toLowerCase().replace(/\s+/g, "_").replace(/[^a-z0-9_-]/g, "");
}

function normalizeUnitId(val: unknown): string {
  if (!val) return "unknown";
  return String(val).trim().toLowerCase().replace(/\s+/g, "_");
}

// ── Normalised event type ─────────────────────────────────────────────────────

type EventType = "move_in" | "move_out";

interface TurnoverEvent {
  tenant_id:     string;
  unit_id:       string;
  move_in_date:  string | null;
  move_out_date: string | null;
  event_type:    EventType;
}

// ── Extract events from a flat rows array ─────────────────────────────────────
//
// Each row may represent a move-in, a move-out, or both.
// We emit one event per direction that has a date.

function extractFromRows(rows: Record<string, unknown>[]): TurnoverEvent[] {
  const events: TurnoverEvent[] = [];

  for (const row of rows) {
    const tenantId = normalizeTenantId(
      row.tenant_id ?? row.tenant ?? row.name ?? row.resident
    );
    const unitId = normalizeUnitId(
      row.unit_id ?? row.unit ?? row.unit_number
    );

    const moveInDate  = toDateStr(row.move_in_date  ?? row.move_in  ?? row.date_in);
    const moveOutDate = toDateStr(row.move_out_date ?? row.move_out ?? row.date_out);

    // Emit move_in event if move_in_date is present
    if (moveInDate) {
      events.push({
        tenant_id:     tenantId,
        unit_id:       unitId,
        move_in_date:  moveInDate,
        move_out_date: null,
        event_type:    "move_in",
      });
    }

    // Emit move_out event if move_out_date is present
    if (moveOutDate) {
      events.push({
        tenant_id:     tenantId,
        unit_id:       unitId,
        move_in_date:  null,
        move_out_date: moveOutDate,
        event_type:    "move_out",
      });
    }
  }

  return events;
}

// ── Extract events from separate move_ins / move_outs arrays ─────────────────

function extractFromSeparateArrays(
  moveIns:  Record<string, unknown>[],
  moveOuts: Record<string, unknown>[]
): TurnoverEvent[] {
  const events: TurnoverEvent[] = [];

  for (const row of moveIns) {
    const tenantId = normalizeTenantId(
      row.tenant_id ?? row.tenant ?? row.name ?? row.resident
    );
    const unitId = normalizeUnitId(row.unit_id ?? row.unit ?? row.unit_number);
    const moveInDate = toDateStr(
      row.move_in_date ?? row.move_in ?? row.date ?? row.date_in
    );
    if (moveInDate) {
      events.push({
        tenant_id:     tenantId,
        unit_id:       unitId,
        move_in_date:  moveInDate,
        move_out_date: null,
        event_type:    "move_in",
      });
    }
  }

  for (const row of moveOuts) {
    const tenantId = normalizeTenantId(
      row.tenant_id ?? row.tenant ?? row.name ?? row.resident
    );
    const unitId = normalizeUnitId(row.unit_id ?? row.unit ?? row.unit_number);
    const moveOutDate = toDateStr(
      row.move_out_date ?? row.move_out ?? row.date ?? row.date_out
    );
    if (moveOutDate) {
      events.push({
        tenant_id:     tenantId,
        unit_id:       unitId,
        move_in_date:  null,
        move_out_date: moveOutDate,
        event_type:    "move_out",
      });
    }
  }

  return events;
}

// ── Strategy ─────────────────────────────────────────────────────────────────

export const moveInMoveOutStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────

  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data;

    let events: TurnoverEvent[] = [];

    // Shape 2: separate move_ins / move_outs arrays
    if (Array.isArray(raw.move_ins) || Array.isArray(raw.move_outs)) {
      const moveIns  = Array.isArray(raw.move_ins)  ? (raw.move_ins  as Record<string, unknown>[]) : [];
      const moveOuts = Array.isArray(raw.move_outs) ? (raw.move_outs as Record<string, unknown>[]) : [];
      events = extractFromSeparateArrays(moveIns, moveOuts);
    } else if (Array.isArray(raw.rows)) {
      // Shape 1: flat rows array
      events = extractFromRows(raw.rows as Record<string, unknown>[]);
    } else if (Array.isArray(raw.events)) {
      // Alternative: events array
      events = extractFromRows(raw.events as Record<string, unknown>[]);
    }

    return {
      normalized_data: {
        events,
        event_count: events.length,
      },
    };
  },

  // ── Gold promotion ────────────────────────────────────────────────────────

  async promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    const nd = ctx.silver.normalized_data as {
      events: TurnoverEvent[];
    };

    const events = nd.events ?? [];
    if (events.length === 0) {
      return { gold_ids: [], skipped: true, skip_reason: "no_events_extracted" };
    }

    const goldIds: string[] = [];

    for (const event of events) {
      const rows = await ctx.sql`
        INSERT INTO gold_unit_turnover (
          bronze_report_id,
          tenant_id,
          unit_id,
          move_in_date,
          move_out_date,
          event_type
        ) VALUES (
          ${ctx.bronze.id},
          ${event.tenant_id},
          ${event.unit_id},
          ${event.move_in_date},
          ${event.move_out_date},
          ${event.event_type}
        )
        ON CONFLICT (bronze_report_id, tenant_id, unit_id, event_type) DO NOTHING
        RETURNING id
      `;

      const inserted = (rows as unknown as { id: string }[]);
      if (inserted.length > 0) {
        goldIds.push(inserted[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
