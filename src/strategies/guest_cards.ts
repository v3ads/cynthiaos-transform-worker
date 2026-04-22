// ── guest_cards strategy ──────────────────────────────────────────────────────
//
// Handles the AppFolio "guest_cards" report type (leasing prospects).
//
// AppFolio sends one row per prospect (guest card) with PascalCase fields
// covering contact info, unit interest, lead source, and activity history.
//
// Silver: normalises each row into a consistent prospect record:
//   { guest_card_id, guest_card_uuid, inquiry_id, prospect_name, email, phone,
//     status, lead_type, source, unit_id, bed_bath_preference, max_rent,
//     move_in_preference, received_at, last_activity_date, last_activity_type,
//     monthly_income, credit_score, pet_preference, assigned_user, tenant_id }
//
// Gold:   promotes each prospect into gold_prospects.
//         Idempotent via ON CONFLICT (guest_card_id).

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";
import { normalizeUnitId } from "../utils/normalize";

// ── Helpers ───────────────────────────────────────────────────────────────────

function toNum(v: unknown): number | null {
  if (v == null || v === "" || v === "--") return null;
  if (typeof v === "number") return v;
  const n = parseFloat(String(v).replace(/[^0-9.-]/g, ""));
  return isNaN(n) ? null : n;
}

function toInt(v: unknown): number | null {
  const n = toNum(v);
  return n !== null ? Math.round(n) : null;
}

function toDateStr(val: unknown): string | null {
  if (!val) return null;
  const s = String(val).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const mdyMatch = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (mdyMatch) {
    const [, m, d, y] = mdyMatch;
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }
  return null;
}

// "01/01/2026 at 02:04 AM" → ISO timestamp
function toTimestamp(val: unknown): string | null {
  if (!val) return null;
  const s = String(val).trim();
  const match = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+at\s+(\d{1,2}):(\d{2})\s*(AM|PM)/i);
  if (match) {
    const [, m, d, y, hRaw, min, ampm] = match;
    let h = parseInt(hRaw, 10);
    if (ampm.toUpperCase() === "PM" && h !== 12) h += 12;
    if (ampm.toUpperCase() === "AM" && h === 12) h = 0;
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}T${String(h).padStart(2, "0")}:${min}:00Z`;
  }
  return toDateStr(val) ? `${toDateStr(val)}T00:00:00Z` : null;
}

// ── Strategy ─────────────────────────────────────────────────────────────────

export const guestCardsStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────

  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data;
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];

    const normalizedRows = rows.map((r) => {
      const rawUnit = String(r.Unit ?? r.unit ?? "");
      return {
        guest_card_id:       r.GuestCardId ?? null,
        guest_card_uuid:     r.GuestCardUuid ?? null,
        inquiry_id:          r.InquiryID ?? null,
        prospect_name:       r.Name ?? null,
        email:               r.EmailAddress ?? r.Email ?? null,
        phone:               r.PhoneNumber ?? null,
        status:              r.Status ?? null,
        lead_type:           r.LeadType ?? null,
        source:              r.Source ?? null,
        unit_id:             rawUnit ? normalizeUnitId(rawUnit) : null,
        unit_name:           rawUnit || null,
        bed_bath_preference: r.BedBathPreference ?? null,
        max_rent:            toNum(r.MaxRent),
        move_in_preference:  toDateStr(r.MoveInPreference),
        received_at:         toTimestamp(r.Received),
        last_activity_date:  toDateStr(r.LastActivityDate),
        last_activity_type:  r.LastActivityType ?? null,
        monthly_income:      toNum(r.MonthlyIncome),
        credit_score:        toInt(r.CreditScore),
        pet_preference:      r.PetPreference ?? null,
        assigned_user:       r.AssignedUser ?? null,
        tenant_id:           r.TenantId ? String(r.TenantId) : null,
      };
    });

    // Summary: count by status
    const byStatus: Record<string, number> = {};
    for (const row of normalizedRows) {
      const s = String(row.status ?? "Unknown");
      byStatus[s] = (byStatus[s] ?? 0) + 1;
    }

    // Summary: count by source
    const bySource: Record<string, number> = {};
    for (const row of normalizedRows) {
      const s = String(row.source ?? "Unknown");
      bySource[s] = (bySource[s] ?? 0) + 1;
    }

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
          total_prospects: normalizedRows.length,
          active_count:    normalizedRows.filter((r) => r.status === "Active").length,
          by_status:       byStatus,
          by_source:       bySource,
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
      const inserted = await sql<{ id: string }[]>`
        INSERT INTO gold_prospects (
          bronze_report_id,
          report_date,
          guest_card_id,
          guest_card_uuid,
          inquiry_id,
          prospect_name,
          email,
          phone,
          status,
          lead_type,
          source,
          unit_id,
          unit_name,
          bed_bath_preference,
          max_rent,
          move_in_preference,
          received_at,
          last_activity_date,
          last_activity_type,
          monthly_income,
          credit_score,
          pet_preference,
          assigned_user,
          tenant_id,
          created_at
        ) VALUES (
          ${bronze.id},
          ${ctx.reportDate},
          ${(row.guest_card_id as number | null) ?? null},
          ${(row.guest_card_uuid as string | null) ?? null},
          ${(row.inquiry_id as number | null) ?? null},
          ${(row.prospect_name as string | null) ?? null},
          ${(row.email as string | null) ?? null},
          ${(row.phone as string | null) ?? null},
          ${(row.status as string | null) ?? null},
          ${(row.lead_type as string | null) ?? null},
          ${(row.source as string | null) ?? null},
          ${(row.unit_id as string | null) ?? null},
          ${(row.unit_name as string | null) ?? null},
          ${(row.bed_bath_preference as string | null) ?? null},
          ${(row.max_rent as number | null) ?? null},
          ${(row.move_in_preference as string | null) ?? null},
          ${(row.received_at as string | null) ?? null},
          ${(row.last_activity_date as string | null) ?? null},
          ${(row.last_activity_type as string | null) ?? null},
          ${(row.monthly_income as number | null) ?? null},
          ${(row.credit_score as number | null) ?? null},
          ${(row.pet_preference as string | null) ?? null},
          ${(row.assigned_user as string | null) ?? null},
          ${(row.tenant_id as string | null) ?? null},
          NOW()
        )
        ON CONFLICT (guest_card_id)
        DO UPDATE SET
          bronze_report_id    = EXCLUDED.bronze_report_id,
          report_date         = EXCLUDED.report_date,
          status              = EXCLUDED.status,
          last_activity_date  = EXCLUDED.last_activity_date,
          last_activity_type  = EXCLUDED.last_activity_type,
          unit_id             = EXCLUDED.unit_id,
          unit_name           = EXCLUDED.unit_name,
          assigned_user       = EXCLUDED.assigned_user,
          tenant_id           = EXCLUDED.tenant_id
        RETURNING id
      `;

      if (inserted.length > 0) {
        goldIds.push(inserted[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
