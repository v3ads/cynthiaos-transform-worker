// ── rental_applications strategy ──────────────────────────────────────────────
//
// Handles the AppFolio "rental_applications" report type.
//
// AppFolio sends one row per applicant with PascalCase fields including full
// personal, employment, and prior-residence history.
//
// Silver: normalises each row into a compact application record:
//   { rental_application_id, applicant_name, email, phone, unit_id,
//     status, application_status, received_date, screened_on,
//     desired_move_in, lease_start_date, lease_end_date, monthly_rent,
//     monthly_salary, source, assigned_user }
//
// Gold:   promotes each application into gold_rental_applications.
//         Idempotent via ON CONFLICT (rental_application_id).

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";
import { normalizeTenantId, normalizeUnitId } from "../utils/normalize";

// ── Helpers ───────────────────────────────────────────────────────────────────

function toNum(v: unknown): number | null {
  if (v == null || v === "" || v === "--") return null;
  if (typeof v === "number") return v;
  const n = parseFloat(String(v).replace(/[^0-9.-]/g, ""));
  return isNaN(n) ? null : n;
}

// Converts MM/DD/YYYY, YYYY-MM-DD, or "MM/DD/YYYY at HH:MM AM/PM" → YYYY-MM-DD
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

// Converts "MM/DD/YYYY at HH:MM AM/PM" → ISO timestamp string
function toTimestamp(val: unknown): string | null {
  if (!val) return null;
  const s = String(val).trim();
  // "03/23/2026 at 11:12 AM"
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

export const rentalApplicationsStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────

  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data;
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];

    const normalizedRows = rows.map((r) => {
      const rawUnit = String(r.ApplyingFor ?? r.UnitName ?? r.UnitId ?? r.unit ?? "");
      return {
        rental_application_id:       r.RentalApplicationId ?? null,
        rental_application_group_id: r.RentalApplicationGroupId ?? null,
        integration_id:              r.RentalApplicationIntegrationId ?? null,
        applicant_name:              r.Applicants ?? r.Name ?? null,
        email:                       r.Email ?? null,
        phone:                       r.PhoneNumber ?? null,
        unit_id:                     normalizeUnitId(rawUnit),
        unit_name:                   rawUnit || null,
        property_name:               r.PropertyName ?? null,
        status:                      r.Status ?? null,
        application_status:          r.ApplicationStatus ?? null,
        received_date:               toDateStr(r.Received),
        screened_on:                 toDateStr(r.ScreenedOn),
        decision_made_at:            toTimestamp(r.DecisionMadeAt),
        time_to_conversion_days:     toNum(r.TimeToConversion),
        desired_move_in:             toDateStr(r.DesiredMoveIn ?? r.MoveInDate),
        lease_start_date:            toDateStr(r.LeaseStartDate),
        lease_end_date:              toDateStr(r.LeaseEndDate),
        monthly_rent:                toNum(r.CurrentMonthlyRent),
        monthly_salary:              toNum(r.MonthlySalary),
        admin_fee_paid:              r.AdminFeePaid === "Yes",
        application_fee_paid:        r.ApplicationFeePaid === "Yes",
        source:                      r.ApplicantReportedSource ?? r.CampaignTitle ?? null,
        assigned_user:               r.AssignedUser ?? null,
      };
    });

    const byStatus: Record<string, number> = {};
    for (const row of normalizedRows) {
      const s = String(row.status ?? "Unknown");
      byStatus[s] = (byStatus[s] ?? 0) + 1;
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
          total_applications: normalizedRows.length,
          by_status:          byStatus,
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
        INSERT INTO gold_rental_applications (
          bronze_report_id,
          report_date,
          rental_application_id,
          rental_application_group_id,
          integration_id,
          applicant_name,
          email,
          phone,
          unit_id,
          unit_name,
          property_name,
          status,
          application_status,
          received_date,
          screened_on,
          decision_made_at,
          time_to_conversion_days,
          desired_move_in,
          lease_start_date,
          lease_end_date,
          monthly_rent,
          monthly_salary,
          admin_fee_paid,
          application_fee_paid,
          source,
          assigned_user,
          created_at
        ) VALUES (
          ${bronze.id},
          ${ctx.reportDate},
          ${(row.rental_application_id as number | null) ?? null},
          ${(row.rental_application_group_id as number | null) ?? null},
          ${(row.integration_id as string | null) ?? null},
          ${(row.applicant_name as string | null) ?? null},
          ${(row.email as string | null) ?? null},
          ${(row.phone as string | null) ?? null},
          ${(row.unit_id as string) ?? "unknown"},
          ${(row.unit_name as string | null) ?? null},
          ${(row.property_name as string | null) ?? null},
          ${(row.status as string | null) ?? null},
          ${(row.application_status as string | null) ?? null},
          ${(row.received_date as string | null) ?? null},
          ${(row.screened_on as string | null) ?? null},
          ${(row.decision_made_at as string | null) ?? null},
          ${(row.time_to_conversion_days as number | null) ?? null},
          ${(row.desired_move_in as string | null) ?? null},
          ${(row.lease_start_date as string | null) ?? null},
          ${(row.lease_end_date as string | null) ?? null},
          ${(row.monthly_rent as number | null) ?? null},
          ${(row.monthly_salary as number | null) ?? null},
          ${(row.admin_fee_paid as boolean) ?? false},
          ${(row.application_fee_paid as boolean) ?? false},
          ${(row.source as string | null) ?? null},
          ${(row.assigned_user as string | null) ?? null},
          NOW()
        )
        ON CONFLICT (rental_application_id)
        DO UPDATE SET
          bronze_report_id            = EXCLUDED.bronze_report_id,
          report_date                 = EXCLUDED.report_date,
          status                      = EXCLUDED.status,
          application_status          = EXCLUDED.application_status,
          decision_made_at            = EXCLUDED.decision_made_at,
          time_to_conversion_days     = EXCLUDED.time_to_conversion_days,
          lease_start_date            = EXCLUDED.lease_start_date,
          lease_end_date              = EXCLUDED.lease_end_date,
          monthly_rent                = EXCLUDED.monthly_rent,
          assigned_user               = EXCLUDED.assigned_user
        RETURNING id
      `;

      if (inserted.length > 0) {
        goldIds.push(inserted[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
