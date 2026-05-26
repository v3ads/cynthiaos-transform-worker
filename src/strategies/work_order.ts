// ── work_order strategy ────────────────────────────────────────────────────────
//
// Handles the AppFolio "work_order" report type.
//
// AppFolio sends one row per work order with PascalCase fields. The YTD report
// is fetched daily, so each run contains the full current state of all work
// orders opened since Jan 1. The upsert key is WorkOrderId — status changes
// (e.g. Open → Completed) are automatically reflected on each daily sync.
//
// Silver: normalises each row into a compact maintenance record with snake_case
//   fields, parsed dates, and numeric amounts.
//
// Gold:   upserts each work order into gold_maintenance.
//         Idempotent via ON CONFLICT (work_order_id) DO UPDATE.
import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";

// ── Helpers ───────────────────────────────────────────────────────────────────

function toNum(v: unknown): number | null {
  if (v == null || v === "" || v === "--" || v === "None") return null;
  if (typeof v === "number") return v;
  const n = parseFloat(String(v).replace(/[^0-9.-]/g, ""));
  return isNaN(n) ? null : n;
}

function toInt(v: unknown): number | null {
  const n = toNum(v);
  return n == null ? null : Math.round(n);
}

// Converts MM/DD/YYYY → YYYY-MM-DD string (null if blank/None)
function toDateStr(val: unknown): string | null {
  if (!val || val === "None" || val === "") return null;
  const s = String(val).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const mdyMatch = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (mdyMatch) {
    const [, m, d, y] = mdyMatch;
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }
  return null;
}

function toBool(v: unknown): boolean | null {
  if (v == null || v === "None" || v === "") return null;
  if (typeof v === "boolean") return v;
  const s = String(v).trim().toLowerCase();
  if (s === "yes" || s === "true" || s === "1") return true;
  if (s === "no"  || s === "false" || s === "0") return false;
  return null;
}

function toStr(v: unknown): string | null {
  if (v == null || v === "None" || v === "") return null;
  return String(v).trim() || null;
}

// ── Strategy ──────────────────────────────────────────────────────────────────

export const workOrderStrategy: TransformStrategy = {

  // ── Silver normalisation ────────────────────────────────────────────────────
  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data;
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];

    const normalizedRows = rows.map((r) => ({
      work_order_id:           toInt(r.WorkOrderId),
      work_order_number:       toStr(r.WorkOrderNumber),
      service_request_id:      toInt(r.ServiceRequestId),
      service_request_number:  toStr(r.ServiceRequestNumber),

      unit_id:                 toStr(r.UnitId),
      unit_name:               toStr(r.UnitName),
      property_id:             toStr(r.PropertyId),
      property_name:           toStr(r.PropertyName),
      unit_address:            toStr(r.UnitAddress),
      occupancy_id:            toInt(r.OccupancyId),

      status:                  toStr(r.Status),
      priority:                toStr(r.Priority),
      work_order_type:         toStr(r.WorkOrderType),
      work_order_issue:        toStr(r.WorkOrderIssue),

      job_description:         toStr(r.JobDescription),
      instructions:            toStr(r.Instructions),
      status_notes:            toStr(r.StatusNotes),

      created_by:              toStr(r.CreatedBy),
      assigned_user:           toStr(r.AssignedUser),
      vendor:                  toStr(r.Vendor),
      vendor_id:               toStr(r.vendor_id ?? r.VendorId),
      primary_tenant:          toStr(r.PrimaryTenant),
      requesting_tenant:       toStr(r.RequestingTenant),
      primary_tenant_email:    toStr(r.PrimaryTenantEmail),
      primary_tenant_phone:    toStr(r.PrimaryTenantPhoneNumber),
      submitted_by_tenant:     toBool(r.SubmittedByTenant),

      created_at_appfolio:     toDateStr(r.CreatedAt),
      work_done_on:            toDateStr(r.WorkDoneOn),
      completed_on:            toDateStr(r.CompletedOn),
      canceled_on:             toDateStr(r.CanceledOn),
      follow_up_on:            toDateStr(r.FollowUpOn),
      scheduled_start:         toDateStr(r.ScheduledStart),
      scheduled_end:           toDateStr(r.ScheduledEnd),
      estimated_on:            toDateStr(r.EstimatedOn),
      last_billed_on:          toDateStr(r.LastBilledOn),

      amount:                  toNum(r.Amount),
      markup_amount:           toNum(r.MarkupAmount),
      discount_amount:         toNum(r.DiscountAmount),
      estimate_amount:         toNum(r.EstimateAmount),
      vendor_bill_amount:      toNum(r.VendorBillAmount),
      vendor_charge_amount:    toNum(r.VendorChargeAmount),
      corporate_charge_amount: toNum(r.CorporateChargeAmount),
      tenant_total_charge_amt: toNum(r.TenantTotalChargeAmount),
      maintenance_limit:       toNum(r.MaintenanceLimit),

      recurring:               toBool(r.Recurring),
      unit_turn_id:            toInt(r.UnitTurnId),
      unit_turn_category:      toStr(r.UnitTurnCategory),
      inspection_id:           toInt(r.InspectionId),
      inspection_date:         toDateStr(r.InspectionDate),
      survey_id:               toInt(r.SurveyId),
      vendor_portal_invoices:  toInt(r.VendorPortalInvoices),
    }));

    return { normalized_data: { rows: normalizedRows } };
  },

  // ── Gold promotion ──────────────────────────────────────────────────────────
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
      // Skip rows with no work_order_id (shouldn't happen but guard anyway)
      if (row.work_order_id == null) continue;

      const inserted = await sql<{ id: string }[]>`
        INSERT INTO gold_maintenance (
          bronze_report_id,
          report_date,
          work_order_id,
          work_order_number,
          service_request_id,
          service_request_number,
          unit_id,
          unit_name,
          property_id,
          property_name,
          unit_address,
          occupancy_id,
          status,
          priority,
          work_order_type,
          work_order_issue,
          job_description,
          instructions,
          status_notes,
          created_by,
          assigned_user,
          vendor,
          vendor_id,
          primary_tenant,
          requesting_tenant,
          primary_tenant_email,
          primary_tenant_phone,
          submitted_by_tenant,
          created_at_appfolio,
          work_done_on,
          completed_on,
          canceled_on,
          follow_up_on,
          scheduled_start,
          scheduled_end,
          estimated_on,
          last_billed_on,
          amount,
          markup_amount,
          discount_amount,
          estimate_amount,
          vendor_bill_amount,
          vendor_charge_amount,
          corporate_charge_amount,
          tenant_total_charge_amt,
          maintenance_limit,
          recurring,
          unit_turn_id,
          unit_turn_category,
          inspection_id,
          inspection_date,
          survey_id,
          vendor_portal_invoices,
          promoted_at,
          updated_at
        ) VALUES (
          ${bronze.id},
          ${ctx.reportDate},
          ${row.work_order_id as number},
          ${(row.work_order_number as string | null) ?? null},
          ${(row.service_request_id as number | null) ?? null},
          ${(row.service_request_number as string | null) ?? null},
          ${(row.unit_id as string | null) ?? null},
          ${(row.unit_name as string | null) ?? null},
          ${(row.property_id as string | null) ?? null},
          ${(row.property_name as string | null) ?? null},
          ${(row.unit_address as string | null) ?? null},
          ${(row.occupancy_id as number | null) ?? null},
          ${(row.status as string | null) ?? null},
          ${(row.priority as string | null) ?? null},
          ${(row.work_order_type as string | null) ?? null},
          ${(row.work_order_issue as string | null) ?? null},
          ${(row.job_description as string | null) ?? null},
          ${(row.instructions as string | null) ?? null},
          ${(row.status_notes as string | null) ?? null},
          ${(row.created_by as string | null) ?? null},
          ${(row.assigned_user as string | null) ?? null},
          ${(row.vendor as string | null) ?? null},
          ${(row.vendor_id as string | null) ?? null},
          ${(row.primary_tenant as string | null) ?? null},
          ${(row.requesting_tenant as string | null) ?? null},
          ${(row.primary_tenant_email as string | null) ?? null},
          ${(row.primary_tenant_phone as string | null) ?? null},
          ${(row.submitted_by_tenant as boolean | null) ?? null},
          ${(row.created_at_appfolio as string | null) ?? null},
          ${(row.work_done_on as string | null) ?? null},
          ${(row.completed_on as string | null) ?? null},
          ${(row.canceled_on as string | null) ?? null},
          ${(row.follow_up_on as string | null) ?? null},
          ${(row.scheduled_start as string | null) ?? null},
          ${(row.scheduled_end as string | null) ?? null},
          ${(row.estimated_on as string | null) ?? null},
          ${(row.last_billed_on as string | null) ?? null},
          ${(row.amount as number | null) ?? null},
          ${(row.markup_amount as number | null) ?? null},
          ${(row.discount_amount as number | null) ?? null},
          ${(row.estimate_amount as number | null) ?? null},
          ${(row.vendor_bill_amount as number | null) ?? null},
          ${(row.vendor_charge_amount as number | null) ?? null},
          ${(row.corporate_charge_amount as number | null) ?? null},
          ${(row.tenant_total_charge_amt as number | null) ?? null},
          ${(row.maintenance_limit as number | null) ?? null},
          ${(row.recurring as boolean | null) ?? null},
          ${(row.unit_turn_id as number | null) ?? null},
          ${(row.unit_turn_category as string | null) ?? null},
          ${(row.inspection_id as number | null) ?? null},
          ${(row.inspection_date as string | null) ?? null},
          ${(row.survey_id as number | null) ?? null},
          ${(row.vendor_portal_invoices as number | null) ?? null},
          NOW(),
          NOW()
        )
        ON CONFLICT (work_order_id) DO UPDATE SET
          bronze_report_id        = EXCLUDED.bronze_report_id,
          report_date             = EXCLUDED.report_date,
          status                  = EXCLUDED.status,
          priority                = EXCLUDED.priority,
          work_order_type         = EXCLUDED.work_order_type,
          work_order_issue        = EXCLUDED.work_order_issue,
          job_description         = EXCLUDED.job_description,
          instructions            = EXCLUDED.instructions,
          status_notes            = EXCLUDED.status_notes,
          assigned_user           = EXCLUDED.assigned_user,
          vendor                  = EXCLUDED.vendor,
          vendor_id               = EXCLUDED.vendor_id,
          primary_tenant          = EXCLUDED.primary_tenant,
          requesting_tenant       = EXCLUDED.requesting_tenant,
          primary_tenant_email    = EXCLUDED.primary_tenant_email,
          primary_tenant_phone    = EXCLUDED.primary_tenant_phone,
          work_done_on            = EXCLUDED.work_done_on,
          completed_on            = EXCLUDED.completed_on,
          canceled_on             = EXCLUDED.canceled_on,
          follow_up_on            = EXCLUDED.follow_up_on,
          scheduled_start         = EXCLUDED.scheduled_start,
          scheduled_end           = EXCLUDED.scheduled_end,
          amount                  = EXCLUDED.amount,
          markup_amount           = EXCLUDED.markup_amount,
          discount_amount         = EXCLUDED.discount_amount,
          estimate_amount         = EXCLUDED.estimate_amount,
          vendor_bill_amount      = EXCLUDED.vendor_bill_amount,
          vendor_charge_amount    = EXCLUDED.vendor_charge_amount,
          corporate_charge_amount = EXCLUDED.corporate_charge_amount,
          tenant_total_charge_amt = EXCLUDED.tenant_total_charge_amt,
          unit_turn_id            = EXCLUDED.unit_turn_id,
          unit_turn_category      = EXCLUDED.unit_turn_category,
          inspection_id           = EXCLUDED.inspection_id,
          inspection_date         = EXCLUDED.inspection_date,
          vendor_portal_invoices  = EXCLUDED.vendor_portal_invoices,
          updated_at              = NOW()
        RETURNING id
      `;
      if (inserted.length > 0) {
        goldIds.push(inserted[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
