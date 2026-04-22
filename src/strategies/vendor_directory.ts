// ── vendor_directory strategy ─────────────────────────────────────────────────
//
// Handles the AppFolio "vendor_directory" report type.
//
// AppFolio sends one row per vendor with PascalCase fields including contact
// info, trade specialties, compliance expiry dates, and payment preferences.
//
// Silver: normalises each vendor into a consistent shape:
//   { vendor_id, company_name, first_name, last_name, email, phone_numbers,
//     vendor_type, vendor_trades, payment_type, terms,
//     liability_ins_expires, auto_ins_expires, workers_comp_expires,
//     state_lic_expires, epa_cert_expires, contract_expires,
//     do_not_use, send_1099, portal_activated }
//
// Gold:   promotes each vendor into gold_vendors.
//         Idempotent via ON CONFLICT (vendor_id).

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";

// ── Helpers ───────────────────────────────────────────────────────────────────

// Converts MM/DD/YYYY → YYYY-MM-DD; returns null if unparseable.
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

function toBool(val: unknown, trueVal = "Yes"): boolean {
  return String(val ?? "").trim().toLowerCase() === trueVal.toLowerCase();
}

// ── Strategy ─────────────────────────────────────────────────────────────────

export const vendorDirectoryStrategy: TransformStrategy = {

  // ── Silver normalisation ──────────────────────────────────────────────────

  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data;
    const rows = Array.isArray(raw.results)
      ? (raw.results as Record<string, unknown>[])
      : Array.isArray(raw.rows)
      ? (raw.rows as Record<string, unknown>[])
      : [];

    const normalizedRows = rows.map((r) => ({
      vendor_id:              r.VendorId ?? null,
      company_name:           r.CompanyName ?? null,
      first_name:             r.FirstName ?? null,
      last_name:              r.LastName ?? r.Name ?? null,
      email:                  r.Email ?? null,
      phone_numbers:          r.PhoneNumbers ?? null,
      vendor_type:            r.VendorType ?? null,
      vendor_trades:          r.VendorTrades ?? null,
      tags:                   r.Tags ?? null,
      payment_type:           r.PaymentType ?? null,
      terms:                  r.Terms ?? null,
      default_gl_account:     r.DefaultGLAccount ?? null,
      vendor_address:         r.VendorAddress ?? null,
      vendor_city:            r.VendorCity ?? null,
      vendor_state:           r.VendorState ?? null,
      vendor_zip:             r.VendorZip ?? null,
      liability_ins_expires:  toDateStr(r.LiabilityInsExpires),
      auto_ins_expires:       toDateStr(r.AutoInsExpires),
      workers_comp_expires:   toDateStr(r.WorkersCompExpires),
      state_lic_expires:      toDateStr(r.StateLicExpires),
      epa_cert_expires:       toDateStr(r.EPACertExpires),
      contract_expires:       toDateStr(r.ContractExpires),
      do_not_use:             toBool(r.DoNotUseForWorkOrder),
      send_1099:              toBool(r.Send1099),
      portal_activated:       toBool(r.PortalActivated),
    }));

    // Summary: count by vendor_type
    const byType: Record<string, number> = {};
    for (const row of normalizedRows) {
      const t = String(row.vendor_type ?? "Unknown");
      byType[t] = (byType[t] ?? 0) + 1;
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
          total_vendors:    normalizedRows.length,
          do_not_use_count: normalizedRows.filter((r) => r.do_not_use).length,
          by_type:          byType,
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
        INSERT INTO gold_vendors (
          bronze_report_id,
          report_date,
          vendor_id,
          company_name,
          first_name,
          last_name,
          email,
          phone_numbers,
          vendor_type,
          vendor_trades,
          tags,
          payment_type,
          terms,
          default_gl_account,
          vendor_address,
          vendor_city,
          vendor_state,
          vendor_zip,
          liability_ins_expires,
          auto_ins_expires,
          workers_comp_expires,
          state_lic_expires,
          epa_cert_expires,
          contract_expires,
          do_not_use,
          send_1099,
          portal_activated,
          created_at
        ) VALUES (
          ${bronze.id},
          ${ctx.reportDate},
          ${(row.vendor_id as number | null) ?? null},
          ${(row.company_name as string | null) ?? null},
          ${(row.first_name as string | null) ?? null},
          ${(row.last_name as string | null) ?? null},
          ${(row.email as string | null) ?? null},
          ${(row.phone_numbers as string | null) ?? null},
          ${(row.vendor_type as string | null) ?? null},
          ${(row.vendor_trades as string | null) ?? null},
          ${(row.tags as string | null) ?? null},
          ${(row.payment_type as string | null) ?? null},
          ${(row.terms as string | null) ?? null},
          ${(row.default_gl_account as string | null) ?? null},
          ${(row.vendor_address as string | null) ?? null},
          ${(row.vendor_city as string | null) ?? null},
          ${(row.vendor_state as string | null) ?? null},
          ${(row.vendor_zip as string | null) ?? null},
          ${(row.liability_ins_expires as string | null) ?? null},
          ${(row.auto_ins_expires as string | null) ?? null},
          ${(row.workers_comp_expires as string | null) ?? null},
          ${(row.state_lic_expires as string | null) ?? null},
          ${(row.epa_cert_expires as string | null) ?? null},
          ${(row.contract_expires as string | null) ?? null},
          ${(row.do_not_use as boolean) ?? false},
          ${(row.send_1099 as boolean) ?? false},
          ${(row.portal_activated as boolean) ?? false},
          NOW()
        )
        ON CONFLICT (vendor_id)
        DO UPDATE SET
          bronze_report_id      = EXCLUDED.bronze_report_id,
          report_date           = EXCLUDED.report_date,
          company_name          = EXCLUDED.company_name,
          first_name            = EXCLUDED.first_name,
          last_name             = EXCLUDED.last_name,
          email                 = EXCLUDED.email,
          phone_numbers         = EXCLUDED.phone_numbers,
          vendor_type           = EXCLUDED.vendor_type,
          vendor_trades         = EXCLUDED.vendor_trades,
          payment_type          = EXCLUDED.payment_type,
          liability_ins_expires = EXCLUDED.liability_ins_expires,
          auto_ins_expires      = EXCLUDED.auto_ins_expires,
          workers_comp_expires  = EXCLUDED.workers_comp_expires,
          state_lic_expires     = EXCLUDED.state_lic_expires,
          epa_cert_expires      = EXCLUDED.epa_cert_expires,
          contract_expires      = EXCLUDED.contract_expires,
          do_not_use            = EXCLUDED.do_not_use,
          portal_activated      = EXCLUDED.portal_activated
        RETURNING id
      `;

      if (inserted.length > 0) {
        goldIds.push(inserted[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
