// ── general_ledger strategy ───────────────────────────────────────────────────
//
// Handles the AppFolio "general_ledger" report type.
//
// AppFolio sends one row per GL transaction detail line with PascalCase fields.
// Each row represents a single debit or credit entry against a GL account.
//
// Silver: normalises each row into a consistent accounting record:
//   { txn_id, txn_detail_id, post_date, deposit_date, txn_type,
//     gl_account_id, gl_account_name, bank_account,
//     debit, credit, party_id, party_name, party_type,
//     unit_id, description, reference, year, month_label, quarter_label }
//
// Gold:   promotes each entry into gold_general_ledger.
//         Idempotent via ON CONFLICT (txn_detail_id).

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

// "01/01/2026 at 08:07 AM" → ISO timestamp
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

export const generalLedgerStrategy: TransformStrategy = {

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
      const debit   = toNum(r.Debit);
      const credit  = toNum(r.Credit);
      return {
        txn_id:            r.TxnId ?? null,
        txn_detail_id:     r.TxnDetailId ?? null,
        integration_id:    r.TxnDetailIntegrationId ?? r.TxnIntegrationId ?? null,
        post_date:         toDateStr(r.PostDate),
        deposit_date:      toDateStr(r.DepositDate),
        txn_created_at:    toTimestamp(r.TxnCreatedAt),
        txn_updated_at:    toTimestamp(r.TxnUpdatedAt),
        txn_type:          r.Type ?? null,
        gl_account_id:     r.GlAccountId ?? null,
        gl_account_name:   r.GlAccountName ?? null,
        bank_account:      r.BankAccount ?? null,
        debit,
        credit,
        party_id:          r.PartyId ?? null,
        party_name:        r.PartyName ?? null,
        party_type:        r.PartyType ?? null,
        unit_id:           rawUnit ? normalizeUnitId(rawUnit) : null,
        unit_name:         rawUnit || null,
        property_name:     r.PropertyName ?? null,
        description:       r.Description ?? null,
        reference:         r.Reference ?? null,
        deposit_number:    r.DepositNumber ?? null,
        year:              r.Year ? parseInt(String(r.Year), 10) : null,
        month_label:       r.Month ?? null,
        quarter_label:     r.Quarter ?? null,
      };
    });

    // Summary: total debits, credits, net by GL account
    const accountSummary: Record<string, { debit: number; credit: number }> = {};
    for (const row of normalizedRows) {
      const acct = String(row.gl_account_name ?? "Unknown");
      if (!accountSummary[acct]) accountSummary[acct] = { debit: 0, credit: 0 };
      accountSummary[acct].debit  += row.debit  ?? 0;
      accountSummary[acct].credit += row.credit ?? 0;
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
          total_entries:  normalizedRows.length,
          total_debits:   normalizedRows.reduce((s, r) => s + (r.debit ?? 0), 0),
          total_credits:  normalizedRows.reduce((s, r) => s + (r.credit ?? 0), 0),
          account_count:  Object.keys(accountSummary).length,
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
        INSERT INTO gold_general_ledger (
          bronze_report_id,
          report_date,
          txn_id,
          txn_detail_id,
          integration_id,
          post_date,
          deposit_date,
          txn_created_at,
          txn_updated_at,
          txn_type,
          gl_account_id,
          gl_account_name,
          bank_account,
          debit,
          credit,
          party_id,
          party_name,
          party_type,
          unit_id,
          unit_name,
          property_name,
          description,
          reference,
          deposit_number,
          year,
          month_label,
          quarter_label,
          created_at
        ) VALUES (
          ${bronze.id},
          ${ctx.reportDate},
          ${(row.txn_id as number | null) ?? null},
          ${(row.txn_detail_id as number | null) ?? null},
          ${(row.integration_id as string | null) ?? null},
          ${(row.post_date as string | null) ?? null},
          ${(row.deposit_date as string | null) ?? null},
          ${(row.txn_created_at as string | null) ?? null},
          ${(row.txn_updated_at as string | null) ?? null},
          ${(row.txn_type as string | null) ?? null},
          ${(row.gl_account_id as number | null) ?? null},
          ${(row.gl_account_name as string | null) ?? null},
          ${(row.bank_account as string | null) ?? null},
          ${(row.debit as number | null) ?? null},
          ${(row.credit as number | null) ?? null},
          ${(row.party_id as number | null) ?? null},
          ${(row.party_name as string | null) ?? null},
          ${(row.party_type as string | null) ?? null},
          ${(row.unit_id as string | null) ?? null},
          ${(row.unit_name as string | null) ?? null},
          ${(row.property_name as string | null) ?? null},
          ${(row.description as string | null) ?? null},
          ${(row.reference as string | null) ?? null},
          ${(row.deposit_number as string | null) ?? null},
          ${(row.year as number | null) ?? null},
          ${(row.month_label as string | null) ?? null},
          ${(row.quarter_label as string | null) ?? null},
          NOW()
        )
        ON CONFLICT (txn_detail_id)
        DO UPDATE SET
          bronze_report_id  = EXCLUDED.bronze_report_id,
          report_date       = EXCLUDED.report_date,
          post_date         = EXCLUDED.post_date,
          deposit_date      = EXCLUDED.deposit_date,
          txn_type          = EXCLUDED.txn_type,
          gl_account_name   = EXCLUDED.gl_account_name,
          debit             = EXCLUDED.debit,
          credit            = EXCLUDED.credit,
          party_name        = EXCLUDED.party_name,
          description       = EXCLUDED.description
        RETURNING id
      `;

      if (inserted.length > 0) {
        goldIds.push(inserted[0].id);
      }
    }

    return { gold_ids: goldIds, skipped: false };
  },
};
