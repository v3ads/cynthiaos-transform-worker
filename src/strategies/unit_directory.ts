/**
 * unit_directory strategy — CynthiaOS Transform Worker
 *
 * The canonical unit dimension is built from the latest authoritative
 * AppFolio roster-bearing reports rather than from unit_directory alone.
 * This is necessary because suffix/student units and family units can be
 * present in rent-roll, lease, or tenant feeds while absent from the
 * standalone unit directory export.
 */

import {
  TransformContext,
  TransformStrategy,
  SilverNormalizeResult,
  GoldPromoteResult,
  SilverAppfolioReport,
} from "../types";
import { normalizeUnitId } from "../utils/normalize";

interface CanonicalUnit {
  unit_id: string;
  raw_name: string;
}

interface SourceUnitRow {
  report_type: string;
  raw_name: string;
  report_date: string | null;
}

const AUTHORITATIVE_UNIT_REPORTS = [
  "unit_directory",
  "rent_roll",
  "tenant_directory",
  "lease_expiration_detail",
  "lease_expiration",
];

// Confirmed leaseable family units that AppFolio can omit from roster exports.
// Additional deployment-specific IDs may be supplied as a comma-separated list.
const DEFAULT_UNIT_OVERRIDES = ["202", "313"];

function expandCanonicalIds(rawName: string): string[] {
  const compact = rawName
    .trim()
    .replace(/\s*-\s*/g, "-")
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[^a-z0-9_-]/g, "")
    .replace(/_+-_+/g, "-");

  // Legacy ingestion occasionally concatenated a base unit and a student
  // suffix unit (for example `120-120-a`). Preserve both physical units.
  const duplicatedPrefix = compact.match(/^(\d+)-\1-([a-z0-9]+)$/);
  if (duplicatedPrefix) {
    return [duplicatedPrefix[1], `${duplicatedPrefix[1]}-${duplicatedPrefix[2]}`];
  }

  const unitId = normalizeUnitId(compact);
  return unitId === "unknown" ? [] : [unitId];
}

function addCanonicalUnit(
  unitsById: Map<string, CanonicalUnit>,
  rawName: string
): void {
  for (const unitId of expandCanonicalIds(rawName)) {
    if (!unitsById.has(unitId)) {
      unitsById.set(unitId, { unit_id: unitId, raw_name: rawName });
    }
  }
}

export const unitDirectoryStrategy: TransformStrategy = {
  normalizeSilver(ctx: TransformContext): SilverNormalizeResult {
    const raw = ctx.bronze.raw_data as {
      results?: Record<string, unknown>[];
      data?: Record<string, unknown>[];
      rows?: Record<string, unknown>[];
    };
    const rows = raw.results ?? raw.data ?? raw.rows ?? [];
    const unitsById = new Map<string, CanonicalUnit>();

    for (const row of rows) {
      const rawName = String(
        row["UnitName"] ?? row["Unit"] ?? row["unit_name"] ??
        row["unit"] ?? row["unit_id"] ?? ""
      ).trim();
      if (rawName) addCanonicalUnit(unitsById, rawName);
    }

    const units = Array.from(unitsById.values()).sort((a, b) =>
      a.unit_id.localeCompare(b.unit_id, undefined, { numeric: true })
    );

    return {
      normalized_data: {
        report_date: ctx.bronze.report_date,
        units,
        unit_count: units.length,
      },
    };
  },

  async promoteGold(
    ctx: TransformContext & { silver: SilverAppfolioReport }
  ): Promise<GoldPromoteResult> {
    const nd = ctx.silver.normalized_data as {
      report_date: string;
      units: CanonicalUnit[];
      unit_count: number;
    };
    const reportDate = nd.report_date ?? ctx.bronze.report_date;
    const unitsById = new Map<string, CanonicalUnit>();

    for (const unit of nd.units ?? []) addCanonicalUnit(unitsById, unit.raw_name);

    // Query every row from the latest report date for each authoritative source.
    // A report date can have multiple Bronze chunks, so selecting one report ID
    // would silently omit units.
    const sourceRows = await ctx.sql<SourceUnitRow[]>`
      WITH latest_dates AS (
        SELECT report_type, MAX(report_date) AS report_date
        FROM bronze_appfolio_reports
        WHERE report_type = ANY(${AUTHORITATIVE_UNIT_REPORTS}::text[])
        GROUP BY report_type
      )
      SELECT
        b.report_type,
        COALESCE(
          NULLIF(TRIM(elem->>'UnitName'), ''),
          NULLIF(TRIM(elem->>'Unit'), ''),
          NULLIF(TRIM(elem->>'unit_name'), ''),
          NULLIF(TRIM(elem->>'unit'), ''),
          NULLIF(TRIM(elem->>'unit_id'), '')
        ) AS raw_name,
        b.report_date::text
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
      ) AS elem
      WHERE COALESCE(
        NULLIF(TRIM(elem->>'UnitName'), ''),
        NULLIF(TRIM(elem->>'Unit'), ''),
        NULLIF(TRIM(elem->>'unit_name'), ''),
        NULLIF(TRIM(elem->>'unit'), ''),
        NULLIF(TRIM(elem->>'unit_id'), '')
      ) IS NOT NULL
    `;

    for (const row of sourceRows) addCanonicalUnit(unitsById, row.raw_name);

    const configuredOverrides = String(process.env.CANONICAL_UNIT_OVERRIDES ?? "")
      .split(",")
      .map((v) => v.trim())
      .filter(Boolean);
    for (const rawName of [...DEFAULT_UNIT_OVERRIDES, ...configuredOverrides]) {
      addCanonicalUnit(unitsById, rawName);
    }

    const canonicalUnits = Array.from(unitsById.values()).sort((a, b) =>
      a.unit_id.localeCompare(b.unit_id, undefined, { numeric: true })
    );
    const contributingReports = new Set(sourceRows.map((row) => row.report_type));
    const rosterLooksSafe =
      canonicalUnits.length >= 150 &&
      canonicalUnits.length <= 250 &&
      contributingReports.size >= 2 &&
      contributingReports.has("unit_directory");

    if (!rosterLooksSafe) {
      console.warn(
        `[unit_directory] REFUSING Gold roster replacement: ` +
        `canonical_units=${canonicalUnits.length}, ` +
        `contributing_reports=[${Array.from(contributingReports).sort().join(", ")}]`
      );
      return {
        gold_ids: [],
        skipped: true,
        skip_reason: "canonical_roster_failed_safety_guard",
      };
    }

    console.log(
      `[unit_directory] canonical_unit_ids=${JSON.stringify(canonicalUnits.map((unit) => unit.unit_id))}`
    );

    await ctx.sql`
      CREATE TABLE IF NOT EXISTS gold_units (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        unit_id     TEXT NOT NULL UNIQUE,
        raw_name    TEXT,
        report_date DATE,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        updated_at  TIMESTAMPTZ DEFAULT NOW()
      )
    `;
    await ctx.sql`ALTER TABLE gold_units ADD COLUMN IF NOT EXISTS raw_name TEXT`;

    const goldIds: string[] = [];
    for (const unit of canonicalUnits) {
      const rows = await ctx.sql`
        INSERT INTO gold_units (unit_id, raw_name, report_date, updated_at)
        VALUES (${unit.unit_id}, ${unit.raw_name}, ${reportDate}, NOW())
        ON CONFLICT (unit_id) DO UPDATE SET
          raw_name    = EXCLUDED.raw_name,
          report_date = EXCLUDED.report_date,
          updated_at  = NOW()
        RETURNING id
      `;
      const id = (rows as unknown as { id: string }[])[0]?.id;
      if (id) goldIds.push(id);
    }

    const currentUnitIds = canonicalUnits.map((u) => u.unit_id);
    await ctx.sql`
      DELETE FROM gold_units
      WHERE unit_id != ALL(${currentUnitIds}::text[])
    `;

    console.log(
      `[unit_directory] date=${reportDate} canonical_units=${canonicalUnits.length} ` +
      `source_rows=${sourceRows.length} gold_rows=${goldIds.length}`
    );

    return { gold_ids: goldIds, skipped: false };
  },
};
