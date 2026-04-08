// ── Silver Validation Layer ───────────────────────────────────────────────────
//
// Validates normalized Silver data for each supported report type.
// Checks required fields, numeric ranges, and structural integrity.
//
// Design principles:
//  - NEVER throws or blocks the pipeline — all failures are logged as warnings
//  - Returns a structured ValidationResult that is stored in pipeline_logs
//  - Each report type has an explicit schema definition
//  - Anomalies are counted and surfaced, not silently swallowed

export type ValidationStatus = "passed" | "warned" | "failed";

export interface FieldAnomaly {
  field: string;
  issue: string;
  value?: unknown;
}

export interface ValidationResult {
  report_type: string;
  row_count: number;
  anomaly_count: number;
  validation_status: ValidationStatus;
  anomalies: FieldAnomaly[];
  validated_at: string;
}

// ── Per-report schema definitions ────────────────────────────────────────────

interface FieldRule {
  field: string;
  required?: boolean;
  numericPositive?: boolean;   // must be > 0
  numericNonNeg?: boolean;     // must be >= 0
  notUnknown?: boolean;        // must not equal 'unknown'
  notEmpty?: boolean;          // must not be empty string / null / undefined
}

interface ReportSchema {
  // Rules applied to the top-level normalized_data object
  topLevel?: FieldRule[];
  // Rules applied to each row in normalized_data.rows[]
  rowLevel?: FieldRule[];
  // Minimum expected row count (warn if below)
  minRows?: number;
}

const SCHEMAS: Record<string, ReportSchema> = {
  delinquency: {
    rowLevel: [
      { field: "tenant_id",   required: true,  notEmpty: true, notUnknown: true },
      { field: "unit",        required: true,  notEmpty: true, notUnknown: true },
      { field: "total_balance", required: true, numericNonNeg: true },
    ],
    minRows: 1,
  },
  aged_receivables: {
    rowLevel: [
      { field: "tenant_id",     required: true, notEmpty: true, notUnknown: true },
      { field: "unit",          required: true, notEmpty: true, notUnknown: true },
      { field: "total_balance", required: true, numericNonNeg: true },
    ],
    minRows: 1,
  },
  rent_roll: {
    rowLevel: [
      { field: "tenant_id", required: true, notEmpty: true, notUnknown: true },
      { field: "unit_id",   required: true, notEmpty: true, notUnknown: true },
      { field: "rent",      required: true, numericPositive: true },
    ],
    minRows: 1,
  },
  tenant_directory: {
    rowLevel: [
      { field: "tenant_id",  required: true, notEmpty: true, notUnknown: true },
      { field: "unit_id",    required: true, notEmpty: true, notUnknown: true },
      { field: "full_name",  required: true, notEmpty: true },
    ],
    minRows: 1,
  },
  income_statement: {
    topLevel: [
      { field: "summary",                required: true },
    ],
    // Warn if rental_income is zero — indicates field mapping failure
    minRows: 0,
  },
  unit_vacancy: {
    topLevel: [
      { field: "total_units",  required: true, numericPositive: true },
      { field: "vacant_units", required: true, numericNonNeg: true },
    ],
    minRows: 0,
  },
  unit_turn_detail: {
    rowLevel: [
      { field: "unit_id", required: true, notEmpty: true, notUnknown: true },
    ],
    minRows: 1,
  },
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function toNum(v: unknown): number {
  if (typeof v === "number") return v;
  if (typeof v === "string") {
    const n = parseFloat(v.replace(/[^0-9.-]/g, ""));
    return isNaN(n) ? NaN : n;
  }
  return NaN;
}

function checkRule(
  obj: Record<string, unknown>,
  rule: FieldRule,
  prefix: string
): FieldAnomaly | null {
  const val = obj[rule.field];

  if (rule.required || rule.notEmpty) {
    if (val === undefined || val === null || val === "") {
      return { field: `${prefix}${rule.field}`, issue: "missing or empty", value: val };
    }
  }

  if (rule.notUnknown && String(val).toLowerCase() === "unknown") {
    return { field: `${prefix}${rule.field}`, issue: "value is 'unknown'", value: val };
  }

  if (rule.numericPositive) {
    const n = toNum(val);
    if (isNaN(n) || n <= 0) {
      return { field: `${prefix}${rule.field}`, issue: `expected > 0, got ${val}`, value: val };
    }
  }

  if (rule.numericNonNeg) {
    const n = toNum(val);
    if (isNaN(n) || n < 0) {
      return { field: `${prefix}${rule.field}`, issue: `expected >= 0, got ${val}`, value: val };
    }
  }

  return null;
}

// ── Main validator ────────────────────────────────────────────────────────────

export function validateSilver(
  report_type: string,
  normalized_data: Record<string, unknown>
): ValidationResult {
  const anomalies: FieldAnomaly[] = [];
  const schema = SCHEMAS[report_type];
  const validatedAt = new Date().toISOString();

  const rows: Record<string, unknown>[] = Array.isArray(normalized_data.rows)
    ? (normalized_data.rows as Record<string, unknown>[])
    : [];

  const rowCount = rows.length;

  if (!schema) {
    // No schema defined — pass through with a note
    return {
      report_type,
      row_count: rowCount,
      anomaly_count: 0,
      validation_status: "passed",
      anomalies: [],
      validated_at: validatedAt,
    };
  }

  // ── Top-level field checks ────────────────────────────────────────────────
  if (schema.topLevel) {
    for (const rule of schema.topLevel) {
      const anomaly = checkRule(normalized_data, rule, "");
      if (anomaly) anomalies.push(anomaly);
    }
  }

  // ── Special: income_statement rental_income zero check ───────────────────
  if (report_type === "income_statement") {
    const summary = normalized_data.summary as Record<string, unknown> | undefined;
    if (summary) {
      const rentalIncome = toNum(summary.rental_income);
      if (!isNaN(rentalIncome) && rentalIncome === 0) {
        anomalies.push({
          field: "summary.rental_income",
          issue: "rental_income is 0 — possible field mapping failure",
          value: 0,
        });
      }
    }
  }

  // ── Row-level checks (first 5 anomalous rows only to avoid log spam) ─────
  if (schema.rowLevel && rows.length > 0) {
    let rowAnomalyCount = 0;
    for (let i = 0; i < rows.length; i++) {
      for (const rule of schema.rowLevel) {
        const anomaly = checkRule(rows[i] as Record<string, unknown>, rule, `rows[${i}].`);
        if (anomaly) {
          rowAnomalyCount++;
          if (rowAnomalyCount <= 5) {
            anomalies.push(anomaly);
          }
        }
      }
    }
    if (rowAnomalyCount > 5) {
      anomalies.push({
        field: "rows",
        issue: `${rowAnomalyCount - 5} additional row anomalies suppressed (showing first 5)`,
      });
    }
  }

  // ── Minimum row count check ───────────────────────────────────────────────
  if (schema.minRows !== undefined && schema.minRows > 0 && rows.length < schema.minRows) {
    anomalies.push({
      field: "rows",
      issue: `expected at least ${schema.minRows} rows, got ${rows.length}`,
      value: rows.length,
    });
  }

  // ── Determine status ──────────────────────────────────────────────────────
  let status: ValidationStatus = "passed";
  if (anomalies.length > 0) {
    // Warnings do not block the pipeline
    status = "warned";
  }

  return {
    report_type,
    row_count: rowCount,
    anomaly_count: anomalies.length,
    validation_status: status,
    anomalies,
    validated_at: validatedAt,
  };
}
