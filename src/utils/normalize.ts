/**
 * Shared normalisation utilities for CynthiaOS transform strategies.
 *
 * All strategies MUST use these functions to ensure consistent key formats
 * across Gold tables, enabling reliable cross-table joins in insight modules.
 */

/**
 * Derive a stable, canonical tenant_id from a raw name and unit identifier.
 *
 * Algorithm:
 *   1. Trim whitespace from both inputs.
 *   2. Concatenate as `{name}_{unit}`.
 *   3. Lowercase the entire string.
 *   4. Replace any run of non-alphanumeric characters with a single underscore.
 *   5. Strip leading/trailing underscores.
 *
 * Examples:
 *   normalizeTenantId("Maria Santos", "101")  → "maria_santos_101"
 *   normalizeTenantId("  Carlos Rivera ", "202A") → "carlos_rivera_202a"
 *   normalizeTenantId("Maria Santos", "")     → "maria_santos"
 */
export function normalizeTenantId(name: unknown, unit: unknown): string {
  const n = String(name ?? "").trim();
  const u = String(unit ?? "").trim();
  const raw = u ? `${n}_${u}` : n;
  if (!raw) return "unknown";
  return raw
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

/**
 * Normalise a unit_id to a consistent lowercase, underscore-separated string.
 *
 * Examples:
 *   normalizeUnitId("101A")  → "101a"
 *   normalizeUnitId("  202 B ") → "202_b"
 */
export function normalizeUnitId(val: unknown): string {
  if (!val) return "unknown";
  return String(val)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_-]/g, "");
}
