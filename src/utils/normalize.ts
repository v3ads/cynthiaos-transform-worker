/**
 * Shared normalisation utilities for CynthiaOS transform strategies.
 *
 * All strategies MUST use these functions to ensure consistent key formats
 * across Gold tables, enabling reliable cross-table joins in insight modules.
 */

/**
 * Derive a stable, canonical tenant_id from a raw name string.
 *
 * IMPORTANT: tenant_id is derived from the tenant NAME ONLY — never from
 * the unit number. The unit is stored separately in the `unit_id` column.
 * This ensures consistent tenant_id values across all Gold tables
 * (gold_tenants, gold_lease_expirations, gold_delinquency_records,
 *  gold_aged_receivables) so cross-table JOINs work correctly.
 *
 * Algorithm:
 *   1. Trim whitespace from the name.
 *   2. Lowercase the entire string.
 *   3. Replace any run of non-alphanumeric characters with a single underscore.
 *   4. Strip leading/trailing underscores.
 *
 * The second `_unit` parameter is accepted but IGNORED — it exists only
 * for backward compatibility with call sites that previously passed the unit.
 *
 * Examples:
 *   normalizeTenantId("Maria Santos", "101")  → "maria_santos"
 *   normalizeTenantId("  Carlos Rivera ", "202A") → "carlos_rivera"
 *   normalizeTenantId("PICINICH, ALEC", "")   → "picinich_alec"
 *   normalizeTenantId("Jose Feliu", "103")    → "jose_feliu"
 */
export function normalizeTenantId(name: unknown, _unit?: unknown): string {
  const n = String(name ?? "").trim();
  if (!n) return "unknown";
  return n
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

/**
 * Normalise a unit_id to a consistent lowercase, hyphen-separated string.
 *
 * AppFolio uses ' - ' (space-hyphen-space) for student unit sub-designations,
 * e.g. '114 - A' and '120 - B'. These must be collapsed to a single hyphen
 * BEFORE spaces are removed, so the result is '114-a' and '120-b' — not
 * '114_-_a' which breaks cross-table JOINs.
 *
 * Algorithm:
 *   1. Trim leading/trailing whitespace.
 *   2. Collapse any sequence of optional-spaces + hyphen + optional-spaces
 *      into a single hyphen (handles ' - ', '- ', ' -', '-').
 *   3. Lowercase the entire string.
 *   4. Remove any remaining whitespace characters.
 *   5. Strip characters that are not alphanumeric, hyphen, or underscore.
 *
 * Examples:
 *   normalizeUnitId("101")        → "101"
 *   normalizeUnitId("114 - A")   → "114-a"
 *   normalizeUnitId("120 - B")   → "120-b"
 *   normalizeUnitId("220_dnu-b") → "220_dnu-b"
 *   normalizeUnitId("  202 B ")  → "202b"
 */
export function normalizeUnitId(val: unknown): string {
  if (!val) return "unknown";
  return String(val)
    .trim()
    // Collapse ' - ' (AppFolio student unit separator) into a single hyphen
    .replace(/\s*-\s*/g, "-")
    .toLowerCase()
    // Remove any remaining whitespace
    .replace(/\s+/g, "")
    // Strip characters that are not alphanumeric, hyphen, or underscore
    .replace(/[^a-z0-9_-]/g, "");
}
