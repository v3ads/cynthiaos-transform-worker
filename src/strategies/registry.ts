// ── Transform Strategy Registry ───────────────────────────────────────────────
//
// Maps each AppFolio report_type string to its TransformStrategy implementation.
//
// To add support for a new report type:
//   1. Create src/strategies/<report_type>.ts implementing TransformStrategy
//   2. Import it here
//   3. Add an entry to TRANSFORM_STRATEGIES
//
// Report types NOT listed here will be handled by the unsupportedStrategy,
// which safely preserves raw data in Silver and skips Gold promotion.

import { TransformStrategy } from "../types";
import { rentRollStrategy } from "./rent_roll";
import { delinquencyStrategy } from "./delinquency";
import { agedReceivablesStrategy } from "./aged_receivables";
import { tenantDirectoryStrategy } from "./tenant_directory";
import { incomeStatementStrategy } from "./income_statement";
import { occupancySummaryStrategy } from "./occupancy_summary";
import { moveInMoveOutStrategy } from "./move_in_move_out";
import { unsupportedStrategy } from "./unsupported";

// ── Registry ──────────────────────────────────────────────────────────────────

const TRANSFORM_STRATEGIES: Record<string, TransformStrategy> = {
  // ── Implemented ──────────────────────────────────────────────────────────
  rent_roll:          rentRollStrategy,
  delinquency:        delinquencyStrategy,
  aged_receivables:    agedReceivablesStrategy,
  tenant_directory:    tenantDirectoryStrategy,
  income_statement:    incomeStatementStrategy,

  // ── Occupancy: legacy key preserved for historical Bronze records ─────────
  // The AppFolio API uses 'unit_vacancy' as the canonical report type name.
  // 'occupancy_summary' was the original (incorrect) name used during early
  // development. Both keys map to the same strategy for backward compatibility.
  occupancy_summary:   occupancySummaryStrategy,  // legacy — kept for historical records
  unit_vacancy:        occupancySummaryStrategy,  // FIX: correct AppFolio report type name

  // ── Turnover: legacy key preserved for historical Bronze records ──────────
  // The AppFolio API uses 'unit_turn_detail' as the canonical report type name.
  // 'move_in_move_out' was the original (incorrect) name used during early
  // development. Both keys map to the same strategy for backward compatibility.
  move_in_move_out:    moveInMoveOutStrategy,     // legacy — kept for historical records
  unit_turn_detail:    moveInMoveOutStrategy,     // FIX: correct AppFolio report type name

  // ── Planned (add handlers here as they are implemented) ──────────────────
  // maintenance_request: maintenanceRequestStrategy,
  // ... (28 more report types)
};

// ── Resolver ──────────────────────────────────────────────────────────────────

/**
 * Returns the registered strategy for the given report_type.
 * Falls back to unsupportedStrategy if no handler is registered,
 * ensuring the pipeline never crashes on unknown report types.
 */
export function getStrategy(reportType: string): TransformStrategy {
  return TRANSFORM_STRATEGIES[reportType] ?? unsupportedStrategy;
}

/**
 * Returns true if a dedicated strategy is registered for the given report_type.
 * Useful for logging and diagnostics.
 */
export function isSupported(reportType: string): boolean {
  return reportType in TRANSFORM_STRATEGIES;
}

/**
 * Returns the list of all explicitly registered (supported) report types.
 */
export function getSupportedTypes(): string[] {
  return Object.keys(TRANSFORM_STRATEGIES);
}
