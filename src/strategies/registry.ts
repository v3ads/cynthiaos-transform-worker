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
import { unsupportedStrategy } from "./unsupported";

// ── Registry ──────────────────────────────────────────────────────────────────

const TRANSFORM_STRATEGIES: Record<string, TransformStrategy> = {
  // ── Implemented ──────────────────────────────────────────────────────────
  rent_roll:          rentRollStrategy,
  delinquency:        delinquencyStrategy,
  aged_receivables:   agedReceivablesStrategy,

  // ── Planned (add handlers here as they are implemented) ──────────────────
  // tenant_directory:    tenantDirectoryStrategy,
  // income_statement:    incomeStatementStrategy,
  // occupancy_summary:   occupancySummaryStrategy,
  // move_in_move_out:    moveInMoveOutStrategy,
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
