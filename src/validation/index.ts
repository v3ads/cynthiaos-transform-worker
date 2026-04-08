// ── Validation Layer — Barrel Export ─────────────────────────────────────────
export { validateSilver, ValidationResult, FieldAnomaly, ValidationStatus } from "./silverValidator";
export { guardGoldRow, logRejection, RejectionResult, GoldTableName } from "./goldGuard";
export { runIntegrityChecks, IntegrityReport, IntegrityCheck } from "./integrityChecker";
export {
  ensurePipelineLogsTable,
  logSilverValidation,
  logGoldPromotion,
  logIntegrityReport,
} from "./pipelineLogger";
