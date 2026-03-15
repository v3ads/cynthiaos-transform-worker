import express, { Request, Response } from "express";

const app = express();
const PORT = parseInt(process.env.PORT ?? "3002", 10);
const SERVICE_NAME = "cynthiaos-transform-worker";

app.use(express.json());

// ── Health check ──────────────────────────────────────────────────────────────
app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({
    service: SERVICE_NAME,
    status: "ok",
    timestamp: new Date().toISOString(),
  });
});

// ── Catch-all ─────────────────────────────────────────────────────────────────
app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: "not_found" });
});

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
  console.log(`[${SERVICE_NAME}] listening on port ${PORT}`);
});

export default app;
