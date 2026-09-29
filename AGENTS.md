# Repository Guidance

## Working Style
- When a request is clear and in scope, carry it through without pausing for routine approval or preference checks.
- This preference does not bypass tool permission prompts, safety requirements, or confirmation for destructive, irreversible, external, or production-impacting actions. Ask before those actions; never claim they were automatically approved.
- Keep changes narrow and preserve established simulator behavior. Trace the owning flow before editing; avoid broad refactors or changes to unrelated services.
- For lifecycle fixes, follow event identity and state through generators, handlers, orchestration, and persistence. Verify persisted database state when relevant; a successful process exit alone is not proof of success.

## Project Map
- The FastAPI surface is under `api/`; simulator orchestration and event runtime are under `simulator/`.
- PostgreSQL access and transactional outbox behavior are in `simulator/core/db.py` and `simulator/core/outbox.py`.
- Kafka, Spark, and consumer directories are placeholders. Do not treat them as implemented integrations or expand into them unless requested.
- See [README.md](README.md) for the project overview and [simulator/flow.md](simulator/flow.md) for event flow details.

## Validation
- Run the focused tests for the changed behavior, then the broader relevant suite when practical. The project has no documented test configuration; pytest tests may require an isolated database.
- Contract validation: `.\\.venv\\Scripts\\python.exe scripts\\run_simulator_validation.py` from the repository root.
- In-memory simulator: `.\\.venv\\Scripts\\python.exe run_simulator.py --mode memory`.
- PostgreSQL mode applies migrations and writes data; use only an isolated development/test database.
- Scripts and tests may add `simulator/` to `sys.path`; preserve that import setup when working on those entry points.