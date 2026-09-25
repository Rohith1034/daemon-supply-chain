from datetime import datetime, timezone
from api.schemas.simulator_schema import SimulationRequest
from api.services.simulator_service import run_full_flow

#Existing morning/inbound run:
# request = SimulationRequest(
#     simulation_start_timestamp=datetime(2026, 9, 25, 6, 0, tzinfo=timezone.utc),
#     run_window="morning",
#     duration_hours=2,
#     reset_before_run=False,
# )

# Afternoon/outbound run. Keep reset disabled so the morning state remains available.
request = SimulationRequest(
    simulation_start_timestamp=datetime(2026, 9, 25, 14, 0, tzinfo=timezone.utc),
    run_window="afternoon",
    duration_hours=2,
    reset_before_run=False,
)

result = run_full_flow(request)
print(result)