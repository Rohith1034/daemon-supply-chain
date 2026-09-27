"""Run only the inbound lifecycle."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SIMULATOR_ROOT = ROOT / "simulator"
for path in (ROOT, SIMULATOR_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from loading_scripts.domain_flow_runner import run_domain_flow


def main():
    result = run_domain_flow("inbound")
    print(json.dumps(result, indent=2, default=str))
    return result


if __name__ == "__main__":
    main()
