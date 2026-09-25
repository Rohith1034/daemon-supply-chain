import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.validation.validator_utils import generate_validation_report, print_summary


if __name__ == "__main__":
    print("Validation mode: this script only checks the event contract and does not execute the live simulator or write to the orders table.")
    report = generate_validation_report()
    print_summary(report)
