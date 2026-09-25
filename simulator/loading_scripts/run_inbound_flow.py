"""Deprecated testing utility: use master_simulator.run_simulation_cycle instead."""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from master_simulator import run_simulation_cycle


def main():
    print("Testing utility only. Use master_simulator as the production scheduler entry point.")
    return run_simulation_cycle()


if __name__ == "__main__":
    main()
