import argparse
import runpy


def main():
    parser = argparse.ArgumentParser(description="Run one simulator cycle")
    parser.add_argument("--mode", choices=("memory", "postgres"), default="memory")
    args = parser.parse_args()
    script = "scripts.run_memory_simulation" if args.mode == "memory" else "scripts.run_db_simulation"
    runpy.run_module(script, run_name="__main__")


if __name__ == "__main__":
    main()