import os
import sys
import subprocess
import argparse
from pathlib import Path


def run_step(label: str, cmd: list[str], env: dict[str, str]) -> None:
    print(f"\n=== {label} ===")
    print(" ", " ".join(cmd))
    completed = subprocess.run(cmd, env=env)
    if completed.returncode != 0:
        raise SystemExit(f"Step failed: {label} (exit {completed.returncode})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ETL pipeline: Extract -> Transform -> Load")
    parser.add_argument("--date", dest="date", help="Batch date YYYYMMDD. If omitted, steps may auto-detect latest.")
    parser.add_argument("--max-rows", dest="max_rows", type=int, default=0, help="Limit rows during transform for faster dev runs")
    parser.add_argument("--skip-extract", action="store_true", help="Skip Extract step")
    parser.add_argument("--skip-transform", action="store_true", help="Skip Transform step")
    parser.add_argument("--skip-load", action="store_true", help="Skip Load step")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    py = sys.executable or "python"

    env = os.environ.copy()
    if args.date:
        env["DATE"] = args.date
    if args.max_rows and args.max_rows > 0:
        env["MAX_ROWS"] = str(args.max_rows)

    if not args.skip_extract:
        run_step(
            "Extract",
            [py, str(project_root / "etl" / "etl.py")],
            env,
        )

    if not args.skip_transform:
        run_step(
            "Transform",
            [py, str(project_root / "etl" / "transform.py")],
            env,
        )

    if not args.skip_load:
        run_step(
            "Load",
            [py, str(project_root / "etl" / "load.py")],
            env,
        )

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()


