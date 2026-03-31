from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


DEFAULT_TEST_TARGETS = [
    "tests/test_runtime_policy.py",
    "tests/test_routed_backtest.py",
    "tests/test_demo_loop_runtime.py",
    "tests/test_demo_portfolio_loop_runtime.py",
    "tests/test_service_monitor.py",
    "tests/test_service_client_visuals.py",
    "tests/test_reporting.py",
]


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    extra_args = sys.argv[1:]
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "--maxfail=1",
        *DEFAULT_TEST_TARGETS,
        *extra_args,
    ]

    print(
        json.dumps(
            {
                "repo_root": str(repo_root),
                "python": sys.executable,
                "targets": DEFAULT_TEST_TARGETS,
                "extra_args": extra_args,
                "command": command,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    completed = subprocess.run(command, cwd=repo_root)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
