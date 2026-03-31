from __future__ import annotations

import argparse
import subprocess
import sys
import venv
from pathlib import Path


def main() -> int:
    args = _parse_args()
    repo_path = args.repo_path.expanduser().resolve()
    venv_path = args.venv_path.expanduser().resolve()

    if not (repo_path / "pyproject.toml").exists():
        raise SystemExit(f"TradingAgents repo not found or missing pyproject.toml: {repo_path}")

    builder = venv.EnvBuilder(with_pip=True, upgrade_deps=args.upgrade_pip)
    builder.create(str(venv_path))

    python_executable = _venv_python(venv_path)
    install_command = [str(python_executable), "-m", "pip", "install"]
    if args.editable:
        install_command.append("-e")
    install_command.append(str(repo_path))

    print(f"[bootstrap] repo_path={repo_path}")
    print(f"[bootstrap] venv_path={venv_path}")
    print(f"[bootstrap] python={python_executable}")
    subprocess.run(install_command, check=True)

    print("")
    print("quant-lab research_agent settings:")
    print(f"  local_repo_path: {repo_path}")
    print(f"  python_executable: {python_executable}")
    return 0


def _venv_python(venv_path: Path) -> Path:
    if sys.platform.startswith("win"):
        return venv_path / "Scripts" / "python.exe"
    return venv_path / "bin" / "python"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a dedicated TradingAgents virtualenv.")
    parser.add_argument("repo_path", type=Path, help="Path to the TradingAgents repository.")
    parser.add_argument("venv_path", type=Path, help="Path where the virtualenv should be created.")
    parser.add_argument(
        "--no-editable",
        dest="editable",
        action="store_false",
        help="Install TradingAgents as a regular package instead of editable mode.",
    )
    parser.add_argument(
        "--no-upgrade-pip",
        dest="upgrade_pip",
        action="store_false",
        help="Skip pip/setuptools/wheel upgrade during venv creation.",
    )
    parser.set_defaults(editable=True, upgrade_pip=True)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
