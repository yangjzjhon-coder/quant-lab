from __future__ import annotations

from pathlib import Path

from quant_lab.logging_utils import configure_logging, get_logger


def test_configure_logging_writes_project_log_file(tmp_path: Path) -> None:
    configure_logging(project_root=tmp_path, level="INFO")
    logger = get_logger("quant_lab.test")
    logger.info("logging smoke message")

    root_logger = get_logger("quant_lab")
    for handler in root_logger.handlers:
        handler.flush()

    log_path = tmp_path / "data" / "logs" / "quant_lab.log"
    assert log_path.exists()
    assert "logging smoke message" in log_path.read_text(encoding="utf-8")
