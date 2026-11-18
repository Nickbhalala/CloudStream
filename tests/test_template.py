"""Static checks on the SAM template (no AWS calls)."""

import shutil
import subprocess
from pathlib import Path

import pytest

TEMPLATE = Path(__file__).resolve().parent.parent / "template.yaml"


@pytest.mark.skipif(not shutil.which("cfn-lint"), reason="cfn-lint not installed")
def test_template_passes_cfn_lint():
    proc = subprocess.run(["cfn-lint", str(TEMPLATE)], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_template_matches_code():
    text = TEMPLATE.read_text()
    # Handler path, env vars and key names must match what the code expects.
    assert "Handler: cloudstream.handler.lambda_handler" in text
    for var in ("AGGREGATES_TABLE", "DEDUPE_TABLE", "DLQ_URL"):
        assert f"{var}:" in text
    for key in ("series", "minute", "event_id", "expires_at"):
        assert key in text
    assert "ReportBatchItemFailures" in text
