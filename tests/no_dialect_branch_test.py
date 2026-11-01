"""SC-007: no engine module branches on the source type; every dialect difference is an adapter method."""
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src" / "lakefed_ingest"


@pytest.mark.parametrize("module", ["cursor_engine.py", "worker.py"])
@pytest.mark.parametrize("needle", ["_is_pg", "src_type =="])
def test_engine_modules_have_no_dialect_branch(module, needle):
    assert needle not in (SRC / module).read_text()
