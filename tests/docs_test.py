"""FR-041: the docs and the module headers describe the cursor-only engine. No file below names a
deleted module or a deleted control column. docs/cursor_orchestration_analysis.md and docs/baseline.md
are history and are not scanned."""
import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCS = ["README.md", "docs/deploy.md", "docs/monitoring.md", "docs/lakebase_schema.md", "CLAUDE.md"]
MODULES = sorted(p.relative_to(ROOT).as_posix() for p in (ROOT / "src/lakefed_ingest").glob("*.py"))
FORBIDDEN = ["ct_engine", "batch_append", "CHANGETABLE", "change tracking", "write_mode", "cursor_column"]


def _found(text: str):
    low = text.lower()
    return [term for term in FORBIDDEN if term.lower() in low]


@pytest.mark.parametrize("doc", DOCS)
def test_doc_has_no_deleted_terms(doc):
    assert _found((ROOT / doc).read_text()) == []


@pytest.mark.parametrize("module", MODULES)
def test_module_docstring_has_no_deleted_terms(module):
    docstring = ast.get_docstring(ast.parse((ROOT / module).read_text())) or ""
    assert _found(docstring) == []
