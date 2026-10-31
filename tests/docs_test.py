"""FR-041: the docs, the module headers, and the notebook text describe the cursor-only engine. No
file below names a deleted module, a deleted control column, or the old bundle name. Principle VIII:
no doc sentence is over 25 words. docs/cursor_orchestration_analysis.md and docs/baseline.md are
history and are not scanned."""
import ast
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCS = ["README.md", "docs/deploy.md", "docs/monitoring.md", "docs/lakebase_schema.md", "CLAUDE.md"]
MODULES = sorted(p.relative_to(ROOT).as_posix() for p in (ROOT / "src/lakefed_ingest").glob("*.py"))
FORBIDDEN = ["ct_engine", "batch_append", "CHANGETABLE", "change tracking", "write_mode", "cursor_column",
             "lakefed_ingest_setup"]
NOTEBOOKS = ["src/lakefed_ingest/ingest.ipynb", "src/lakefed_ingest/apply_lakebase_schema.ipynb"]
MAX_WORDS = 25


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


def _notebook_text(path: Path) -> str:
    """Markdown cells plus the comment lines of code cells."""
    out = []
    for cell in json.loads(path.read_text())["cells"]:
        lines = "".join(cell["source"]).splitlines()
        out += lines if cell["cell_type"] == "markdown" else [l for l in lines if l.lstrip().startswith("#")]
    return "\n".join(out)


@pytest.mark.parametrize("notebook", NOTEBOOKS)
def test_notebook_text_has_no_deleted_terms(notebook):
    assert _found(_notebook_text(ROOT / notebook)) == []


def long_sentences(text: str):
    """Sentences over MAX_WORDS words. Code blocks, code spans, headings, and tables do not count."""
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    text = re.sub(r"`[^`]*`", "CODE", text)
    text = re.sub(r"\b(e\.g|i\.e|etc)\.", r"\1", text)
    out = []
    for para in re.split(r"\n\s*\n", text):
        if para.lstrip().startswith(("#", "|", "<")):
            continue
        for item in re.split(r"\n(?=\s*(?:[-*]|\d+\.)\s)", para):
            item = re.sub(r"^\s*(?:[-*]|\d+\.)\s+", "", item)
            for sentence in re.split(r"(?<=[.!?])\s+", " ".join(item.split())):
                if len(sentence.split()) > MAX_WORDS and not sentence.endswith("`"):
                    out.append(sentence)
    return out


@pytest.mark.parametrize("doc", ["README.md", "docs/deploy.md", "docs/monitoring.md", "docs/lakebase_schema.md"])
def test_doc_sentences_are_short(doc):
    assert long_sentences((ROOT / doc).read_text()) == []
