"""The ingestion job's parameters line up: every widget the notebook reads is passed by the job task
and declared as a job parameter, and each tuning parameter defaults to the bundle variable of the same
name (the collection and the Lakebase names have their own defaults)."""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OWN_DEFAULTS = {"task_collection", "db_instance", "db_name"}


def _widgets():
    notebook = json.loads((ROOT / "src/lakefed_ingest/ingest.ipynb").read_text())
    code = "\n".join(c["source"] if isinstance(c["source"], str) else "".join(c["source"])
                     for c in notebook["cells"] if c["cell_type"] == "code")
    return set(re.findall(r'widget\("([a-z_]+)"', code))


def test_every_notebook_widget_is_a_job_parameter_with_a_bundle_variable():
    job = (ROOT / "resources/lakefed_cursor.yml").read_text()
    bundle = (ROOT / "databricks.yml").read_text()
    passed = set(re.findall(r'^\s+([a-z_]+): "\{\{job\.parameters\.\1\}\}"', job, re.M))
    declared = set(re.findall(r"^\s+- name: ([a-z_]+)$", job, re.M))
    defaults = dict(re.findall(r"^\s+- name: ([a-z_]+)\n\s+default: \$\{var\.([a-z_]+)\}", job, re.M))
    variables = set(re.findall(r"^  ([a-z_]+):\n    description:", bundle, re.M))
    widgets = _widgets()
    assert {"table_retries", "table_retry_budget"} <= widgets
    assert widgets - passed == set()
    assert widgets - declared == set()
    tuning = widgets - OWN_DEFAULTS
    assert {w for w in tuning if defaults.get(w) != w} == set()
    assert tuning - variables == set()


def test_table_retries_are_off_by_default():
    bundle = (ROOT / "databricks.yml").read_text()
    assert re.search(r'^  table_retries:\n    description: .*\n    default: "0"$', bundle, re.M)


def test_no_resource_name_starts_with_lakefed_ingest_or_edp():
    """Principle IX: the sister engine reuses no lakefed_ingest or edp resource (FR-038)."""
    import state_store
    bundle = (ROOT / "databricks.yml").read_text()
    resources = [p.read_text() for p in sorted((ROOT / "resources").glob("*.yml"))]
    names = re.findall(r"^bundle:\n  name: (\S+)", bundle, re.M)
    names += re.findall(r"^    default: (\S+)$", bundle, re.M)                 # every variable default
    for text in resources:
        names += re.findall(r"^  jobs:\n    (\w+):", text, re.M)               # job keys
        names += re.findall(r'^\s+name: "?([^"\s$]+)"?$', text, re.M)          # job names (literal ones)
    names += [state_store.SCHEMA]
    names += [p.name for p in (ROOT / "resources").glob("*.yml")]
    assert names and {"lakefed_cursor", "lakefed_cursor_setup", "lakefed-cursor", "lakefed_cursor_lakebase"} <= set(names)
    assert [n for n in names if n.startswith(("lakefed_ingest", "edp"))] == []
