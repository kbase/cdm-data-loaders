"""Load converter specification manifests: rule catalogues, per-case manifests and error manifests.

A converter specification lives next to its test fixtures:

- ``rules.yaml``: rule catalogue (sections of rule IDs and statements).
- ``<suite>/<case>/case.yaml``: one manifest per fixture directory listing the rules it exercises and its runs.
- ``<suite>/errors/manifest.yaml``: error fixtures mapped to error codes.

The same manifests drive the specification tests and the generated specification document.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

CASE_MANIFEST = "case.yaml"
ERROR_MANIFEST = "manifest.yaml"


@dataclass(frozen=True, slots=True)
class Rule:
    """A single conversion rule."""

    id: str
    statement: str
    section_id: str


@dataclass(frozen=True, slots=True)
class Section:
    """A group of rules rendered under one heading."""

    id: str
    title: str
    intro: str
    generated: str | None
    rules: tuple[Rule, ...]


@dataclass(frozen=True, slots=True)
class RuleCatalogue:
    """All rules for one converter."""

    title: str
    intro: str
    sections: tuple[Section, ...]

    @property
    def rules(self) -> dict[str, Rule]:
        """Map rule ID to rule, in catalogue order."""
        return {rule.id: rule for section in self.sections for rule in section.rules}


@dataclass(frozen=True, slots=True)
class Run:
    """One execution of a case: input data, parameters and expected outputs."""

    id: str
    data: Path
    expected_json: Path
    expected_table: Path
    xml_tag: str | None
    params: dict[str, Any] = field(default_factory=dict)
    schema_attr: str = "EXPECTED_SCHEMA"
    table_attr: str = "EXPECTED_TABLE"
    expected_count: int | None = None


@dataclass(frozen=True, slots=True)
class Case:
    """A fixture directory with its manifest."""

    suite: str
    name: str
    directory: Path
    title: str
    schema: Path
    expected_schema: Path
    xml_tag: str | None
    rules: tuple[str, ...]
    notes: str
    runs: tuple[Run, ...]

    @property
    def anchor(self) -> str:
        """Stable HTML anchor for the case."""
        return f"case-{self.suite}-{self.name}".replace("_", "-").lower()


@dataclass(frozen=True, slots=True)
class ErrorCase:
    """An input that must be rejected with a specific error code."""

    file: Path
    code: str
    match: str
    description: str
    xml_tag: str | None


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh)
    if not isinstance(loaded, dict):
        msg = f"{path}: expected a mapping at the top level"
        raise TypeError(msg)
    return loaded


def load_rules(path: Path) -> RuleCatalogue:
    """Load a ``rules.yaml`` catalogue, rejecting duplicate rule IDs."""
    raw = _load_yaml(path)
    seen: set[str] = set()
    sections: list[Section] = []
    for raw_section in raw["sections"]:
        rules: list[Rule] = []
        for raw_rule in raw_section.get("rules", []):
            rule_id = raw_rule["id"]
            if rule_id in seen:
                msg = f"{path}: duplicate rule id {rule_id!r}"
                raise ValueError(msg)
            seen.add(rule_id)
            rules.append(Rule(id=rule_id, statement=raw_rule["statement"].strip(), section_id=raw_section["id"]))
        sections.append(
            Section(
                id=raw_section["id"],
                title=raw_section["title"],
                intro=raw_section.get("intro", "").strip(),
                generated=raw_section.get("generated"),
                rules=tuple(rules),
            )
        )
    return RuleCatalogue(title=raw["title"], intro=raw.get("intro", "").strip(), sections=tuple(sections))


def _load_run(directory: Path, raw: dict[str, Any], case_xml_tag: str | None) -> Run:
    return Run(
        id=raw["id"],
        data=(directory / raw["data"]).resolve(),
        expected_json=(directory / raw.get("expected_json", "expected.json")).resolve(),
        expected_table=(directory / raw.get("expected_table", "expected_table.py")).resolve(),
        xml_tag=raw.get("xml_tag", case_xml_tag),
        params=dict(raw.get("params", {})),
        schema_attr=raw.get("schema_attr", "EXPECTED_SCHEMA"),
        table_attr=raw.get("table_attr", "EXPECTED_TABLE"),
        expected_count=raw.get("expected_count"),
    )


def load_case(manifest: Path, suite: str) -> Case:
    """Load one ``case.yaml`` manifest."""
    raw = _load_yaml(manifest)
    directory = manifest.parent
    xml_tag = raw.get("xml_tag")
    return Case(
        suite=suite,
        name=directory.name,
        directory=directory,
        title=raw["title"],
        schema=(directory / raw["schema"]).resolve(),
        expected_schema=(directory / raw.get("expected_schema", "expected_schema.py")).resolve(),
        xml_tag=xml_tag,
        rules=tuple(raw.get("rules", [])),
        notes=raw.get("notes", "").strip(),
        runs=tuple(_load_run(directory, raw_run, xml_tag) for raw_run in raw["runs"]),
    )


def load_cases(suite_dir: Path) -> list[Case]:
    """Load every ``<suite_dir>/*/case.yaml`` manifest, sorted by directory name."""
    return [load_case(manifest, suite_dir.name) for manifest in sorted(suite_dir.glob(f"*/{CASE_MANIFEST}"))]


def load_error_cases(manifest: Path) -> list[ErrorCase]:
    """Load an error ``manifest.yaml``."""
    raw = _load_yaml(manifest)
    return [
        ErrorCase(
            file=(manifest.parent / entry["file"]).resolve(),
            code=entry["code"],
            match=entry["match"],
            description=entry.get("description", "").strip(),
            xml_tag=entry.get("xml_tag"),
        )
        for entry in raw["errors"]
    ]
