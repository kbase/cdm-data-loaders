"""Load converter specification manifests: rule catalogues, per-case manifests, and error manifests.

A converter specification lives next to its test fixtures:

- ``rules.yaml``: rule catalogue (sections of rule IDs and statements, plus optional front-matter metadata).
- ``<suite>/<case>/case.yaml``: one manifest per fixture directory listing exercised rules and runs.
- ``<suite>/errors/manifest.yaml``: error fixtures mapped to error codes.

The same manifests drive the specification tests and the generated specification documents.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from specs._loader.yaml_io import CASE_MANIFEST, load_yaml_dict


@dataclass(frozen=True, slots=True)
class Rule:
    """A single conversion or pipeline specification rule."""

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
    """All rules and front-matter metadata for one converter or pipeline specification."""

    title: str
    intro: str
    sections: tuple[Section, ...]
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def rules(self) -> dict[str, Rule]:
        """Map rule ID to rule in catalogue order.

        :return: Mapping of rule IDs to ``Rule`` instances.
        :rtype: dict[str, Rule]
        """
        return {rule.id: rule for section in self.sections for rule in section.rules}


@dataclass(frozen=True, slots=True)
class Run:
    """One execution of a converter case: input data, parameters, and expected outputs."""

    id: str
    data: Path
    expected_json: Path
    expected_table: Path
    xml_tag: str | None
    params: dict[str, Any] = field(default_factory=dict)
    schema_attr: str = "EXPECTED_SCHEMA"
    table_attr: str = "EXPECTED_TABLE"
    expected_count: int | None = None
    expected_rejected: Path | None = None
    extra_expected_json: dict[str, Path] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Case:
    """A converter fixture directory with its manifest."""

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
        """Return the stable HTML anchor for the case.

        :return: Lowercase kebab-case HTML anchor string.
        :rtype: str
        """
        return f"case-{self.suite}-{self.name}".replace("_", "-").lower()


@dataclass(frozen=True, slots=True)
class ErrorCase:
    """An input that must be rejected with a specific error code."""

    file: Path
    code: str
    match: str
    description: str
    xml_tag: str | None
    params: dict[str, Any] = field(default_factory=dict)


def load_rules(path: Path) -> RuleCatalogue:
    """Load a ``rules.yaml`` catalogue, rejecting duplicate rule IDs.

    :param path: Path to ``rules.yaml``.
    :type path: Path
    :raises ValueError: If duplicate rule IDs appear in the catalogue.
    :return: Parsed ``RuleCatalogue`` including top-level front-matter metadata.
    :rtype: RuleCatalogue
    """
    raw = load_yaml_dict(path)
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
    metadata = {k: v for k, v in raw.items() if k not in {"title", "intro", "sections"}}
    return RuleCatalogue(
        title=raw["title"],
        intro=raw.get("intro", "").strip(),
        sections=tuple(sections),
        metadata=metadata,
    )


def _load_run(directory: Path, raw: dict[str, Any], case_xml_tag: str | None) -> Run:
    """Parse one run entry inside a converter ``case.yaml`` manifest.

    :param directory: Case directory containing fixture files.
    :type directory: Path
    :param raw: Raw YAML mapping for one run.
    :type raw: dict[str, Any]
    :param case_xml_tag: Optional case-level default XML tag.
    :type case_xml_tag: str | None
    :return: Parsed ``Run`` specification.
    :rtype: Run
    """
    rejected = raw.get("expected_rejected")
    extra_tables = raw.get("extra_expected_json", {})
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
        expected_rejected=(directory / rejected).resolve() if rejected else None,
        extra_expected_json={name: (directory / path).resolve() for name, path in extra_tables.items()},
    )


def load_case(manifest: Path, suite: str) -> Case:
    """Load one converter ``case.yaml`` manifest.

    :param manifest: Path to ``case.yaml``.
    :type manifest: Path
    :param suite: Suite name (e.g. ``"spec"`` or ``"examples"``).
    :type suite: str
    :return: Parsed ``Case`` specification.
    :rtype: Case
    """
    raw = load_yaml_dict(manifest)
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
    """Load every ``<suite_dir>/*/case.yaml`` manifest sorted by directory name.

    :param suite_dir: Suite directory containing case subdirectories.
    :type suite_dir: Path
    :return: Ordered list of ``Case`` instances.
    :rtype: list[Case]
    """
    return [load_case(manifest, suite_dir.name) for manifest in sorted(suite_dir.glob(f"*/{CASE_MANIFEST}"))]


def load_error_cases(manifest: Path) -> list[ErrorCase]:
    """Load an error ``manifest.yaml`` file.

    :param manifest: Path to ``errors/manifest.yaml``.
    :type manifest: Path
    :return: Ordered list of ``ErrorCase`` instances.
    :rtype: list[ErrorCase]
    """
    raw = load_yaml_dict(manifest)
    return [
        ErrorCase(
            file=(manifest.parent / entry["file"]).resolve(),
            code=entry["code"],
            match=entry["match"],
            description=entry.get("description", "").strip(),
            xml_tag=entry.get("xml_tag"),
            params=dict(entry.get("params", {})),
        )
        for entry in raw["errors"]
    ]
