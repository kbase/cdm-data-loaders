"""Tests for converter and pipeline specification documentation rendering and live reference tables."""

from pathlib import Path
from typing import Final

import pyarrow as pa
import pytest
from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.exceptions.facades.jsonschema_to_pyarrow import (
    JSONSCHEMA_TO_PYARROW_ERRORS,
)
from cdm_data_loaders.converters.exceptions.facades.pyarrow_to_jsonschema import (
    PYARROW_TO_JSONSCHEMA_ERRORS,
)
from cdm_data_loaders.converters.exceptions.facades.pyarrow_to_linkml import (
    PYARROW_TO_LINKML_ERRORS,
)
from cdm_data_loaders.converters.exceptions.facades.pydantic_to_pyarrow import (
    PYDANTIC_TO_PYARROW_ERRORS,
)
from cdm_data_loaders.converters.exceptions.facades.xsd_to_pyarrow import (
    XSD_TO_PYARROW_ERRORS,
)
from cdm_data_loaders.pipelines.jsonlines.exceptions import (
    JSONLINES_JSONSCHEMA_ERRORS,
    JsonlJsonschemaError,
)
from cdm_data_loaders.pipelines.xmltopyarrow.exceptions import (
    XMLTOPYARROW_CONFIG_ERRORS,
    XmlToPyArrowConfigError,
)
from cdm_data_loaders.pipelines.xsv.exceptions import (
    XSV_SCHEMA_ERRORS,
    XsvSchemaError,
)
from docs_macros.env import (
    build_generated_docs_plan,
    render_converter_spec,
    render_pipeline_spec,
    render_reference_table,
)
from docs_macros.formatting import pyarrow_to_python_type_label
from docs_macros.spec_context import (
    CONVERTER_SPECS,
    PIPELINE_SPECS,
    PipelineSpecConfig,
)
from tests.converter_spec import load_cases, load_rules
from tests.pipeline_spec import load_pipeline_cases

pytestmark = pytest.mark.docs

type SpecErrorClass = type[ConversionError | JsonlJsonschemaError | XmlToPyArrowConfigError | XsvSchemaError]

CONVERTER_ERRORS_BY_SPEC: Final[frozendict[str, tuple[type[ConversionError], ...]]] = frozendict(
    {
        "jsonschema_to_pyarrow": JSONSCHEMA_TO_PYARROW_ERRORS,
        "pyarrow_to_jsonschema": PYARROW_TO_JSONSCHEMA_ERRORS,
        "pyarrow_to_linkml": PYARROW_TO_LINKML_ERRORS,
        "pydantic_to_pyarrow": PYDANTIC_TO_PYARROW_ERRORS,
        "xsd_to_pyarrow": XSD_TO_PYARROW_ERRORS,
    }
)

PIPELINE_ERRORS_BY_FAMILY: Final[frozendict[str, tuple[SpecErrorClass, ...]]] = frozendict(
    {
        "jsonlines_extract": (),
        "jsonlines_jsonschema": JSONLINES_JSONSCHEMA_ERRORS,
        "jsonlines_pydantic": (),
        "xmltodict": (),
        "xmltopyarrow": XMLTOPYARROW_CONFIG_ERRORS,
        "xsv": XSV_SCHEMA_ERRORS,
    }
)


def test_converter_and_pipeline_spec_error_registries_cover_all_specs() -> None:
    """Every converter and pipeline specification in docs_macros is covered by the spec doc test registries."""
    assert set(CONVERTER_ERRORS_BY_SPEC) == set(CONVERTER_SPECS)
    assert set(PIPELINE_ERRORS_BY_FAMILY) == set(PIPELINE_SPECS)


@pytest.mark.parametrize(
    ("spec_name", "error_classes"),
    [pytest.param(name, errs, id=name) for name, errs in sorted(CONVERTER_ERRORS_BY_SPEC.items())],
)
def test_converter_specification_document_pass_up_to_date(
    spec_name: str,
    error_classes: tuple[type[ConversionError], ...],
) -> None:
    """Each converter specification stub invokes render_converter_spec and renders all rules, cases, and errors."""
    spec_cfg = CONVERTER_SPECS[spec_name]
    stub_text = build_generated_docs_plan()[Path(spec_cfg.output.name)]
    assert "render_macros: true" in stub_text
    assert f'render_converter_spec("{spec_name}")' in stub_text

    rules = load_rules(spec_cfg.data_dir / "rules.yaml")
    cases = [case for suite in spec_cfg.suites for case in load_cases(spec_cfg.data_dir / suite)]

    rendered = render_converter_spec(spec_cfg)
    assert f"# {rules.title}" in rendered
    for rule_id in rules.rules:
        assert f"**{rule_id}**" in rendered
        assert f'<a id="{rule_id.lower()}"></a>' in rendered
    for case in cases:
        assert f'<a id="{case.anchor}"></a>' in rendered
    for err_cls in error_classes:
        assert f"**{err_cls.code}**" in rendered
        assert f"`{err_cls.__name__}`" in rendered


@pytest.mark.parametrize(
    ("family_name", "error_classes"),
    [pytest.param(name, errs, id=name) for name, errs in sorted(PIPELINE_ERRORS_BY_FAMILY.items())],
)
def test_pipeline_specification_document_pass_up_to_date(
    family_name: str,
    error_classes: tuple[SpecErrorClass, ...],
) -> None:
    """Each pipeline specification stub invokes render_pipeline_spec and renders all rules, cases, and errors."""
    spec = PIPELINE_SPECS[family_name]
    stub_text = build_generated_docs_plan()[Path(spec.output_path.name)]
    assert "render_macros: true" in stub_text
    assert f'render_pipeline_spec("{spec.name}")' in stub_text

    catalogue = load_rules(spec.data_dir / "rules.yaml")
    pipeline_cases = load_pipeline_cases(spec.data_dir)
    converter_cases = load_cases(spec.data_dir / "spec") if spec.converter_config is not None else []

    rendered = render_pipeline_spec(spec)
    assert f"# {catalogue.title}" in rendered
    for rule_id in catalogue.rules:
        assert f"**{rule_id}**" in rendered
        assert f'<a id="{rule_id.lower()}"></a>' in rendered
    for pipeline_case in pipeline_cases:
        assert f'<a id="{pipeline_case.anchor}"></a>' in rendered
    for converter_case in converter_cases:
        assert f'<a id="{converter_case.anchor}"></a>' in rendered
    for err_cls in error_classes:
        assert f"**{err_cls.code}**" in rendered
        assert f"`{err_cls.__name__}`" in rendered


def test_render_pipeline_spec_custom_config_and_unknown_name() -> None:
    """render_pipeline_spec accepts a custom PipelineSpecConfig and raises KeyError on unknown names."""
    custom_spec = PipelineSpecConfig(
        name="xmltopyarrow",
        data_dir=PIPELINE_SPECS["xmltopyarrow"].data_dir,
        output_path=PIPELINE_SPECS["xmltopyarrow"].output_path,
        presets=PIPELINE_SPECS["xmltopyarrow"].presets,
    )
    rendered = render_pipeline_spec(custom_spec)
    assert "# XML to PyArrow Pipeline Specification" in rendered

    with pytest.raises(KeyError, match="Unknown pipeline specification"):
        render_pipeline_spec("nonexistent_pipeline")


@pytest.mark.parametrize(
    ("table_id", "expected_fragments"),
    [
        pytest.param(
            "xsv_config_table",
            ("| `x-delimiter` |", "| `x-has-header` |"),
            id="xsv_config_table",
        ),
        pytest.param(
            "column_type_table",
            (
                "| `BIGINT` | `False` | `pa.int64()` | `False` |",
                "| `VARCHAR` | `True` | `pa.string()` | `False` |",
                "| `VARCHAR (auto)` | `False` | `pa.string()` | `True` |",
            ),
            id="column_type_table",
        ),
        pytest.param(
            "pyarrow_to_jsonschema_scalar_table",
            (
                "| `pa.int64()` |",
                "| `pa.decimal128(10, 2)` |",
                '| `pa.timestamp("us", tz="UTC")` |',
            ),
            id="pyarrow_to_jsonschema_scalar_table",
        ),
        pytest.param(
            "pyarrow_to_linkml_scalar_table",
            (
                "| `pa.uint8()` | `int` | `integer` | `0` .. `255` |",
                "| `pa.decimal128(10, 2)` | `str` (fixed-point decimal) | `decimal` | — |",
                '| `pa.timestamp("us", tz="UTC")` |',
            ),
            id="pyarrow_to_linkml_scalar_table",
        ),
        pytest.param(
            "presets_table",
            (
                "| `pubmed` | `PubMedXmlToPyArrowSettings` |",
                "| `validate_md5_sidecar` |",
            ),
            id="presets_table",
        ),
    ],
)
def test_render_reference_table_expected_rows(table_id: str, expected_fragments: tuple[str, ...]) -> None:
    """Reference table templates derive expected converter and DuckDB rows live from source mappings."""
    rendered = render_reference_table(table_id)
    for fragment in expected_fragments:
        assert fragment in rendered


def test_render_pipeline_spec_preset_overrides_pass() -> None:
    """Preset hierarchy tables render public preset bindings and added preset fields."""
    xml_rendered = render_pipeline_spec("xmltopyarrow")
    assert "`PRECOMPUTED_SCHEMAS`" in xml_rendered
    assert "`DEFAULT_FILE_PREPROCESSORS`" in xml_rendered
    assert "`(validate_md5_sidecar,)`" in xml_rendered

    xsv_rendered = render_pipeline_spec("xsv")
    assert "### Preset Pipeline: `uniprot_idmapping` (`UniProtIdMappingSettings`)" in xsv_rendered
    assert "| `partition_buckets` | Added preset field | — | `64` |" in xsv_rendered


@pytest.mark.parametrize(
    ("data_type", "expected_label"),
    [
        pytest.param(pa.bool_(), "`bool`", id="bool"),
        pytest.param(pa.int32(), "`int`", id="int32"),
        pytest.param(pa.uint64(), "`int`", id="uint64"),
        pytest.param(pa.float64(), "`float`", id="float64"),
        pytest.param(pa.decimal128(10, 2), "`decimal.Decimal`", id="decimal128"),
        pytest.param(pa.date32(), "`datetime.date`", id="date32"),
        pytest.param(pa.time64("us"), "`datetime.time`", id="time64"),
        pytest.param(pa.timestamp("us", tz="UTC"), "`datetime.datetime` (UTC)", id="timestamp-utc"),
        pytest.param(pa.timestamp("us"), "`datetime.datetime` (naive)", id="timestamp-naive"),
        pytest.param(pa.binary(), "`bytes`", id="binary"),
        pytest.param(pa.string(), "`str`", id="string"),
        pytest.param(pa.list_(pa.string()), "`list[str]`", id="list-str"),
        pytest.param(pa.null(), "`null`", id="fallback-null"),
    ],
)
def test_pyarrow_to_python_type_label_pass_scalar_list_and_fallback_types(
    data_type: pa.DataType,
    expected_label: str,
) -> None:
    """pyarrow_to_python_type_label maps scalar, list, and fallback Arrow DataTypes to Python type labels."""
    assert pyarrow_to_python_type_label(data_type) == expected_label
