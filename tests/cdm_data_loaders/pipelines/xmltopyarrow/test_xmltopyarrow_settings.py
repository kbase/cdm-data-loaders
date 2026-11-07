"""Tests for XmlToPyArrowSettings field validation and defaults."""

from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar, Final
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
import xmlschema
from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError

import cdm_data_loaders.pipelines.xmltopyarrow.settings as xmltopyarrow_settings_module
from cdm_data_loaders.converters.xsd_to_pyarrow import (
    DEFAULT_MAX_RECURSION_DEPTH,
    generate_pyarrow_schema,
    generate_pyarrow_schemas_at_level,
    load_schema,
)
from cdm_data_loaders.core.fields import XML_GZ_FILE_GLOB, DEFAULTS
from cdm_data_loaders.core.preprocessors import validate_md5_sidecar
from cdm_data_loaders.core.registry import ArrowSchemaRegistry
from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    ENTRY_TABLE_NAME,
    ENTRY_XML_TAG,
    PUBMED_PIPELINE_NAME,
    PUBMED_ROOT_TAG,
    PUBMED_XSD_FILE,
    UNIPROT_PIPELINE_NAME,
    UNIPROT_XSD_FILE,
    UNIREF_FIFTY,
    UNIREF_HUNDRED,
    UNIREF_NINETY,
    UNIREF_XSD_FILE,
    LevelPresetXmlToPyArrowSettings,
    PresetXmlToPyArrowSettings,
    PubMedXmlToPyArrowSettings,
    UniProtXmlToPyArrowSettings,
    UniRef50XmlToPyArrowSettings,
    UniRef90XmlToPyArrowSettings,
    UniRef100XmlToPyArrowSettings,
    XmlToPyArrowSettings,
)
from cdm_data_loaders.schemas.pubmed.schema import SCHEMAS as PUBMED_SCHEMAS
from cdm_data_loaders.schemas.uniprot.schema import SCHEMA as UNIPROT_SCHEMA
from cdm_data_loaders.schemas.uniprot.schema import SCHEMAS as UNIPROT_SCHEMAS
from cdm_data_loaders.schemas.uniref.schema import SCHEMA as UNIREF_SCHEMA
from cdm_data_loaders.schemas.uniref.schema import SCHEMAS as UNIREF_SCHEMAS
from tests.helpers import register_temp_module, settings_to_argv

UNIREF_LIKE_XSD: Final[str] = "uniref_like.xsd"


def test_xmltopyarrow_settings_pass_defaults(settings_factory: Callable[..., XmlToPyArrowSettings]) -> None:
    """file_glob defaults to '*.xml*'. buffer_size, log_interval, and max_recursion_depth default to expected values."""
    settings = settings_factory()
    assert settings.file_glob == "*.xml*"
    assert settings.buffer_size == DEFAULTS["buffer_size"]
    assert settings.log_interval == DEFAULTS["log_interval"]
    assert settings.max_recursion_depth == DEFAULT_MAX_RECURSION_DEPTH


def test_xmltopyarrow_settings_pass_custom_file_glob(
    settings_factory: Callable[..., XmlToPyArrowSettings],
) -> None:
    """file_glob accepts a custom glob pattern."""
    assert settings_factory(file_glob="*.rdf").file_glob == "*.rdf"


def test_xmltopyarrow_settings_pass_custom_xml_tag(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    copy_xsd_fixture: Callable[[str], str],
) -> None:
    """xml_tag accepts any tag name present in the XSD."""
    xsd_file = copy_xsd_fixture(UNIREF_LIKE_XSD)
    settings = settings_factory(xsd_file=xsd_file, xml_tag="entry")
    assert settings.xml_tag == "entry"


@pytest.mark.parametrize(
    "missing_field",
    ["xml_tag", "table_name", "dataset_name", "xsd_file"],
    ids=["xml_tag", "table_name", "dataset_name", "xsd_file"],
)
def test_xmltopyarrow_settings_fail_missing_required_fields(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    missing_field: str,
) -> None:
    """Omitting xml_tag, table_name, dataset_name, or xsd_file raises ValidationError."""
    with pytest.raises(ValidationError):
        settings_factory(**{missing_field: None})


def test_xmltopyarrow_settings_pass_schema_properties_populated_when_xsd_file_set(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    copy_xsd_fixture: Callable[[str], str],
) -> None:
    """With xsd_file set, xsd_schema and schema_map are populated for xml_tag."""
    xsd_file = copy_xsd_fixture(UNIREF_LIKE_XSD)
    settings = settings_factory(xsd_file=xsd_file, xml_tag="entry", table_name="entry")

    assert isinstance(settings.xsd_schema, xmlschema.XMLSchema)
    assert settings.schema_map is not None
    expected_schema = generate_pyarrow_schema(
        load_schema(xsd_file),
        "entry",
        max_recursion_depth=DEFAULT_MAX_RECURSION_DEPTH,
    )
    assert settings.schema_map == ArrowSchemaRegistry(registry={"entry": expected_schema})
    assert settings.schema_map.registry["entry"].equals(expected_schema, check_metadata=True)


@pytest.mark.parametrize("depth", [1, 2], ids=["depth-1", "depth-2"])
def test_xmltopyarrow_settings_pass_custom_max_recursion_depth_controls_pyarrow_schema(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    copy_xsd_fixture: Callable[[str], str],
    depth: int,
) -> None:
    """Custom max_recursion_depth is forwarded to generate_pyarrow_schema."""
    xsd_file = copy_xsd_fixture("self_referential.xsd")
    settings = settings_factory(xsd_file=xsd_file, xml_tag="node", table_name="node", max_recursion_depth=depth)
    expected_schema = generate_pyarrow_schema(load_schema(xsd_file), "node", max_recursion_depth=depth)

    assert settings.schema_map is not None
    assert settings.schema_map.registry["node"].equals(expected_schema, check_metadata=True)


def test_xmltopyarrow_settings_fail_non_positive_max_recursion_depth(
    settings_factory: Callable[..., XmlToPyArrowSettings],
) -> None:
    """max_recursion_depth must be a positive integer."""
    with pytest.raises(ValidationError):
        settings_factory(max_recursion_depth=0)


def test_xmltopyarrow_settings_fail_xsd_file_not_found_raises_validation_error(
    settings_factory: Callable[..., XmlToPyArrowSettings],
) -> None:
    """An xsd_file that does not exist raises ValidationError at construction."""
    with pytest.raises(ValidationError, match="Path does not point to a file"):
        settings_factory(xsd_file="does_not_exist.xsd")


def test_xmltopyarrow_settings_fail_xml_tag_not_in_xsd_raises_validation_error(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    copy_xsd_fixture: Callable[[str], str],
) -> None:
    """An xml_tag not present in xsd_file raises ValidationError at construction."""
    xsd_file = copy_xsd_fixture(UNIREF_LIKE_XSD)
    with pytest.raises(ValidationError, match="Could not generate PyArrow schema from XSD"):
        settings_factory(xsd_file=xsd_file, xml_tag="nonExistentTag")


SUBCLASS_CASES = [
    pytest.param(UniProtXmlToPyArrowSettings, UNIPROT_PIPELINE_NAME, UNIPROT_XSD_FILE, UNIPROT_SCHEMA, id="uniprot"),
    pytest.param(UniRef50XmlToPyArrowSettings, UNIREF_FIFTY, UNIREF_XSD_FILE, UNIREF_SCHEMA, id="uniref50"),
    pytest.param(UniRef90XmlToPyArrowSettings, UNIREF_NINETY, UNIREF_XSD_FILE, UNIREF_SCHEMA, id="uniref90"),
    pytest.param(UniRef100XmlToPyArrowSettings, UNIREF_HUNDRED, UNIREF_XSD_FILE, UNIREF_SCHEMA, id="uniref100"),
]


@pytest.mark.parametrize(
    ("settings_cls", "expected_dataset_name", "expected_xsd_file", "expected_schema"),
    SUBCLASS_CASES,
)
def test_subclass_xmltopyarrow_settings_pass_defaults(
    settings_factory: Callable[..., Any],
    *,
    settings_cls: type[XmlToPyArrowSettings],
    expected_dataset_name: str,
    expected_xsd_file: Path,
    expected_schema: pa.Schema,
) -> None:
    """UniProt and UniRef settings subclasses default dataset_name, table_name, xml_tag, xsd_file, and precomputed SCHEMA."""
    with patch.object(xmltopyarrow_settings_module, "generate_pyarrow_schema") as mock_gen_schema:
        settings = settings_factory(settings_cls)

    mock_gen_schema.assert_not_called()
    assert settings.dataset_name == expected_dataset_name
    assert settings.table_name == "entry"
    assert settings.xml_tag == "entry"
    assert settings.xsd_file == expected_xsd_file
    assert isinstance(settings.xsd_schema, xmlschema.XMLSchema)
    assert settings.schema_map == ArrowSchemaRegistry(registry={"entry": expected_schema})


@pytest.mark.parametrize(
    ("settings_cls", "expected_dataset_name", "expected_xsd_file", "expected_schema"),
    SUBCLASS_CASES,
)
def test_subclass_xmltopyarrow_settings_pass_cli(
    settings_factory: Callable[..., Any],
    *,
    settings_cls: type[XmlToPyArrowSettings],
    expected_dataset_name: str,
    expected_xsd_file: Path,
    expected_schema: pa.Schema,
) -> None:
    """CliApp.run populates preset defaults for UniProt and UniRef settings subclasses."""
    base_settings = settings_factory(CtsSettings)
    cli_settings = CliApp.run(settings_cls, cli_args=settings_to_argv(base_settings))
    assert cli_settings.dataset_name == expected_dataset_name
    assert cli_settings.table_name == "entry"
    assert cli_settings.xml_tag == "entry"
    assert cli_settings.xsd_file == expected_xsd_file
    assert cli_settings.schema_map == ArrowSchemaRegistry(registry={"entry": expected_schema})


@pytest.mark.parametrize(
    "settings_cls",
    [
        UniProtXmlToPyArrowSettings,
        UniRef50XmlToPyArrowSettings,
        UniRef90XmlToPyArrowSettings,
        UniRef100XmlToPyArrowSettings,
    ],
    ids=["uniprot", "uniref50", "uniref90", "uniref100"],
)
@pytest.mark.parametrize(
    ("locked_field", "flag"),
    [
        pytest.param("dataset_name", "--dataset-name", id="dataset-name"),
        pytest.param("table_name", "--table-name", id="table-name"),
        pytest.param("xml_tag", "--xml-tag", id="xml-tag"),
    ],
)
def test_subclass_xmltopyarrow_settings_fail_locked_fields(
    settings_factory: Callable[..., Any],
    settings_cls: type[XmlToPyArrowSettings],
    locked_field: str,
    flag: str,
) -> None:
    """UniProt and UniRef settings subclasses reject overrides of locked preset fields in Python and CLI."""
    assert locked_field not in settings_cls.model_fields
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        settings_factory(settings_cls, **{locked_field: "custom"})
    base_argv = settings_to_argv(settings_factory(CtsSettings))
    with pytest.raises(SettingsError, match=f"unrecognized arguments: {flag}"):
        CliApp.run(settings_cls, cli_args=[*base_argv, flag, "custom"])


def test_preset_xmltopyarrow_settings_pass_base_defaults(
    settings_factory: Callable[..., Any],
    copy_xsd_fixture: Callable[[str], str],
) -> None:
    """PresetXmlToPyArrowSettings subclasses inherit default table_name and xml_tag as 'entry' and use _PRECOMPUTED_SCHEMA."""

    class CustomPresetSettings(PresetXmlToPyArrowSettings):
        _PRECOMPUTED_SCHEMA: ClassVar[pa.Schema] = UNIREF_SCHEMA

    xsd_path = copy_xsd_fixture(UNIREF_LIKE_XSD)
    settings = settings_factory(
        CustomPresetSettings,
        dataset_name="preset_ds",
        xsd_file=xsd_path,
    )
    assert ENTRY_TABLE_NAME == "entry"
    assert ENTRY_XML_TAG == "entry"
    assert settings.table_name == ENTRY_TABLE_NAME
    assert settings.xml_tag == ENTRY_XML_TAG
    assert settings.schema_map == ArrowSchemaRegistry(registry={ENTRY_TABLE_NAME: UNIREF_SCHEMA})
    assert isinstance(settings.xsd_schema, xmlschema.XMLSchema)


def test_preset_xmltopyarrow_settings_fail_missing_precomputed_schema(
    settings_factory: Callable[..., Any],
    copy_xsd_fixture: Callable[[str], str],
) -> None:
    """PresetXmlToPyArrowSettings raises ValidationError when _PRECOMPUTED_SCHEMA is not defined on the class."""
    xsd_path = copy_xsd_fixture(UNIREF_LIKE_XSD)
    with pytest.raises(ValidationError, match="_PRECOMPUTED_SCHEMA"):
        settings_factory(
            PresetXmlToPyArrowSettings,
            dataset_name="preset_ds",
            xsd_file=xsd_path,
        )


@pytest.mark.parametrize(
    ("use_transforms", "use_output_schemas"),
    [
        pytest.param(False, False, id="default-from-input-schema"),
        pytest.param(True, False, id="transforms-only-unschematized"),
        pytest.param(False, True, id="output-schemas-only"),
        pytest.param(True, True, id="transforms-with-output-schemas"),
    ],
)
def test_xmltopyarrow_settings_pass_skips_input_schema_generation_when_transformed_or_overridden(
    monkeypatch: pytest.MonkeyPatch,
    settings_factory: Callable[..., XmlToPyArrowSettings],
    copy_xsd_fixture: Callable[[str], str],
    use_transforms: bool,
    use_output_schemas: bool,
) -> None:
    """XmlToPyArrowSettings skips generating a PyArrow schema from XSD when a transform or output schema is set."""
    xsd_file = copy_xsd_fixture(UNIREF_LIKE_XSD)
    override_schema = pa.schema([pa.field("_id", pa.string(), nullable=False)])
    hooks_mod = register_temp_module(
        monkeypatch,
        "_test_xmltopyarrow_settings_hooks",
        TABLE_TRANSFORMS={"entry": lambda row: {"entry": [row]}},
        OUTPUT_SCHEMAS={"entry": override_schema},
    )
    gen_spy = MagicMock(wraps=generate_pyarrow_schema)
    monkeypatch.setattr(xmltopyarrow_settings_module, "generate_pyarrow_schema", gen_spy)

    settings = settings_factory(
        xsd_file=xsd_file,
        xml_tag="entry",
        table_name="entry",
        transforms_module=hooks_mod if use_transforms else None,
        output_schemas_module=hooks_mod if use_output_schemas else None,
    )
    if use_output_schemas:
        expected_map: ArrowSchemaRegistry | None = ArrowSchemaRegistry(registry={"entry": override_schema})
        assert gen_spy.call_count == 0
    elif use_transforms:
        expected_map = None
        assert gen_spy.call_count == 0
    else:
        assert gen_spy.call_count == 1
        expected_map = ArrowSchemaRegistry(
            registry={
                "entry": generate_pyarrow_schema(
                    load_schema(xsd_file),
                    "entry",
                    max_recursion_depth=DEFAULT_MAX_RECURSION_DEPTH,
                )
            }
        )

    assert settings.schema_map == expected_map


def test_xmltopyarrow_settings_fail_unknown_transform_table_name(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """XmlToPyArrowSettings rejects TABLE_TRANSFORMS that reference an unknown table name."""
    transforms_mod = register_temp_module(
        monkeypatch,
        "_test_xmltopyarrow_unknown_table_transforms",
        TABLE_TRANSFORMS={"unknown_table": lambda r: {"unknown_table": [r]}},
    )
    with pytest.raises(ValidationError, match="Unknown table names"):
        settings_factory(transforms_module=transforms_mod)


def test_xmltopyarrow_settings_pass_level_mode_populates_pyarrow_schemas(
    settings_factory: Callable[..., XmlToPyArrowSettings],
) -> None:
    """In level mode, XmlToPyArrowSettings populates pyarrow_schemas for all tags at xml_level and sets pyarrow_schema to None."""
    settings = settings_factory(
        xsd_file=str(PUBMED_XSD_FILE),
        xml_level=1,
        root_tag=PUBMED_ROOT_TAG,
    )
    assert settings.xml_level == 1
    assert settings.root_tag == PUBMED_ROOT_TAG
    assert settings.table_name is None
    assert settings.xml_tag is None
    assert settings.pyarrow_schema is None
    assert set(settings.pyarrow_schemas.keys()) == {"PubmedArticle", "PubmedBookArticle", "DeleteCitation"}
    expected_schemas = generate_pyarrow_schemas_at_level(
        load_schema(PUBMED_XSD_FILE),
        1,
        root_tag=PUBMED_ROOT_TAG,
    )
    for tag, expected_schema in expected_schemas.items():
        assert settings.pyarrow_schemas[tag].equals(expected_schema, check_metadata=True)


@pytest.mark.parametrize(
    ("kwargs", "expected_match"),
    [
        pytest.param(
            {"xml_level": 1, "xml_tag": "entry"},
            "xml_level cannot be combined with xml_tag or table_name",
            id="xml_level_with_xml_tag",
        ),
        pytest.param(
            {"xml_level": 1, "table_name": "entry"},
            "xml_level cannot be combined with xml_tag or table_name",
            id="xml_level_with_table_name",
        ),
        pytest.param(
            {"xml_tag": "entry", "table_name": "entry", "root_tag": "UniRef50"},
            "root_tag can only be specified when xml_level is set",
            id="root_tag_without_xml_level",
        ),
        pytest.param(
            {"xml_level": 99},
            "Could not generate PyArrow schema from XSD",
            id="unreachable_xml_level",
        ),
    ],
)
def test_xmltopyarrow_settings_fail_mutually_exclusive_or_invalid_level_args(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    copy_xsd_fixture: Callable[[str], str],
    *,
    kwargs: dict[str, Any],
    expected_match: str,
) -> None:
    """Passing xml_level with xml_tag/table_name or root_tag without xml_level raises ValidationError."""
    xsd_file = copy_xsd_fixture(UNIREF_LIKE_XSD)
    with pytest.raises(ValidationError, match=expected_match):
        settings_factory(xsd_file=xsd_file, **kwargs)


def test_pubmed_xmltopyarrow_settings_pass_defaults_and_cli(
    settings_factory: Callable[..., Any],
    copy_xsd_fixture: Callable[[str], str],
) -> None:
    """PubMedXmlToPyArrowSettings defaults dataset_name, xml_level, root_tag, xsd_file, and precomputed SCHEMAS."""
    with patch.object(xmltopyarrow_settings_module, "generate_pyarrow_schemas_at_level") as mock_gen_schemas:
        settings = settings_factory(PubMedXmlToPyArrowSettings)

    mock_gen_schemas.assert_not_called()
    assert settings.dataset_name == PUBMED_PIPELINE_NAME
    assert settings.file_glob == XML_GZ_FILE_GLOB
    assert settings.get_file_preprocessors() == (validate_md5_sidecar,)
    assert settings.xml_level == 1
    assert settings.root_tag == PUBMED_ROOT_TAG
    assert settings.table_name is None
    assert settings.xml_tag is None
    assert settings.xsd_file == PUBMED_XSD_FILE
    assert isinstance(settings.xsd_schema, xmlschema.XMLSchema)
    assert settings.pyarrow_schema is None
    assert settings.pyarrow_schemas == dict(PUBMED_SCHEMAS)

    base_settings = settings_factory(CtsSettings)
    base_argv = settings_to_argv(base_settings)
    cli_settings = CliApp.run(PubMedXmlToPyArrowSettings, cli_args=base_argv)
    assert cli_settings.dataset_name == PUBMED_PIPELINE_NAME
    assert cli_settings.file_glob == XML_GZ_FILE_GLOB
    assert cli_settings.get_file_preprocessors() == (validate_md5_sidecar,)
    assert cli_settings.xml_level == 1
    assert cli_settings.root_tag == PUBMED_ROOT_TAG
    assert cli_settings.pyarrow_schemas == dict(PUBMED_SCHEMAS)

    for locked_field, flag in (
        ("dataset_name", "--dataset-name"),
        ("table_name", "--table-name"),
        ("xml_tag", "--xml-tag"),
        ("xml_level", "--xml-level"),
        ("root_tag", "--root-tag"),
    ):
        assert locked_field not in PubMedXmlToPyArrowSettings.model_fields
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            settings_factory(PubMedXmlToPyArrowSettings, **{locked_field: "custom"})
        with pytest.raises(SettingsError, match=f"unrecognized arguments: {flag}"):
            CliApp.run(PubMedXmlToPyArrowSettings, cli_args=[*base_argv, flag, "custom"])

    xsd_path = copy_xsd_fixture(UNIREF_LIKE_XSD)
    with pytest.raises(ValidationError, match="_PRECOMPUTED_SCHEMAS"):
        settings_factory(
            LevelPresetXmlToPyArrowSettings,
            dataset_name="preset_ds",
            xsd_file=xsd_path,
        )
    assert UNIPROT_SCHEMAS["entry"] is UNIPROT_SCHEMA
    assert UNIREF_SCHEMAS["entry"] is UNIREF_SCHEMA
