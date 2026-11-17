"""Preset XSV ingestion pipeline for UniProt ID mapping files."""

from typing import Annotated, ClassVar, Final

import pyarrow as pa
from frozendict import frozendict
from pydantic import Field, FilePath
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.fields import FileGlob, file_glob_description
from cdm_data_loaders.core.registry import ArrowSchemaRegistry, TransformRegistry
from cdm_data_loaders.core.settings import (
    default_settings_with_shortcuts,
    select_cli_shortcuts,
)
from cdm_data_loaders.core.transforms.routing import emit_raw_table
from cdm_data_loaders.core.transforms.settings_mixin import LockedTransformHooksMixin
from cdm_data_loaders.datasets.uniprot.idmapping import (
    UNIPROT_IDMAPPING_OUTPUT_SCHEMAS,
    transform_uniprot_id_mapping,
)
from cdm_data_loaders.pipelines.xsv.pipeline import run_xsv_pipeline
from cdm_data_loaders.pipelines.xsv.settings import SHORTCUTS, XsvIngestSettings
from cdm_data_loaders.schemas.uniprot.idmapping.schema import (
    ID_MAPPING_SCHEMA,
    ID_MAPPING_TABLE_NAME,
    IDMAPPING_SCHEMA_FILE,
    RAW_ID_MAPPING_DESCRIPTION,
    RAW_ID_MAPPING_TABLE_NAME,
)
from cdm_data_loaders.utils.arrow_schema import with_schema_description

PIPELINE_NAME: Final[str] = "uniprot_idmapping"
DEFAULT_IDMAPPING_FILE_GLOB: Final[str] = "**/*idmapping*.dat*"

UNIPROT_IDMAPPING_SHORTCUTS: Final[frozendict[str, str]] = frozendict(
    select_cli_shortcuts(
        SHORTCUTS,
        "file_glob",
        "schema_file",
    )
)


class UniProtIdMappingSettings(LockedTransformHooksMixin, XsvIngestSettings):
    """Settings for the UniProt ID mapping XSV ingestion pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=UNIPROT_IDMAPPING_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = PIPELINE_NAME
    table_name: ClassVar[str] = ID_MAPPING_TABLE_NAME

    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_IDMAPPING_FILE_GLOB,
            description=file_glob_description("Glob pattern for UniProt ID mapping TSV files"),
        ),
    ]
    schema_file: Annotated[
        FilePath,
        Field(
            default=IDMAPPING_SCHEMA_FILE,
            description="Path to the JSON Schema file describing the UniProt ID mapping TSV columns.",
        ),
    ]

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Bind the UniProt ID mapping table transforms and output PyArrow schemas.

        :param default_pyarrow_schema: optional override for the XSV reader PyArrow schema
        :type default_pyarrow_schema: pa.Schema | None
        """
        self._table_transforms = TransformRegistry(
            registry={
                self.table_name: (emit_raw_table, transform_uniprot_id_mapping),
            }
        )
        super()._resolve_settings_schema_map(default_pyarrow_schema=default_pyarrow_schema)
        self._pyarrow_schema = with_schema_description(self.pyarrow_schema, RAW_ID_MAPPING_DESCRIPTION)
        if self.use_pyarrow:
            self._schema_map = (
                UNIPROT_IDMAPPING_OUTPUT_SCHEMAS
                if self.schema_file == IDMAPPING_SCHEMA_FILE and default_pyarrow_schema is None
                else ArrowSchemaRegistry(
                    registry={
                        RAW_ID_MAPPING_TABLE_NAME: self.pyarrow_schema,
                        ID_MAPPING_TABLE_NAME: ID_MAPPING_SCHEMA,
                    }
                )
            )


cli = CliRunner(UniProtIdMappingSettings, run_xsv_pipeline)


if __name__ == "__main__":
    cli()
