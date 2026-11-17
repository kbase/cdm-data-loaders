"""Preset XSV ingestion pipeline for UniProt ID mapping files."""

from typing import Annotated, ClassVar, Final

import pyarrow as pa
from dlt.destinations.impl.filesystem.iceberg_adapter import iceberg_partition
from frozendict import frozendict
from pydantic import Field, FilePath, PositiveInt
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.fields import FileGlob, file_glob_description
from cdm_data_loaders.core.registry import ArrowSchemaRegistry, TransformRegistry
from cdm_data_loaders.core.settings import (
    default_settings_with_shortcuts,
    select_cli_shortcuts,
)
from cdm_data_loaders.core.transforms.hints import OutputTableHints
from cdm_data_loaders.core.transforms.routing import emit_raw_table
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
    UNIPROT_ID_COL,
)

PIPELINE_NAME: Final[str] = "uniprot_idmapping"
DEFAULT_IDMAPPING_FILE_GLOB: Final[str] = "**/*idmapping*.dat*"
DEFAULT_PARTITION_BUCKETS: Final[int] = 64

UNIPROT_IDMAPPING_SHORTCUTS: Final[frozendict[str, str]] = frozendict(
    select_cli_shortcuts(
        SHORTCUTS,
        "file_glob",
        "schema_file",
    )
)


class UniProtIdMappingSettings(XsvIngestSettings):
    """Settings for the UniProt ID mapping XSV ingestion pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=UNIPROT_IDMAPPING_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = PIPELINE_NAME
    table_name: ClassVar[str] = ID_MAPPING_TABLE_NAME
    transforms_module: ClassVar[str | None] = None
    output_schemas_module: ClassVar[str | None] = None

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
    partition_buckets: Annotated[
        PositiveInt,
        Field(
            default=DEFAULT_PARTITION_BUCKETS,
            description="Number of Iceberg hash buckets for partitioning output tables on uniprot_id.",
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
        raw_metadata = {
            **(self.pyarrow_schema.metadata or {}),
            b"comment": RAW_ID_MAPPING_DESCRIPTION.encode("utf-8"),
        }
        self._pyarrow_schema = self.pyarrow_schema.with_metadata(raw_metadata)
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

    def get_output_table_hints(self) -> tuple[OutputTableHints, ...]:
        """Return table descriptions, column descriptions, and Iceberg bucket partitions for both output tables.

        :return: output table hints for ``raw_id_mapping`` and ``id_mapping``
        :rtype: tuple[OutputTableHints, ...]
        """
        bucket_spec = iceberg_partition.bucket(self.partition_buckets, UNIPROT_ID_COL)
        return (
            OutputTableHints(
                table_name=RAW_ID_MAPPING_TABLE_NAME,
                pyarrow_schema=self.pyarrow_schema,
                iceberg_partitions=(bucket_spec,),
            ),
            OutputTableHints(
                table_name=ID_MAPPING_TABLE_NAME,
                pyarrow_schema=ID_MAPPING_SCHEMA,
                iceberg_partitions=(bucket_spec,),
            ),
        )


cli = CliRunner(UniProtIdMappingSettings, run_xsv_pipeline)


if __name__ == "__main__":
    cli()
