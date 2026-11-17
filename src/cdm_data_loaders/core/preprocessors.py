"""File-level pre-processor base error and settings registry table validation."""

from collections.abc import Iterable

from cdm_data_loaders.core.registry import PreprocessorRegistry, validate_registry_table_names


class FilePreprocessError(Exception):
    """Raised when a file fails pre-processing validation."""


def validate_file_preprocessors(
    settings: object,
    valid_tables: Iterable[str],
) -> None:
    """Ensure every table key in ``settings.file_preprocessors`` matches a configured input table.

    :param settings: pipeline settings instance
    :type settings: object
    :param valid_tables: valid destination or input table names for the pipeline
    :type valid_tables: Iterable[str]
    :return: ``None``
    :rtype: None
    :raises ValueError: if ``settings.file_preprocessors`` contains unknown table names
    """
    file_preprocessors: PreprocessorRegistry | None = getattr(settings, "file_preprocessors", None)
    if file_preprocessors is None or not file_preprocessors.registry:
        return
    validate_registry_table_names(file_preprocessors.registry, valid_tables, "preprocessors_module")
