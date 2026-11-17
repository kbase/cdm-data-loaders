"""DLT pipeline orchestration for AllTheBacteria bulk OSF downloads."""

import copy
from collections.abc import Generator
from pathlib import Path
from typing import Any

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.extract.items import DataItemWithMeta
from dlt.extract.resource import DltResource

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.filesystem import build_file_ingest_kwargs
from cdm_data_loaders.datasets.all_the_bacteria.osf import (
    DATASET_NAME,
    download_osf_files,
    download_osf_index_tsv,
    iter_atb_download_links,
)
from cdm_data_loaders.pipelines.all_the_bacteria.settings import AtbSettings
from cdm_data_loaders.pipelines.runner import run_pipeline


def download_atb_index_tsv(settings: AtbSettings) -> Path:
    """Download the ATB file index TSV from OSF into ``settings.raw_data_dir``.

    :param settings: pipeline configuration
    :type settings: AtbSettings
    :return: path to the downloaded TSV file
    :rtype: Path
    """
    return download_osf_index_tsv(settings.raw_data_dir)


def get_file_download_links(
    settings: AtbSettings,
    atb_files_tsv: Path,
) -> Generator[list[dict[str, Any]]]:
    """Yield matching ATB file rows from ``atb_files_tsv`` using ``settings.pattern_matches``.

    :param settings: pipeline configuration
    :type settings: AtbSettings
    :param atb_files_tsv: path to the ATB file index TSV file
    :type atb_files_tsv: Path
    :yield: list of matching TSV row dictionaries
    :rtype: Generator[list[dict[str, Any]], None, None]
    """
    yield from iter_atb_download_links(atb_files_tsv, settings.pattern_matches)


def osf_file_downloader(
    settings: AtbSettings,
    atb_file_list: list[dict[str, Any]],
) -> Generator[DataItemWithMeta]:
    """Download OSF files into ``settings.raw_data_dir`` and yield ``downloaded_files`` table items.

    :param settings: pipeline configuration
    :type settings: AtbSettings
    :param atb_file_list: list of file descriptors to transfer
    :type atb_file_list: list[dict[str, Any]]
    :yield: ``DataItemWithMeta`` marked for ``downloaded_files``
    :rtype: Generator[DataItemWithMeta, None, None]
    """
    successful_downloads = download_osf_files(settings.raw_data_dir, atb_file_list)
    yield dlt.mark.with_table_name(successful_downloads, "downloaded_files")


@dlt.resource(name="atb_file_list")
def atb_file_list(settings: AtbSettings) -> Generator[list[dict[str, Any]]]:
    """Generate 1-item batches of matching ATB files from the OSF index TSV.

    :param settings: pipeline configuration
    :type settings: AtbSettings
    :yield: single-file batches for parallel fan-out
    :rtype: Generator[list[dict[str, Any]], None, None]
    """
    atb_files_tsv = download_atb_index_tsv(settings)
    for batch in get_file_download_links(settings, atb_files_tsv):
        if isinstance(batch, list) and len(batch) > 1:
            for row in batch:
                yield [row]
        else:
            yield batch


@dlt.transformer(name="file_downloader", data_from=atb_file_list, parallelized=True)
def file_downloader(
    atb_file_list: list[dict[str, Any]],
    settings: AtbSettings,
) -> Generator[DataItemWithMeta]:
    """Download ATB file batches on worker threads.

    :param atb_file_list: batch of file descriptors to download
    :type atb_file_list: list[dict[str, Any]]
    :param settings: pipeline configuration
    :type settings: AtbSettings
    :yield: ``DataItemWithMeta`` marked for ``downloaded_files``
    :rtype: Generator[DataItemWithMeta, None, None]
    """
    yield from osf_file_downloader(settings, atb_file_list)


def run_atb_pipeline(settings: AtbSettings) -> LoadInfo | None:
    """Run the AllTheBacteria pipeline.

    :param settings: configuration for the pipeline
    :type settings: AtbSettings
    :return: DLT ``LoadInfo`` or ``None``
    :rtype: LoadInfo | None
    """
    if isinstance(atb_file_list, DltResource) and isinstance(file_downloader, DltResource):
        file_list_resource, downloader_resource = copy.deepcopy((atb_file_list, file_downloader))
    else:
        file_list_resource, downloader_resource = atb_file_list, file_downloader
    file_list_resource.bind(settings)
    downloader_resource.bind(settings)

    return run_pipeline(
        settings=settings,
        resource=downloader_resource,
        destination_kwargs={"max_table_nesting": 0},
        **build_file_ingest_kwargs(settings, DATASET_NAME),
    )


cli = CliRunner(AtbSettings, run_atb_pipeline)

if __name__ == "__main__":
    cli()
