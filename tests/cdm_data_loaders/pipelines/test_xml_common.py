"""Unit tests for shared XML pipeline helpers in cdm_data_loaders.pipelines.xml_common."""

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from lxml import etree

from cdm_data_loaders.core.buffer import DictBuffer
from cdm_data_loaders.core.registry import ArrowSchemaRegistry, TransformContext
from cdm_data_loaders.pipelines.xml_common import (
    _log_interval_final,
    buffer_single_tag_element,
    process_single_tag_xml_file,
    stream_and_buffer_element_iter,
)


def _convert_id_one(_el: etree._Element) -> dict[str, str]:
    return {"id": "1"}


def _no_transform(_table: str) -> None:
    return None


def test_log_interval_final_logs_single_entry_when_not_on_boundary(
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """Verify _log_interval_final logs when n_entries == 0 and (n_entries + 1) % log_interval != 0."""
    xml_path = tmp_path / "one.xml"
    xml_path.write_text("<root/>", encoding="utf-8")
    with caplog.at_level(logging.DEBUG, logger="cdm_data_loaders.pipelines.xml_common"):
        _log_interval_final(0, xml_path, 10)
    assert "Processed 1 entries from one.xml" in caplog.text


def test_buffer_single_tag_element_uses_buffer_schema_map(tmp_path: Path) -> None:
    """Verify buffer_single_tag_element passes buffer.schema_map to route_and_coerce_record."""
    schema = pa.schema([pa.field("id", pa.string()), pa.field("count", pa.int64())])
    buf = DictBuffer(max_items=1, schema_map=ArrowSchemaRegistry(registry={"items": schema}))
    el = etree.fromstring(b"<item><id>a</id><count>7</count></item>")
    ctx = TransformContext.for_source("items", str(tmp_path / "a.xml"))

    def _convert(_el: etree._Element) -> dict[str, str]:
        return {"id": "a", "count": "7"}

    flushed = list(
        buffer_single_tag_element(
            el,
            convert_element=_convert,
            transform=None,
            buffer=buf,
            ctx=ctx,
        )
    )
    assert len(flushed) == 1
    table = flushed[0].data
    assert isinstance(table, pa.Table)
    assert table.schema.equals(schema)
    assert table.to_pylist() == [{"id": "a", "count": 7}]


def test_process_single_tag_xml_file_missing_target_raises(tmp_path: Path) -> None:
    """Verify process_single_tag_xml_file raises RuntimeError when table_name is missing."""
    xml_path = tmp_path / "ns.xml"
    xml_path.write_text('<root xmlns="http://example.com"><item><id>1</id></item></root>', encoding="utf-8")
    missing_settings = SimpleNamespace(table_name=None, xml_tag="item", log_interval=10, buffer_size=10)

    with pytest.raises(RuntimeError, match=r"^Expected table_name and xml_tag when xml_level is not set$"):
        list(process_single_tag_xml_file(xml_path, missing_settings, _convert_id_one))


@pytest.mark.parametrize(
    ("resolve_default_ns", "expected_rows"),
    [
        pytest.param(False, [], id="ignore-default-ns"),
        pytest.param(True, [{"id": "1"}], id="resolve-default-ns"),
    ],
)
def test_process_single_tag_xml_file_default_namespace(
    tmp_path: Path,
    resolve_default_ns: bool,
    expected_rows: list[dict[str, str]],
) -> None:
    """Verify process_single_tag_xml_file matches namespaced tags only when resolve_default_ns is True."""
    xml_path = tmp_path / "ns.xml"
    xml_path.write_text('<root xmlns="http://example.com"><item><id>1</id></item></root>', encoding="utf-8")
    schema = pa.schema([pa.field("id", pa.string())])
    settings = SimpleNamespace(
        table_name="items",
        xml_tag="item",
        log_interval=10,
        buffer_size=10,
        schema_map=ArrowSchemaRegistry(registry={"items": schema}),
        use_pyarrow=True,
        get_table_transform=_no_transform,
    )

    pages = list(
        process_single_tag_xml_file(
            xml_path,
            settings,
            _convert_id_one,
            resolve_default_ns=resolve_default_ns,
        )
    )

    rows = [row for page in pages for row in page.data.to_pylist()]
    assert rows == expected_rows


def test_stream_and_buffer_element_iter_flushes_and_logs(caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:
    """Verify stream_and_buffer_element_iter iterates elements, logs progress, and flushes buffered items."""
    xml_path = tmp_path / "items.xml"
    xml_path.write_text("<root/>", encoding="utf-8")
    buf = DictBuffer(max_items=10, use_pyarrow=False)
    elements = [etree.fromstring(b"<item/>"), etree.fromstring(b"<item/>")]

    def _buffer_fn(_el: etree._Element) -> list[Any]:
        return list(buf.add_items({"items": [{"id": "x"}]}))

    with caplog.at_level(logging.DEBUG, logger="cdm_data_loaders.pipelines.xml_common"):
        pages = list(stream_and_buffer_element_iter(xml_path, elements, 2, buf, _buffer_fn))

    assert len(pages) == 1
    assert pages[0].data == [{"id": "x"}, {"id": "x"}]
    assert "Processed 2 entries" in caplog.text
