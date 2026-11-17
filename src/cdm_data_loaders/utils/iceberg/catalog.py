"""PyIceberg catalog configuration and cleanup-registered catalog connectors."""

from typing import Annotated, Final

from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from pyiceberg.catalog import Catalog, load_catalog

from cdm_data_loaders.utils.iceberg.cleanup import install_iceberg_catalog_cleanup

DEFAULT_CATALOG_NAME: Final[str] = "polaris"
CatalogNonEmptyStr = Annotated[str, StringConstraints(min_length=1)]


class PolarisCatalogConfig(BaseModel):
    """Connection parameters for loading a Polaris REST or configured PyIceberg catalog."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    catalog: CatalogNonEmptyStr = DEFAULT_CATALOG_NAME
    uri: CatalogNonEmptyStr | None = None
    warehouse: CatalogNonEmptyStr | None = None
    credential: CatalogNonEmptyStr | None = None
    token: CatalogNonEmptyStr | None = None
    scope: CatalogNonEmptyStr | None = None
    properties: dict[str, str] = Field(default_factory=dict)

    def to_load_catalog_kwargs(self) -> dict[str, str]:
        """Build keyword overrides for :func:`pyiceberg.catalog.load_catalog`.

        :return: non-empty catalog property overrides
        :rtype: dict[str, str]
        """
        explicit: dict[str, str | None] = {
            "uri": self.uri,
            "warehouse": self.warehouse,
            "credential": self.credential,
            "token": self.token,
            "scope": self.scope,
        }
        merged: dict[str, str] = {
            **self.properties,
            **{key: val for key, val in explicit.items() if val is not None},
        }
        if self.uri is not None and self.uri.startswith(("http://", "https://")) and "type" not in merged:
            merged["type"] = "rest"
        return merged


def connect_catalog(name: str, **properties: str) -> Catalog:
    """Install Iceberg cleanup hooks and open a PyIceberg catalog connection.

    Use the returned catalog as a context manager (``with connect_catalog(name) as catalog:``).

    :param name: PyIceberg catalog name
    :type name: str
    :param properties: optional PyIceberg catalog property overrides
    :type properties: str
    :return: initialized PyIceberg ``Catalog`` instance
    :rtype: Catalog
    """
    install_iceberg_catalog_cleanup()
    return load_catalog(name, **properties)


def connect_polaris_catalog(config: PolarisCatalogConfig) -> Catalog:
    """Open a PyIceberg catalog connection using ``config`` merged with ``~/.pyiceberg.yaml``.

    Use the returned catalog as a context manager (``with connect_polaris_catalog(cfg) as catalog:``).

    :param config: Polaris catalog connection settings
    :type config: PolarisCatalogConfig
    :return: initialized PyIceberg ``Catalog`` instance
    :rtype: Catalog
    """
    return connect_catalog(config.catalog, **config.to_load_catalog_kwargs())
