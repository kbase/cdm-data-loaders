"""Entity registry for the NCBI assembly-report reference dataset."""

from pydantic import BaseModel

from tests.data.ncbi_datasets.openapi_pydantic import V2reportsAssemblyDataReport

ENTITY_MODELS: dict[str, type[BaseModel]] = {"dataset": V2reportsAssemblyDataReport}
