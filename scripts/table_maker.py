"""Script for generating a new database table from a JSON Schema file.

Usage examples:
# Dereferenced schema (default)
python create_table.py --schema-path schema.json --db-name mydb --table-name mytable

# Non-dereferenced schema requires resources file
python create_table.py \
    --schema-path schema.json \
    --db-name mydb \
    --table-name mytable \
    --resources-file resources.json
"""

import json
from pathlib import Path
from typing import TYPE_CHECKING

import click
from berdl_notebook_utils.setup_spark_session import get_spark_session
from berdl_notebook_utils.spark.database import create_namespace_if_not_exists
from pyspark.sql import DataFrame

from cdm_data_loaders.converters.jsonschema_to_pyspark.converter import JSONSchemaToPySpark
from cdm_data_loaders.converters.jsonschema_to_pyspark.dereferencer import dereference_schema

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def create_empty_dataframe_from_json_schema(
    schema_path: str,
    db_name: str,
    table_name: str,
    resources_file: str | None = None,
) -> DataFrame:
    """Load a JSON Schema file, convert it to a PySpark StructType, and create and save an empty DataFrame with it.

    The schema file must be pre-dereferenced (no remaining `$ref`/`allOf`) -- see
    `jsonschema_to_pyspark.dereferencer.dereference_schema()` if it isn't. This
    helper uses `JSONSchemaToPySpark()` with all-default settings; construct and
    call `JSONSchemaToPySpark(...).convert_from_file(...)` yourself first if you
    need non-default converter options (e.g. `treat_unknown_as_string=False`).

    :param schema_path: path to a `.json` or `.yaml`/`.yml` JSON Schema file
    :type schema_path: str
    :param db_name: name of the database/namespace to create the table in
    :type db_name: str
    :param table_name: name of the table to create
    :type table_name: str
    :param resources_file: optional path to a JSON file with additional resources for dereferencing
    :type resources_file: str | None
    :return: an empty DataFrame whose schema matches the converted JSON Schema
    :rtype: DataFrame
    """
    spark: SparkSession = get_spark_session("dataframe_creator")
    ns = create_namespace_if_not_exists(spark, db_name, tenant_name="refdata")

    if resources_file and Path(resources_file).is_file():
        additional_resources = json.loads(Path(resources_file).read_bytes())
        dereferenced = dereference_schema(json.loads(Path(schema_path).read_bytes()), additional_resources)
        struct_type = JSONSchemaToPySpark().convert(dereferenced)
    else:
        struct_type = JSONSchemaToPySpark().convert_from_file(schema_path)

    df = spark.createDataFrame([], struct_type)
    df.writeTo(f"{ns}.{table_name}").createOrReplace()
    df.printSchema()  # empty DataFrame, schema derived from the JSON Schema file
    return df


@click.command()
@click.option(
    "--schema-path",
    "schema_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    help="Path to a .json or .yaml/.yml JSON Schema file.",
)
@click.option(
    "--db-name",
    "db_name",
    required=True,
    type=str,
    help="Name of the database/namespace to create the table in.",
)
@click.option(
    "--table-name",
    "table_name",
    required=True,
    type=str,
    help="Name of the table to create.",
)
@click.option(
    "--resources-file",
    "resources_file",
    default=None,
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    help="Optional path to a JSON file with additional resources for dereferencing.",
)
def main(
    schema_path: str,
    db_name: str,
    table_name: str,
    resources_file: str | None,
) -> None:
    """Generate a new database table from a JSON Schema file and save it to the database."""
    click.echo(f"Loading schema from: {schema_path}")
    click.echo(f"Target database: {db_name}")
    click.echo(f"Target table: {table_name}")

    create_empty_dataframe_from_json_schema(
        schema_path=schema_path,
        db_name=db_name,
        table_name=table_name,
        resources_file=resources_file,
    )

    click.echo(f"Successfully created table '{table_name}' in database '{db_name}'.")


if __name__ == "__main__":
    main()
