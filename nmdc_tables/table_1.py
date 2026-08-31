"""NMDC Table 1 pipeline.

Extracts study-person relationships from NMDC API
and writes a normalized parquet table.
"""

import logging

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import first
from pyspark.sql.types import StructField, StructType, StringType


# spark = SparkSession.builder.appName("NMDC Study Pipeline").getOrCreate()


def get_spark():
    return SparkSession.builder.appName("NMDC Study Pipeline").getOrCreate()


BASE_URL = "https://api.microbiomedata.org"

## 4 studies for testing
STUDIES = [
    "nmdc:sty-11-34xj1150",
    "nmdc:sty-11-hht5sb92",
    "nmdc:sty-11-nxrz9m96",
    "nmdc:sty-11-pzmd0x14",
]

## capital words
ROLE_MAP = {
    "Principal Investigator": "principal_investigator",
    "principal_investigator": "principal_investigator",
    "Methodology": "methodology",
    "Data curation": "data_curation",
}

logging.basicConfig(level=logging.INFO)


## helper functions to normalize role, person_id and email
def normalize_role(role: str) -> str:
    """
    Normalize role string to a standardized format.

    Converts role names to lowercase with underscores and maps
    known roles using ROLE_MAP.
    """
    if not role:
        return None
    role = role.strip()
    return ROLE_MAP.get(role, role.lower().replace(" ", "_"))


def normalize_person_id(person: dict) -> str:
    """
    Generate a normalized person identifier.

    Uses ORCID if available, otherwise falls back to name.
    """
    if not person:
        return None

    if person.get("orcid"):
        return f"orcid:{person['orcid']}"

    if person.get("name"):
        return f"name:{person['name']}"

    return None


def normalize_email(person: dict) -> str:
    """
    Normalize email address from person object.

    Returns a lowercase, trimmed email string if present,
    otherwise returns None.
    """
    if not person:
        return None

    email = person.get("email")
    if email:
        return email.strip().lower()

    return None


# Fetch API data for a single study
def fetch_study(study_id: str) -> dict | None:
    """
    Fetch study data from NMDC API.

    Args:
        study_id: NMDC study identifier.

    Returns:
        Parsed JSON response as a dictionary, or None if the request fails.
    """
    url = f"{BASE_URL}/studies/{study_id}"

    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        logging.warning(f"Error fetching {study_id}: {e}")
        return None


# Extract study-person
def extract_study_person(data):
    if not data:
        return []

    entity_id = data.get("id")
    rows = []

    # PI
    pi = data.get("principal_investigator", {})
    if pi:
        pid = normalize_person_id(pi)
        email = normalize_email(pi)

        if pid:
            rows.append(
                (
                    entity_id,
                    pid,
                    pi.get("name"),
                    email,
                    "principal_investigator",
                )
            )

    # Contributors
    for assoc in data.get("has_credit_associations", []):
        person = assoc.get("applies_to_person", {})
        roles = assoc.get("applied_roles", [])

        pid = normalize_person_id(person)
        email = normalize_email(person)

        if not pid:
            continue

        for role in roles:
            role_clean = normalize_role(role)

            rows.append(
                (
                    entity_id,
                    pid,
                    person.get("name"),
                    email,
                    role_clean,
                )
            )

    return rows


# Schema
study_person_schema = StructType(
    [
        StructField("study_id", StringType(), True),
        StructField("person_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("role", StringType(), True),
    ]
)


def main():
    logging.info("starting NMDC Study Pipeline")

    spark = get_spark()

    study_rdd = spark.sparkContext.parallelize(STUDIES)

    def process_study(study_id):
        logging.info(f"Processing {study_id}")
        data = fetch_study(study_id)
        return extract_study_person(data)

    # flatMap to flatten rows
    rows_rdd = study_rdd.flatMap(process_study)

    df = spark.createDataFrame(rows_rdd, schema=study_person_schema)

    study_person_spark = df.groupBy("study_id", "person_id", "role").agg(
        first("name", ignorenulls=True).alias("name"),
        first("email", ignorenulls=True).alias("email"),
    )

    ## Debug
    study_person_spark.show(truncate=False)
    study_person_spark.printSchema()

    ## output to parquet
    output_path = "output/study_person"
    study_person_spark.write.mode("overwrite").parquet(output_path)

    logging.info(f"Output saved to {output_path}")


if __name__ == "__main__":
    main()
