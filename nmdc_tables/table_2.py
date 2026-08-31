import time
import requests
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


BASE_URL = "https://api.microbiomedata.org"
OUTPUT_PATH = "output/study_sample"

STUDIES = [
    "nmdc:sty-11-34xj1150",
    "nmdc:sty-11-hht5sb92",
    "nmdc:sty-11-nxrz9m96",
    "nmdc:sty-11-pzmd0x14",
]


logging.basicConfig(level=logging.INFO)

study_sample_schema = StructType(
    [
        StructField("study_id", StringType(), True),
        StructField("biosample_id", StringType(), True),
    ]
)


def normalize_biosample_id(bid):
    if not bid:
        return None
    return bid if bid.startswith("nmdc:") else f"nmdc:{bid}"


def fetch_biosample_ids(study_id, retries=3):
    url = f"{BASE_URL}/data_objects/study/{study_id}"

    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=(5, 30))
            res.raise_for_status()

            try:
                data = res.json()
            except Exception:
                logging.warning(f"{study_id}: JSON decode failed")
                return []

            biosamples = set()

            for record in data:
                try:
                    metadata = record.get("metadata") or {}
                    if not isinstance(metadata, dict):
                        metadata = {}

                    bid = record.get("biosample_id") or metadata.get("biosample_id")

                    if bid:
                        biosamples.add(bid)

                except Exception:
                    continue

            return list(biosamples)

        except Exception as e:
            logging.warning(f"{study_id} attempt {attempt + 1} failed: {e}")
            time.sleep(2**attempt)

    return []


def main():
    spark = SparkSession.builder.appName("NMDC_Table2_Extraction").getOrCreate()
    study_rdd = spark.sparkContext.parallelize(STUDIES, numSlices=2)

    def process_study(study_id):
        try:
            biosample_ids = fetch_biosample_ids(study_id)
            return [(study_id, bid) for bid in biosample_ids]
        except Exception as e:
            logging.error(f"Fatal error in {study_id}: {e}")
            return []

    rows_rdd = study_rdd.flatMap(process_study)

    df = spark.createDataFrame(rows_rdd, schema=study_sample_schema)
    df_final = df.dropDuplicates()

    total = df_final.count()
    logging.info(f"Total entries: {total}")

    df_final.show(10, truncate=False)

    df_final.write.mode("overwrite").parquet(OUTPUT_PATH)

    logging.info(f"Saved data to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
