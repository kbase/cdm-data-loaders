import requests
import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


BASE_URL = "https://api.microbiomedata.org"
OUTPUT_PATH = "output/sample_data"

STUDIES = [
    "nmdc:sty-11-34xj1150",
    "nmdc:sty-11-hht5sb92",
    "nmdc:sty-11-nxrz9m96",
    "nmdc:sty-11-pzmd0x14",
]

logging.basicConfig(level=logging.INFO)


sample_data_schema = StructType(
    [
        StructField("biosample_id", StringType(), True),
        StructField("data_object_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("data_category", StringType(), True),
        StructField("data_object_type", StringType(), True),
        StructField("file_size_bytes", LongType(), True),
        StructField("md5_checksum", StringType(), True),
        StructField("url", StringType(), True),
        StructField("was_generated_by", StringType(), True),
    ]
)


def normalize_biosample_id(bid):
    if not bid:
        return None
    return bid if bid.startswith("nmdc:") else f"nmdc:{bid}"


def fetch_raw_data(study_id, retries=3):
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

            return data

        except Exception as e:
            logging.warning(f"{study_id} attempt {attempt + 1} failed: {e}")
            time.sleep(2**attempt)

    return []


def extract_records(study_id):
    try:
        raw_data = fetch_raw_data(study_id)

        results = []

        for record in raw_data:
            try:
                metadata = record.get("metadata") or {}
                if not isinstance(metadata, dict):
                    metadata = {}

                bid = record.get("biosample_id") or metadata.get("biosample_id")
                bid = normalize_biosample_id(bid)

                if not bid:
                    continue

                d_objects = record.get("data_objects") or []

                for dobj in d_objects:
                    try:
                        results.append(
                            (
                                bid,
                                dobj.get("id"),
                                dobj.get("name"),
                                dobj.get("type"),
                                dobj.get("data_category"),
                                dobj.get("data_object_type"),
                                dobj.get("file_size_bytes"),
                                dobj.get("md5_checksum"),
                                dobj.get("url"),
                                dobj.get("was_generated_by"),
                            )
                        )
                    except Exception:
                        continue

            except Exception:
                continue

        return results

    except Exception as e:
        logging.error(f"Fatal error in {study_id}: {e}")
        return []


def main():
    spark = SparkSession.builder.appName("NMDC_Table3_Extraction").config("spark.driver.memory", "4g").getOrCreate()

    logging.info(f"Starting Table 3 Extraction for {len(STUDIES)} studies")

    study_rdd = spark.sparkContext.parallelize(STUDIES, numSlices=2)

    def process_study(sid):
        try:
            logging.debug(f"Processing {sid}")
            return extract_records(sid)
        except Exception as e:
            logging.error(f"Error processing {sid}: {e}")
            return []

    rows_rdd = study_rdd.flatMap(process_study)

    df = spark.createDataFrame(rows_rdd, schema=sample_data_schema)

    df_final = df.dropDuplicates().cache()

    logging.info("Calculating total entries:")
    total = df_final.count()
    logging.info(f"Total entries: {total}")

    # Debug
    df_final.show(10, truncate=True)

    df_final.write.mode("overwrite").parquet(OUTPUT_PATH)
    logging.info(f"Saved Table 3 to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
