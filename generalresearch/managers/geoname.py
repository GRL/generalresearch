import os
import subprocess
from pathlib import Path

from generalresearch.download_utils import download_if_modified
from generalresearch.pg_helper import PostgresConfig


def download_postal_codes(data_dir: Path):
    # https://download.geonames.org/export/zip/
    url = "https://download.geonames.org/export/zip/allCountries.zip"
    local_path = Path(os.path.join(data_dir, "geoname-postalcodes-allCountries.zip"))

    download_if_modified(url, local_path)
    return local_path


def prepare_postal_codes(local_path: Path) -> Path:
    """
    Extract allCountries.txt from the GeoNames zip and keep one row per
    (country_iso, postal_code), writing the result next to the zip as
    "<stem>-deduplicated.txt". Returns that path.

    Why: GeoNames lists a postal code once per place it covers, so the same
    (country, postal_code) can appear many times.

    How (a unzip | sort | awk pipeline, streamed so the file is never fully
    in memory):
      - sort by country (col 1), postal code (col 2), then accuracy (col 12)
        numeric descending. `-s` keeps ties in original file order.
      - awk prints only the first row of each (country, postal_code) group,
        i.e. the one with the highest accuracy. Rows with no accuracy sort as
        0, so they're kept only if nothing better exists.
    LC_ALL=C gives fast byte-wise sorting, so the grouping behaves the same
    on every machine regardless of locale.
    """
    output_path = local_path.with_name(f"{local_path.stem}-deduplicated.txt")

    # Paths are passed as $1/$2 (not interpolated) so no shell quoting needed.
    pipeline = (
        r"""unzip -p "$1" allCountries.txt"""
        r""" | LC_ALL=C sort -s -t $'\t' -k1,1 -k2,2 -k12,12nr"""
        r""" | awk -F '\t' '$1 != c || $2 != p { print; c = $1; p = $2 }'"""
        r""" > "$2" """
    )
    subprocess.run(
        ["bash", "-o", "pipefail", "-c", pipeline, "_", local_path, output_path],
        check=True,
    )

    return output_path


def import_postal_codes(pg_config: PostgresConfig, local_path: Path):
    # Takes about 5 sec
    # DELETE + COPY run in one transaction, so a failed load leaves the
    # previous data in place.

    prepared_path = prepare_postal_codes(local_path)
    with pg_config.make_connection() as conn, conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout = '60s';")
        c.execute("DELETE FROM geoname_postalcode;")
        with prepared_path.open("rb") as f, c.copy(COPY_QUERY) as copy:
            for line in f:
                copy.write(line)


COPY_QUERY = """
COPY geoname_postalcode (    
    country_iso, postal_code, place_name, 
    admin_name_1, admin_code_1,
    admin_name_2, admin_code_2,
    admin_name_3, admin_code_3,
    latitude, longitude, accuracy
)
FROM STDIN WITH (FORMAT text, DELIMITER E'\t', NULL '')
"""
