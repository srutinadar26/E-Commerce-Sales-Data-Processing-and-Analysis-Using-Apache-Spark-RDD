"""
src/data_cleaner.py
-------------------
RDD-based data cleaning and validation pipeline.

Demonstrates:
  • map()    – parse raw CSV rows into typed dicts
  • filter() – remove invalid / incomplete records
  • distinct() – remove exact duplicate rows
"""

from pyspark import RDD
from src.utils.logger import get_logger

logger = get_logger(__name__)

# -----------------------------------------------------------------------------
# Column index mapping (matches the CSV header)
# -----------------------------------------------------------------------------
COLUMNS = [
    "transaction_id", "date", "customer_id", "product_id", "product_name",
    "category", "quantity", "unit_price", "discount", "payment_method",
    "city", "state", "order_status", "rating",
]
COL_IDX = {name: i for i, name in enumerate(COLUMNS)}


# -----------------------------------------------------------------------------
# Pure functions (serialisable – safe to use inside RDD lambdas)
# -----------------------------------------------------------------------------

def _parse_row(line: str):
    """
    Parse a single CSV line into a tuple of typed values.

    Returns a dict on success, or None if parsing fails hard.
    The dict uses the exact COLUMNS names as keys.
    """
    try:
        parts = line.split(",")
        if len(parts) != len(COLUMNS):
            return None

        row = {}
        for i, col in enumerate(COLUMNS):
            row[col] = parts[i].strip()

        # Type coercions (keep originals as strings on failure)
        try:
            row["quantity"] = int(row["quantity"])
        except ValueError:
            row["quantity"] = None

        try:
            row["unit_price"] = float(row["unit_price"])
        except ValueError:
            row["unit_price"] = None

        try:
            row["discount"] = float(row["discount"]) if row["discount"] else 0.0
        except ValueError:
            row["discount"] = None

        try:
            row["rating"] = float(row["rating"]) if row["rating"] else None
        except ValueError:
            row["rating"] = None

        return row
    except Exception:
        return None


def _is_valid(row) -> bool:
    """
    Return True only if the row passes all validation rules:
      1. Not None (failed to parse)
      2. Required string fields are non-empty
      3. quantity  ≥ 1
      4. unit_price > 0
      5. discount in [0.0, 1.0]
      6. rating in [1.0, 5.0]  (None is allowed – treated as missing)
    """
    if row is None:
        return False

    required_str = [
        "transaction_id", "date", "customer_id", "product_id",
        "product_name", "category", "payment_method",
        "city", "state", "order_status",
    ]
    for field in required_str:
        if not row.get(field):
            return False

    if row.get("quantity") is None or row["quantity"] < 1:
        return False
    if row.get("unit_price") is None or row["unit_price"] <= 0:
        return False
    if row.get("discount") is None or not (0.0 <= row["discount"] <= 1.0):
        return False
    if row.get("rating") is not None and not (1.0 <= row["rating"] <= 5.0):
        return False

    return True


def _compute_revenue(row: dict) -> float:
    """Revenue = quantity × unit_price × (1 – discount)."""
    return round(
        row["quantity"] * row["unit_price"] * (1.0 - row["discount"]), 2
    )


def _row_to_csv_line(row: dict) -> str:
    """Serialise a cleaned row back to a CSV-safe string."""
    return ",".join([
        str(row.get("transaction_id", "")),
        str(row.get("date", "")),
        str(row.get("customer_id", "")),
        str(row.get("product_id", "")),
        str(row.get("product_name", "")),
        str(row.get("category", "")),
        str(row.get("quantity", "")),
        str(row.get("unit_price", "")),
        str(row.get("discount", "")),
        str(row.get("payment_method", "")),
        str(row.get("city", "")),
        str(row.get("state", "")),
        str(row.get("order_status", "")),
        str(row.get("rating", "")),
        str(row.get("revenue", "")),
    ])


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

def load_and_clean_rdd(sc, raw_path: str):
    """
    Load the raw CSV file into an RDD and apply the full
    parse --> validate --> deduplicate pipeline.

    Parameters
    ----------
    sc : pyspark.SparkContext
    raw_path : str
        Absolute path to data/raw/ecommerce_sales.csv

    Returns
    -------
    pyspark.RDD
        An RDD of cleaned row-dicts, cached in memory.
    """
    logger.info("Loading raw data from: %s", raw_path)

    # -- 1. Read file; skip the header line --------------------------------
    raw_rdd = sc.textFile(raw_path)
    header = raw_rdd.first()
    data_rdd = raw_rdd.filter(lambda line: line != header)

    raw_count = data_rdd.count()
    logger.info("Raw rows (excl. header): %d", raw_count)

    # -- 2. map() --> parse each CSV line into a dict ------------------------
    parsed_rdd = data_rdd.map(_parse_row)

    # -- 3. filter() --> keep only valid rows -------------------------------
    valid_rdd = parsed_rdd.filter(_is_valid)

    valid_count = valid_rdd.count()
    logger.info("Valid rows after filter: %d  (dropped %d)",
                valid_count, raw_count - valid_count)

    # -- 4. distinct() --> remove exact duplicate rows -----------------------
    #   Serialise each dict to a canonical JSON string, deduplicate, then
    #   deserialise back.  Using json (not eval) is safe and unambiguous.
    import json as _json

    def _row_to_json(r):
        # Ensure consistent key order by sorting keys
        return _json.dumps({k: r[k] for k in sorted(r.keys())})

    def _json_to_row(s):
        return _json.loads(s)

    deduped_rdd = (
        valid_rdd
        .map(_row_to_json)     # dict --> JSON string
        .distinct()             # distinct() removes duplicates
        .map(_json_to_row)     # JSON string --> dict
    )

    deduped_count = deduped_rdd.count()
    logger.info("Rows after deduplication: %d  (removed %d duplicates)",
                deduped_count, valid_count - deduped_count)

    # -- 5. map() --> add computed 'revenue' field ---------------------------
    enriched_rdd = deduped_rdd.map(
        lambda r: {**r, "revenue": _compute_revenue(r)}
    )

    # -- 6. cache() --> materialise in memory for repeated use ---------------
    enriched_rdd.cache()
    logger.info("Cleaned RDD cached. Final count: %d", enriched_rdd.count())

    return enriched_rdd


def save_cleaned_data(cleaned_rdd, output_path: str) -> None:
    """
    Save the cleaned RDD as a single CSV file (with header).

    Parameters
    ----------
    cleaned_rdd : pyspark.RDD
        The cleaned + enriched RDD from ``load_and_clean_rdd``.
    output_path : str
        Full path to the output CSV file.
    """
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    header = (
        "transaction_id,date,customer_id,product_id,product_name,"
        "category,quantity,unit_price,discount,payment_method,"
        "city,state,order_status,rating,revenue"
    )

    rows = cleaned_rdd.map(_row_to_csv_line).collect()

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for row in rows:
            f.write(row + "\n")

    logger.info("Cleaned data saved --> %s  (%d rows)", output_path, len(rows))
