"""
All analytical computations are performed here using PySpark RDD operations.

RDD Operations demonstrated
----------------------------

map()         - extract key-value pairs for aggregations
filter()      - subset RDDs
flatMap()     - emit multiple items per record
distinct()    - unique product/customer lists
union()       - combine RDDs
groupByKey()  - group transactions
reduceByKey() - aggregate values by key
sortByKey()   - rank/sort results
count()       - count rows
collect()     - pull results to driver
first()       - inspect one element
take()        - inspect first N elements
reduce()      - fold an RDD to a single value
cache()       - materialise frequently reused RDDs
"""

from __future__ import annotations

import os
import csv
from typing import List, Dict

from pyspark import RDD

from src.utils.logger import get_logger


logger = get_logger(__name__)


# -----------------------------------------------------------------------------
# Helper: write analytics result to CSV
# -----------------------------------------------------------------------------

def _save_csv(
    data: list,
    fieldnames: List[str],
    output_path: str
) -> None:
    """Write a list of dictionaries to output_path as CSV."""

    os.makedirs(
        os.path.dirname(output_path),
        exist_ok=True
    )

    with open(
        output_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(data)

    logger.info(
        "Saved --> %s (%d rows)",
        output_path,
        len(data)
    )


# -----------------------------------------------------------------------------
# 1. Summary statistics
# -----------------------------------------------------------------------------

def compute_summary_statistics(
    cleaned_rdd: RDD,
    output_dir: str
) -> Dict:
    """
    Use count(), reduce(), first(), take() and distinct()
    to compute high-level KPIs.
    """

    logger.info("-- Computing summary statistics --")

    # count() --> total number of transactions
    total_transactions = cleaned_rdd.count()

    # reduce() --> total revenue
    total_revenue = (
        cleaned_rdd
        .map(lambda r: r["revenue"])
        .reduce(lambda a, b: a + b)
    )

    # reduce() --> total quantity sold
    total_qty = (
        cleaned_rdd
        .map(lambda r: r["quantity"])
        .reduce(lambda a, b: a + b)
    )

    # Derived KPI
    avg_order_value = (
        round(total_revenue / total_transactions, 2)
        if total_transactions
        else 0
    )

    # first() --> inspect first record
    first_record = cleaned_rdd.first()

    # take() --> inspect first three records
    sample_records = cleaned_rdd.take(3)

    logger.info(
        "first() --> transaction_id: %s",
        first_record.get("transaction_id")
    )

    logger.info(
        "take(3) --> transaction_ids: %s",
        [
            r.get("transaction_id")
            for r in sample_records
        ]
    )

    # distinct() --> unique product count
    unique_products = (
        cleaned_rdd
        .map(lambda r: r["product_name"])
        .distinct()
        .count()
    )

    # distinct() --> unique customer count
    unique_customers = (
        cleaned_rdd
        .map(lambda r: r["customer_id"])
        .distinct()
        .count()
    )

    summary = {
        "metric": [
            "Total Transactions",
            "Total Revenue ($)",
            "Total Quantity Sold",
            "Average Order Value ($)",
            "Unique Products",
            "Unique Customers",
        ],
        "value": [
            total_transactions,
            round(total_revenue, 2),
            total_qty,
            avg_order_value,
            unique_products,
            unique_customers,
        ],
    }

    rows = [
        {
            "metric": metric,
            "value": value
        }
        for metric, value
        in zip(
            summary["metric"],
            summary["value"]
        )
    ]

    _save_csv(
        rows,
        ["metric", "value"],
        os.path.join(
            output_dir,
            "summary_statistics.csv"
        )
    )

    logger.info(
        "Total transactions : %d",
        total_transactions
    )

    logger.info(
        "Total revenue      : $%.2f",
        total_revenue
    )

    logger.info(
        "Avg order value    : $%.2f",
        avg_order_value
    )

    logger.info(
        "Unique products    : %d",
        unique_products
    )

    logger.info(
        "Unique customers   : %d",
        unique_customers
    )

    return {
        "total_transactions": total_transactions,
        "total_revenue": round(total_revenue, 2),
        "total_qty": total_qty,
        "avg_order_value": avg_order_value,
        "unique_products": unique_products,
        "unique_customers": unique_customers,
    }


# -----------------------------------------------------------------------------
# 2. Revenue by category
# -----------------------------------------------------------------------------

def compute_revenue_by_category(
    cleaned_rdd: RDD,
    output_dir: str
) -> list:
    """
    map()        --> (category, revenue)
    reduceByKey() --> total revenue per category
    sortByKey()  --> alphabetical order
    collect()    --> pull results to driver
    """

    logger.info("-- Revenue by category --")

    result = (
        cleaned_rdd
        .map(
            lambda r: (
                r["category"],
                r["revenue"]
            )
        )
        .reduceByKey(
            lambda a, b: a + b
        )
        .sortByKey()
        .collect()
    )

    rows = [
        {
            "category": category,
            "total_revenue": round(revenue, 2)
        }
        for category, revenue in result
    ]

    _save_csv(
        rows,
        ["category", "total_revenue"],
        os.path.join(
            output_dir,
            "revenue_by_category.csv"
        )
    )

    return rows


# -----------------------------------------------------------------------------
# 3. Top 10 products by revenue
# -----------------------------------------------------------------------------

def compute_top_products(
    cleaned_rdd: RDD,
    output_dir: str,
    n: int = 10
) -> list:
    """
    map()        --> (product_name, revenue)
    reduceByKey() --> total revenue per product
    map()        --> swap for sorting
    sortByKey()  --> descending order
    take()       --> top N
    """

    logger.info(
        "-- Top %d products by revenue --",
        n
    )

    result = (
        cleaned_rdd
        .map(
            lambda r: (
                r["product_name"],
                r["revenue"]
            )
        )
        .reduceByKey(
            lambda a, b: a + b
        )
        .map(
            lambda kv: (
                -kv[1],
                kv[0]
            )
        )
        .sortByKey()
        .take(n)
    )

    rows = [
        {
            "rank": i + 1,
            "product_name": name,
            "total_revenue": round(-revenue, 2)
        }
        for i, (revenue, name)
        in enumerate(result)
    ]

    _save_csv(
        rows,
        [
            "rank",
            "product_name",
            "total_revenue"
        ],
        os.path.join(
            output_dir,
            "top_10_products.csv"
        )
    )

    return rows


# -----------------------------------------------------------------------------
# 4. Top customers by revenue
# -----------------------------------------------------------------------------

def compute_top_customers(
    cleaned_rdd: RDD,
    output_dir: str,
    n: int = 10
) -> list:
    """
    map()        --> (customer_id, revenue)
    reduceByKey() --> total spend per customer
    sortByKey()  --> descending order
    take()       --> top N
    """

    logger.info(
        "-- Top %d customers by spend --",
        n
    )

    result = (
        cleaned_rdd
        .map(
            lambda r: (
                r["customer_id"],
                r["revenue"]
            )
        )
        .reduceByKey(
            lambda a, b: a + b
        )
        .map(
            lambda kv: (
                -kv[1],
                kv[0]
            )
        )
        .sortByKey()
        .take(n)
    )

    rows = [
        {
            "rank": i + 1,
            "customer_id": customer_id,
            "total_spend": round(-revenue, 2)
        }
        for i, (revenue, customer_id)
        in enumerate(result)
    ]

    _save_csv(
        rows,
        [
            "rank",
            "customer_id",
            "total_spend"
        ],
        os.path.join(
            output_dir,
            "top_customers.csv"
        )
    )

    return rows


# -----------------------------------------------------------------------------
# 5. Revenue by city
# -----------------------------------------------------------------------------

def compute_revenue_by_city(
    cleaned_rdd: RDD,
    output_dir: str
) -> list:
    """
    map()        --> (city, revenue)
    reduceByKey() --> aggregate revenue
    map()        --> negate for descending sort
    sortByKey()  --> descending
    collect()
    """

    logger.info("-- Revenue by city --")

    result = (
        cleaned_rdd
        .map(
            lambda r: (
                r["city"],
                r["revenue"]
            )
        )
        .reduceByKey(
            lambda a, b: a + b
        )
        .map(
            lambda kv: (
                -kv[1],
                kv[0]
            )
        )
        .sortByKey()
        .collect()
    )

    rows = [
        {
            "city": city,
            "total_revenue": round(-revenue, 2)
        }
        for revenue, city in result
    ]

    _save_csv(
        rows,
        [
            "city",
            "total_revenue"
        ],
        os.path.join(
            output_dir,
            "revenue_by_city.csv"
        )
    )

    return rows


# -----------------------------------------------------------------------------
# 6. Revenue by state
# -----------------------------------------------------------------------------

def compute_revenue_by_state(
    cleaned_rdd: RDD,
    output_dir: str
) -> list:
    """
    map()        --> (state, revenue)
    reduceByKey() --> aggregate revenue
    map()        --> negate for descending sort
    sortByKey()  --> descending
    collect()
    """

    logger.info("-- Revenue by state --")

    result = (
        cleaned_rdd
        .map(
            lambda r: (
                r["state"],
                r["revenue"]
            )
        )
        .reduceByKey(
            lambda a, b: a + b
        )
        .map(
            lambda kv: (
                -kv[1],
                kv[0]
            )
        )
        .sortByKey()
        .collect()
    )

    rows = [
        {
            "state": state,
            "total_revenue": round(-revenue, 2)
        }
        for revenue, state in result
    ]

    _save_csv(
        rows,
        [
            "state",
            "total_revenue"
        ],
        os.path.join(
            output_dir,
            "revenue_by_state.csv"
        )
    )

    return rows


# -----------------------------------------------------------------------------
# 7. Payment method analysis - groupByKey()
# -----------------------------------------------------------------------------

def compute_payment_analysis(
    cleaned_rdd: RDD,
    output_dir: str
) -> list:
    """
    Demonstrates groupByKey():

    map()         --> (payment_method, revenue)
    groupByKey()  --> group revenues by payment method
    mapValues()   --> convert ResultIterable to list
    map()         --> calculate count and total revenue
    sortByKey()   --> alphabetical order
    collect()     --> pull results to driver
    """

    logger.info("-- Payment method analysis --")

    # -------------------------------------------------------------------------
    # 1. map()
    # -------------------------------------------------------------------------

    pairs = cleaned_rdd.map(
        lambda r: (
            r["payment_method"],
            r["revenue"]
        )
    )

    # -------------------------------------------------------------------------
    # 2. groupByKey()
    # -------------------------------------------------------------------------

    grouped = (
        pairs
        .groupByKey()
        .mapValues(list)
    )

    # -------------------------------------------------------------------------
    # 3. Calculate transaction count and total revenue
    #
    # IMPORTANT:
    # sortByKey() requires a pair RDD:
    #
    # (key, value)
    #
    # Therefore:
    #
    # (payment_method, (count, total_revenue))
    # -------------------------------------------------------------------------

    result = (
        grouped
        .map(
            lambda kv: (
                kv[0],
                (
                    len(kv[1]),
                    sum(kv[1])
                )
            )
        )
        .sortByKey()
        .collect()
    )

    # -------------------------------------------------------------------------
    # 4. Prepare CSV rows
    # -------------------------------------------------------------------------

    rows = [
        {
            "payment_method": method,
            "transaction_count": count,
            "total_revenue": round(total, 2),
            "avg_transaction_value": (
                round(total / count, 2)
                if count
                else 0
            ),
        }
        for method, (count, total)
        in result
    ]

    # -------------------------------------------------------------------------
    # 5. Save CSV
    # -------------------------------------------------------------------------

    _save_csv(
        rows,
        [
            "payment_method",
            "transaction_count",
            "total_revenue",
            "avg_transaction_value"
        ],
        os.path.join(
            output_dir,
            "payment_method_analysis.csv"
        )
    )

    return rows


# -----------------------------------------------------------------------------
# 8. Order status analysis - union()
# -----------------------------------------------------------------------------

def compute_order_status_analysis(
    cleaned_rdd: RDD,
    output_dir: str
) -> list:
    """
    Demonstrates union():

    Split the RDD into completed and non-completed orders,
    compute statistics separately,
    then union() them back together.
    """

    logger.info(
        "-- Order status analysis (union demo) --"
    )

    completed_statuses = {
        "Delivered",
        "Shipped"
    }

    # filter() --> completed orders
    completed_rdd = cleaned_rdd.filter(
        lambda r:
            r["order_status"]
            in completed_statuses
    )

    # filter() --> pending/non-completed orders
    pending_rdd = cleaned_rdd.filter(
        lambda r:
            r["order_status"]
            not in completed_statuses
    )

    def status_pairs(rdd):
        return (
            rdd
            .map(
                lambda r: (
                    r["order_status"],
                    (
                        r["revenue"],
                        1
                    )
                )
            )
            .reduceByKey(
                lambda a, b: (
                    a[0] + b[0],
                    a[1] + b[1]
                )
            )
        )

    completed_stats = status_pairs(
        completed_rdd
    )

    pending_stats = status_pairs(
        pending_rdd
    )

    # union() --> combine both RDDs
    all_stats = completed_stats.union(
        pending_stats
    )

    result = (
        all_stats
        .map(
            lambda kv: (
                -kv[1][0],
                kv[0],
                kv[1][1]
            )
        )
        .collect()
    )

    # Sort in Python
    result.sort(
        key=lambda x: x[0]
    )

    rows = [
        {
            "order_status": status,
            "total_revenue": round(
                -negative_revenue,
                2
            ),
            "transaction_count": count
        }
        for negative_revenue, status, count
        in result
    ]

    _save_csv(
        rows,
        [
            "order_status",
            "total_revenue",
            "transaction_count"
        ],
        os.path.join(
            output_dir,
            "order_status_analysis.csv"
        )
    )

    return rows


# -----------------------------------------------------------------------------
# 9. Monthly revenue - flatMap()
# -----------------------------------------------------------------------------

def compute_monthly_revenue(
    cleaned_rdd: RDD,
    output_dir: str
) -> list:
    """
    Demonstrates flatMap():

    Each transaction emits two pairs:

    (year_month__revenue, revenue)
    (year_month__count, 1)
    """

    logger.info(
        "-- Monthly revenue (flatMap demo) --"
    )

    def emit_monthly_pairs(r):
        """Emit two pairs per transaction."""

        year_month = r["date"][:7]

        return [
            (
                f"{year_month}__revenue",
                r["revenue"]
            ),
            (
                f"{year_month}__count",
                1.0
            )
        ]

    aggregated = (
        cleaned_rdd
        .flatMap(
            emit_monthly_pairs
        )
        .reduceByKey(
            lambda a, b: a + b
        )
        .sortByKey()
        .collect()
    )

    # Pivot revenue and count
    monthly = {}

    for key, value in aggregated:

        year_month, metric = key.split(
            "__"
        )

        if year_month not in monthly:
            monthly[year_month] = {
                "year_month": year_month,
                "total_revenue": 0.0,
                "transaction_count": 0
            }

        if metric == "revenue":

            monthly[year_month][
                "total_revenue"
            ] = round(value, 2)

        else:

            monthly[year_month][
                "transaction_count"
            ] = int(value)

    rows = sorted(
        monthly.values(),
        key=lambda x: x["year_month"]
    )

    _save_csv(
        rows,
        [
            "year_month",
            "total_revenue",
            "transaction_count"
        ],
        os.path.join(
            output_dir,
            "monthly_revenue.csv"
        )
    )

    return rows


# -----------------------------------------------------------------------------
# 10. Quantity sold by product
# -----------------------------------------------------------------------------

def compute_quantity_by_product(
    cleaned_rdd: RDD,
    output_dir: str
) -> list:
    """
    map()        --> (product_name, quantity)
    reduceByKey() --> total units sold
    map()        --> negate for descending sort
    sortByKey()  --> descending
    collect()
    """

    logger.info(
        "-- Quantity sold by product --"
    )

    result = (
        cleaned_rdd
        .map(
            lambda r: (
                r["product_name"],
                r["quantity"]
            )
        )
        .reduceByKey(
            lambda a, b: a + b
        )
        .map(
            lambda kv: (
                -kv[1],
                kv[0]
            )
        )
        .sortByKey()
        .collect()
    )

    rows = [
        {
            "product_name": name,
            "total_quantity": -quantity
        }
        for quantity, name in result
    ]

    _save_csv(
        rows,
        [
            "product_name",
            "total_quantity"
        ],
        os.path.join(
            output_dir,
            "quantity_by_product.csv"
        )
    )

    return rows


# -----------------------------------------------------------------------------
# Orchestrator
# -----------------------------------------------------------------------------

def run_all_analytics(
    cleaned_rdd: RDD,
    output_dir: str
) -> Dict:
    """
    Run every analytics function and return
    a combined results dictionary.
    """

    os.makedirs(
        output_dir,
        exist_ok=True
    )

    results = {}

    results["summary"] = (
        compute_summary_statistics(
            cleaned_rdd,
            output_dir
        )
    )

    results["by_category"] = (
        compute_revenue_by_category(
            cleaned_rdd,
            output_dir
        )
    )

    results["top_products"] = (
        compute_top_products(
            cleaned_rdd,
            output_dir
        )
    )

    results["top_customers"] = (
        compute_top_customers(
            cleaned_rdd,
            output_dir
        )
    )

    results["by_city"] = (
        compute_revenue_by_city(
            cleaned_rdd,
            output_dir
        )
    )

    results["by_state"] = (
        compute_revenue_by_state(
            cleaned_rdd,
            output_dir
        )
    )

    results["payment"] = (
        compute_payment_analysis(
            cleaned_rdd,
            output_dir
        )
    )

    results["order_status"] = (
        compute_order_status_analysis(
            cleaned_rdd,
            output_dir
        )
    )

    results["monthly"] = (
        compute_monthly_revenue(
            cleaned_rdd,
            output_dir
        )
    )

    results["qty_by_product"] = (
        compute_quantity_by_product(
            cleaned_rdd,
            output_dir
        )
    )

    logger.info(
        "All analytics complete. Output directory: %s",
        output_dir
    )

    return results