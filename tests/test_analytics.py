"""
tests/test_analytics.py
------------------------
Integration-style tests that spin up a local SparkContext and verify
the RDD analytics functions produce correct results.

Run with:
    pytest tests/ -v
"""

import os
import sys
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


# -- SparkContext fixture ------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    from src.spark_session import get_spark_session, stop_spark_session
    s = get_spark_session(app_name="TestSession", log_level="ERROR")
    yield s
    stop_spark_session(s)


@pytest.fixture(scope="module")
def sc(spark):
    return spark.sparkContext


# -- Sample clean RDD ----------------------------------------------------------

SAMPLE_ROWS = [
    {
        "transaction_id": "TXN0000001", "date": "2024-01-10",
        "customer_id": "CUST00001", "product_id": "PROD10001",
        "product_name": "Laptop Pro 15", "category": "Electronics",
        "quantity": 2, "unit_price": 1000.0, "discount": 0.10,
        "payment_method": "Credit Card", "city": "New York",
        "state": "New York", "order_status": "Delivered",
        "rating": 4.5, "revenue": 1800.0,
    },
    {
        "transaction_id": "TXN0000002", "date": "2024-02-15",
        "customer_id": "CUST00002", "product_id": "PROD10002",
        "product_name": "Yoga Mat Non-slip", "category": "Sports",
        "quantity": 3, "unit_price": 30.0, "discount": 0.0,
        "payment_method": "PayPal", "city": "Los Angeles",
        "state": "California", "order_status": "Shipped",
        "rating": 4.0, "revenue": 90.0,
    },
    {
        "transaction_id": "TXN0000003", "date": "2024-01-20",
        "customer_id": "CUST00001", "product_id": "PROD10003",
        "product_name": "Python Crash Course", "category": "Books",
        "quantity": 1, "unit_price": 30.0, "discount": 0.05,
        "payment_method": "Credit Card", "city": "New York",
        "state": "New York", "order_status": "Cancelled",
        "rating": 5.0, "revenue": 28.5,
    },
    {
        "transaction_id": "TXN0000004", "date": "2024-03-05",
        "customer_id": "CUST00003", "product_id": "PROD10001",
        "product_name": "Laptop Pro 15", "category": "Electronics",
        "quantity": 1, "unit_price": 1000.0, "discount": 0.20,
        "payment_method": "Debit Card", "city": "Chicago",
        "state": "Illinois", "order_status": "Delivered",
        "rating": 3.5, "revenue": 800.0,
    },
]


@pytest.fixture(scope="module")
def sample_rdd(sc):
    return sc.parallelize(SAMPLE_ROWS)


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------

class TestSummaryStatistics:
    def test_total_count(self, sample_rdd):
        assert sample_rdd.count() == 4

    def test_total_revenue(self, sample_rdd):
        total = sample_rdd.map(lambda r: r["revenue"]).reduce(lambda a, b: a + b)
        assert total == pytest.approx(1800.0 + 90.0 + 28.5 + 800.0)

    def test_reduce_max_revenue(self, sample_rdd):
        """reduce() can find the max revenue transaction."""
        max_rev = sample_rdd.map(lambda r: r["revenue"]).reduce(max)
        assert max_rev == pytest.approx(1800.0)


class TestMapFilter:
    def test_filter_delivered_orders(self, sample_rdd):
        delivered = sample_rdd.filter(lambda r: r["order_status"] == "Delivered")
        assert delivered.count() == 2

    def test_map_extracts_category(self, sample_rdd):
        categories = sample_rdd.map(lambda r: r["category"]).collect()
        assert set(categories) == {"Electronics", "Sports", "Books"}

    def test_distinct_categories(self, sample_rdd):
        unique = sample_rdd.map(lambda r: r["category"]).distinct().count()
        assert unique == 3


class TestReduceByKey:
    def test_revenue_by_category(self, sample_rdd):
        result = dict(
            sample_rdd
            .map(lambda r: (r["category"], r["revenue"]))
            .reduceByKey(lambda a, b: a + b)
            .collect()
        )
        assert result["Electronics"] == pytest.approx(1800.0 + 800.0)
        assert result["Sports"] == pytest.approx(90.0)
        assert result["Books"] == pytest.approx(28.5)

    def test_quantity_by_product(self, sample_rdd):
        result = dict(
            sample_rdd
            .map(lambda r: (r["product_name"], r["quantity"]))
            .reduceByKey(lambda a, b: a + b)
            .collect()
        )
        assert result["Laptop Pro 15"] == 3   # 2 + 1
        assert result["Yoga Mat Non-slip"] == 3
        assert result["Python Crash Course"] == 1


class TestSortByKey:
    def test_sort_ascending(self, sample_rdd):
        sorted_cats = (
            sample_rdd
            .map(lambda r: (r["category"], 1))
            .reduceByKey(lambda a, b: a + b)
            .sortByKey()
            .keys()
            .collect()
        )
        assert sorted_cats == sorted(sorted_cats)


class TestFlatMap:
    def test_flatmap_monthly_pairs(self, sample_rdd):
        """flatMap should emit 2 items per record."""
        pairs = (
            sample_rdd
            .flatMap(lambda r: [
                (r["date"][:7] + "__revenue", r["revenue"]),
                (r["date"][:7] + "__count", 1),
            ])
            .collect()
        )
        # 4 records × 2 pairs = 8
        assert len(pairs) == 8


class TestUnion:
    def test_union_preserves_all_rows(self, sc):
        rdd1 = sc.parallelize(SAMPLE_ROWS[:2])
        rdd2 = sc.parallelize(SAMPLE_ROWS[2:])
        combined = rdd1.union(rdd2)
        assert combined.count() == 4


class TestGroupByKey:
    def test_group_by_payment_method(self, sample_rdd):
        groups = dict(
            sample_rdd
            .map(lambda r: (r["payment_method"], r["revenue"]))
            .groupByKey()
            .mapValues(list)
            .collect()
        )
        assert "Credit Card" in groups
        assert len(groups["Credit Card"]) == 2   # TXN1 + TXN3
