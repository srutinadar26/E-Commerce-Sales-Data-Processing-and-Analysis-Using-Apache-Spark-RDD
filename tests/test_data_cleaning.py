"""
tests/test_data_cleaning.py
----------------------------
Unit tests for the data cleaning pipeline.

Run with:
    pytest tests/ -v
"""

import os
import sys
import pytest

# -- Project root --------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.data_cleaner import _parse_row, _is_valid, _compute_revenue, COLUMNS


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def make_csv_line(**overrides) -> str:
    """Build a CSV line with valid defaults; override any field via kwargs."""
    defaults = {
        "transaction_id": "TXN0000001",
        "date": "2024-03-15",
        "customer_id": "CUST00001",
        "product_id": "PROD12345",
        "product_name": "Laptop Pro 15",
        "category": "Electronics",
        "quantity": "2",
        "unit_price": "1299.99",
        "discount": "0.10",
        "payment_method": "Credit Card",
        "city": "New York",
        "state": "New York",
        "order_status": "Delivered",
        "rating": "4.5",
    }
    defaults.update(overrides)
    return ",".join(defaults[col] for col in COLUMNS)


def make_valid_row(**overrides) -> dict:
    """Return a fully valid parsed row dict."""
    row = {
        "transaction_id": "TXN0000001",
        "date": "2024-03-15",
        "customer_id": "CUST00001",
        "product_id": "PROD12345",
        "product_name": "Laptop Pro 15",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": 1299.99,
        "discount": 0.10,
        "payment_method": "Credit Card",
        "city": "New York",
        "state": "New York",
        "order_status": "Delivered",
        "rating": 4.5,
    }
    row.update(overrides)
    return row


# -----------------------------------------------------------------------------
# Test group 1 – _parse_row()
# -----------------------------------------------------------------------------

class TestParseRow:
    def test_parse_valid_line(self):
        """A well-formed CSV line should be parsed into a dict."""
        line = make_csv_line()
        row = _parse_row(line)
        assert row is not None
        assert row["transaction_id"] == "TXN0000001"
        assert row["quantity"] == 2
        assert row["unit_price"] == pytest.approx(1299.99)
        assert row["discount"] == pytest.approx(0.10)
        assert row["rating"] == pytest.approx(4.5)

    def test_parse_too_few_columns(self):
        """Lines with fewer columns than expected should return None."""
        line = "TXN0000001,2024-03-15,CUST00001"
        assert _parse_row(line) is None

    def test_parse_invalid_quantity(self):
        """Non-numeric quantity should become None (not raise)."""
        line = make_csv_line(quantity="abc")
        row = _parse_row(line)
        assert row is not None
        assert row["quantity"] is None

    def test_parse_empty_discount_defaults_to_zero(self):
        """An empty discount field should be coerced to 0.0."""
        line = make_csv_line(discount="")
        row = _parse_row(line)
        assert row is not None
        assert row["discount"] == pytest.approx(0.0)

    def test_parse_empty_rating_becomes_none(self):
        """An empty rating field should become None."""
        line = make_csv_line(rating="")
        row = _parse_row(line)
        assert row is not None
        assert row["rating"] is None


# -----------------------------------------------------------------------------
# Test group 2 – _is_valid()
# -----------------------------------------------------------------------------

class TestIsValid:
    def test_valid_row_passes(self):
        assert _is_valid(make_valid_row()) is True

    def test_none_row_fails(self):
        assert _is_valid(None) is False

    def test_missing_required_string_fails(self):
        assert _is_valid(make_valid_row(city="")) is False
        assert _is_valid(make_valid_row(payment_method="")) is False

    def test_zero_quantity_fails(self):
        assert _is_valid(make_valid_row(quantity=0)) is False

    def test_negative_quantity_fails(self):
        assert _is_valid(make_valid_row(quantity=-1)) is False

    def test_negative_unit_price_fails(self):
        assert _is_valid(make_valid_row(unit_price=-50.0)) is False

    def test_zero_unit_price_fails(self):
        assert _is_valid(make_valid_row(unit_price=0.0)) is False

    def test_discount_above_one_fails(self):
        assert _is_valid(make_valid_row(discount=1.5)) is False

    def test_negative_discount_fails(self):
        assert _is_valid(make_valid_row(discount=-0.1)) is False

    def test_rating_out_of_range_fails(self):
        assert _is_valid(make_valid_row(rating=6.0)) is False
        assert _is_valid(make_valid_row(rating=0.0)) is False

    def test_none_rating_is_allowed(self):
        """Missing rating is acceptable (not required)."""
        assert _is_valid(make_valid_row(rating=None)) is True


# -----------------------------------------------------------------------------
# Test group 3 – _compute_revenue()
# -----------------------------------------------------------------------------

class TestComputeRevenue:
    def test_no_discount(self):
        row = make_valid_row(quantity=2, unit_price=100.0, discount=0.0)
        assert _compute_revenue(row) == pytest.approx(200.0)

    def test_with_discount(self):
        row = make_valid_row(quantity=3, unit_price=100.0, discount=0.10)
        # 3 * 100 * 0.90 = 270.0
        assert _compute_revenue(row) == pytest.approx(270.0)

    def test_full_discount(self):
        row = make_valid_row(quantity=5, unit_price=50.0, discount=1.0)
        assert _compute_revenue(row) == pytest.approx(0.0)

    def test_fractional_price(self):
        row = make_valid_row(quantity=1, unit_price=29.99, discount=0.05)
        expected = round(1 * 29.99 * 0.95, 2)
        assert _compute_revenue(row) == pytest.approx(expected)
