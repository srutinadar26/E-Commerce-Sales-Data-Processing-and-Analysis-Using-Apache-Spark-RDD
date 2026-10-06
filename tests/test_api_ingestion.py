"""
tests/test_api_ingestion.py
----------------------------
Tests for the DummyJSON API ingestion layer (src/api_ingestion.py).
These tests run offline by mocking the HTTP request.
"""

import os
import sys
import csv
import json
import pytest
from unittest.mock import patch, MagicMock

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.api_ingestion import DummyJSONIngestor, run_api_ingestion


# -- Fake API response fixture ------------------------------------------------

FAKE_PRODUCTS = [
    {
        "id": 1,
        "title": "Laptop Ultra 15",
        "category": "laptops",
        "price": 999.99,
        "rating": 4.5,
    },
    {
        "id": 2,
        "title": "Wireless Headphones",
        "category": "audio",
        "price": 49.99,
        "rating": 4.2,
    },
    {
        "id": 3,
        "title": "Running Shoes Pro",
        "category": "footwear",
        "price": 89.99,
        "rating": 4.8,
    },
]

FAKE_API_RESPONSE = {"products": FAKE_PRODUCTS, "total": 3, "skip": 0, "limit": 100}


# -- Tests --------------------------------------------------------------------

class TestDummyJSONIngestor:

    def test_fetch_products_success(self, tmp_path):
        """Successful API fetch populates self.products."""
        ingestor = DummyJSONIngestor(str(tmp_path / "out.csv"), num_records=10)

        mock_response = MagicMock()
        mock_response.json.return_value = FAKE_API_RESPONSE
        mock_response.raise_for_status = MagicMock()

        with patch("src.api_ingestion.requests.get", return_value=mock_response):
            ingestor.fetch_products()

        assert len(ingestor.products) == 3
        assert ingestor.products[0]["title"] == "Laptop Ultra 15"

    def test_fetch_products_network_error(self, tmp_path):
        """Network error propagates as expected."""
        ingestor = DummyJSONIngestor(str(tmp_path / "out.csv"), num_records=10)

        with patch("src.api_ingestion.requests.get", side_effect=ConnectionError("offline")):
            with pytest.raises(ConnectionError):
                ingestor.fetch_products()

    def test_generate_transactions_writes_csv(self, tmp_path):
        """generate_transactions() writes a valid CSV with the correct schema."""
        out_path = str(tmp_path / "output.csv")
        ingestor = DummyJSONIngestor(out_path, num_records=50)
        ingestor.products = FAKE_PRODUCTS
        ingestor.generate_transactions()

        assert os.path.exists(out_path)

        with open(out_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 50

        required_fields = {
            "transaction_id", "date", "customer_id", "product_id",
            "product_name", "category", "quantity", "unit_price",
            "discount", "payment_method", "city", "state",
            "order_status", "rating",
        }
        assert required_fields.issubset(set(rows[0].keys()))

    def test_generate_transactions_uses_api_prices(self, tmp_path):
        """Product prices come from the API (modulo injected anomalies)."""
        out_path = str(tmp_path / "output.csv")
        ingestor = DummyJSONIngestor(out_path, num_records=200)
        ingestor.products = FAKE_PRODUCTS
        ingestor.generate_transactions()

        api_prices = {p["price"] for p in FAKE_PRODUCTS}

        with open(out_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                price = abs(float(row["unit_price"]))  # anomalies can be negative
                assert price in api_prices, f"Unexpected price: {price}"

    def test_run_api_ingestion_end_to_end(self, tmp_path):
        """Full run_api_ingestion() call produces a CSV via mocked HTTP."""
        out_path = str(tmp_path / "api_out.csv")

        mock_response = MagicMock()
        mock_response.json.return_value = FAKE_API_RESPONSE
        mock_response.raise_for_status = MagicMock()

        with patch("src.api_ingestion.requests.get", return_value=mock_response):
            run_api_ingestion(out_path, num_records=30)

        assert os.path.exists(out_path)
        with open(out_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 30
