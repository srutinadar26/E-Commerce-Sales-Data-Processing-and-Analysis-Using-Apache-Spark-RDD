import requests
import csv
import logging
import random
from datetime import datetime, timedelta
from faker import Faker

logger = logging.getLogger(__name__)

class DummyJSONIngestor:
    """
    Fetches real product data from DummyJSON API and generates
    transactional e-commerce records around them to match the project schema.
    """
    def __init__(self, output_path: str, num_records: int = 2000):
        self.output_path = output_path
        self.num_records = num_records
        self.products = []
        self.faker = Faker()

    def fetch_products(self):
        """Fetch products from DummyJSON API."""
        url = "https://dummyjson.com/products?limit=100"
        try:
            logger.info(f"Fetching data from API: {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.products = data.get("products", [])
            logger.info(f"Successfully fetched {len(self.products)} products from API.")
        except Exception as e:
            logger.error(f"Failed to fetch from API: {e}")
            raise

    def generate_transactions(self):
        """Generate transactions using the real API products."""
        if not self.products:
            logger.warning("No products fetched. Falling back to local generation.")
            return

        logger.info(f"Generating {self.num_records} synthetic transactions using API products...")
        
        payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'UPI', 'Net Banking']
        states_cities = {
            'California': ['Los Angeles', 'San Francisco', 'San Diego'],
            'New York': ['New York City', 'Buffalo', 'Rochester'],
            'Texas': ['Houston', 'Austin', 'Dallas'],
            'Florida': ['Miami', 'Orlando', 'Tampa']
        }
        statuses = ['Delivered', 'Shipped', 'Processing', 'Cancelled']
        
        records = []
        for i in range(1, self.num_records + 1):
            product = random.choice(self.products)
            
            # Map API fields to our schema
            product_id = f"P{product['id']:04d}"
            product_name = product['title'].replace(",", "")
            category = product['category'].title()
            unit_price = round(float(product['price']), 2)
            rating = product['rating']
            
            # Generate transactional wrapper
            state = random.choice(list(states_cities.keys()))
            city = random.choice(states_cities[state])
            date = self.faker.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
            
            # Inject realistic data anomalies (similar to generate_data.py)
            if random.random() < 0.02:
                unit_price = -unit_price  # Invalid price
            if random.random() < 0.05:
                category = ""  # Missing category
                
            record = {
                'transaction_id': f"TXN{i:06d}",
                'date': date,
                'customer_id': f"C{random.randint(1000, 9999)}",
                'product_id': product_id,
                'product_name': product_name,
                'category': category,
                'quantity': random.randint(1, 5),
                'unit_price': unit_price,
                'discount': round(random.uniform(0, 0.3), 2),
                'payment_method': random.choice(payment_methods),
                'city': city,
                'state': state,
                'order_status': random.choice(statuses),
                'rating': rating
            }
            records.append(record)
            
        # Write to CSV
        fieldnames = list(records[0].keys())
        with open(self.output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
            
        logger.info(f"Successfully saved {len(records)} records to {self.output_path}")

def run_api_ingestion(output_path: str, num_records: int = 2000):
    ingestor = DummyJSONIngestor(output_path, num_records)
    ingestor.fetch_products()
    ingestor.generate_transactions()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_api_ingestion("data/raw/ecommerce_sales_api.csv")
