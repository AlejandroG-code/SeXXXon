import argparse
import random
from datetime import date, timedelta
from decimal import Decimal
from app.db.session import engine, Base, SessionLocal
from app.db.models import Sale
from app.core.logging import logger

CUSTOMERS = [
    'Acme Corp',
    'TechStart Inc.',
    'Global Logistics, LLC',
    'Smith, John & Sons',
    'MegaCorp "Enterprises"',
    'Alpha Beta & Gamma Co.',
    'Omni Consumer Products',
    'Cyberdyne Systems',
    'Wayne Enterprises, Inc.',
    'Stark Industries, Ltd.',
    'Dunder Mifflin, Inc.',
    'Initech "Solutions"',
    'Umbrella Corp.',
    'Hooli, Inc.',
    'Pied Piper, LLC',
]

PRODUCTS = [
    'Cloud Server "Pro" 16GB',
    'Enterprise Router, Dual-Band',
    'Standard Monitor 27"',
    'Ergonomic Keyboard, Backlit',
    'Wireless Mouse "Precision"',
    'USB-C Docking Station, 4K',
    'Noise-Cancelling Headphones, Black',
    'Standing Desk, Bamboo "Edition"',
    'Office Chair, Lumbar Support',
    'Conference Webcam 1080p, Wide-Angle',
    'Ethernet Cable 50ft, Shielded',
    'External SSD 1TB, Rugged "Armor"',
]


def generate_sales_data(count: int = 15000, seed: int = 42) -> list[dict]:
    random.seed(seed)
    sales = []
    start_date = date(2025, 1, 1)
    end_date = date(2026, 12, 31)
    date_range_days = (end_date - start_date).days

    for _ in range(count):
        sale_date = start_date + timedelta(days=random.randint(0, date_range_days))
        customer = random.choice(CUSTOMERS)
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 50)
        # Unit price between $5.00 and $1,500.00
        unit_price = Decimal(str(round(random.uniform(5.0, 1500.0), 2)))
        total_amount = Decimal(str(round(quantity * float(unit_price), 2)))

        sales.append({
            "sale_date": sale_date,
            "customer_name": customer,
            "product_name": product,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
        })
    return sales


def seed_database(count: int = 15000, clear_existing: bool = True) -> int:
    # Ensure tables exist
    Base.metadata.create_all(bind=engine)

    session = SessionLocal()
    try:
        if clear_existing:
            logger.info("Clearing existing sales records...")
            session.query(Sale).delete()
            session.commit()

        logger.info(f"Generating {count} synthetic sales records with fixed seed...")
        records = generate_sales_data(count=count)

        batch_size = 2000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            session.bulk_insert_mappings(Sale, batch)
            session.commit()
            logger.info(f"Inserted sales batch {min(i + batch_size, len(records))}/{len(records)}")

        total_count = session.query(Sale).count()
        logger.info(f"Successfully seeded database with {total_count} sales records.")
        return total_count
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed synthetic sales database.")
    parser.add_argument("--count", type=int, default=15000, help="Number of records to generate (min 10,000)")
    parser.add_argument("--append", action="store_true", help="Append instead of clearing table")
    args = parser.parse_args()

    seed_database(count=args.count, clear_existing=not args.append)
