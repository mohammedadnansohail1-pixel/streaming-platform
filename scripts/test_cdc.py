"""Test CDC (Change Data Capture) with Debezium."""

import sys
import time
import threading
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")

import psycopg2  # noqa: E402
from core.config.loader import ConfigLoader  # noqa: E402
from cdc.consumer import CDCConsumer  # noqa: E402


def make_db_changes():
    """Make database changes to trigger CDC events."""
    time.sleep(3)  # Wait for consumer to start

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="streaming",
        user="postgres",
        password="streaming123",
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("\n[DB] Making changes...")

    # INSERT
    print("[DB] INSERT customer...")
    cur.execute(
        """
        INSERT INTO customers (name, email)
        VALUES ('Jane Smith', 'jane@example.com')
        RETURNING id
    """
    )
    customer_id = cur.fetchone()[0]
    time.sleep(1)

    # UPDATE
    print("[DB] UPDATE customer...")
    cur.execute(
        """
        UPDATE customers
        SET name = 'Jane Doe', updated_at = NOW()
        WHERE id = %s
    """,
        (customer_id,),
    )
    time.sleep(1)

    # INSERT order
    print("[DB] INSERT order...")
    cur.execute(
        """
        INSERT INTO orders (customer_id, amount_cents, status)
        VALUES (%s, 9999, 'pending')
    """,
        (customer_id,),
    )
    time.sleep(1)

    # UPDATE order
    print("[DB] UPDATE order status...")
    cur.execute(
        """
        UPDATE orders
        SET status = 'completed', updated_at = NOW()
        WHERE customer_id = %s
    """,
        (customer_id,),
    )
    time.sleep(1)

    # DELETE
    print("[DB] DELETE order...")
    cur.execute("DELETE FROM orders WHERE customer_id = %s", (customer_id,))
    time.sleep(1)

    print("[DB] DELETE customer...")
    cur.execute("DELETE FROM customers WHERE id = %s", (customer_id,))

    print("[DB] All changes complete!\n")

    cur.close()
    conn.close()


def main():
    print("=" * 60)
    print("CDC (Change Data Capture) Test")
    print("=" * 60)
    print("\nThis test will:")
    print("  1. Subscribe to CDC topics")
    print("  2. Make INSERT/UPDATE/DELETE changes")
    print("  3. Show captured events in real-time")
    print("=" * 60)

    # Load config
    loader = ConfigLoader()
    config = loader.load(domain="ecommerce")

    # Create consumer
    consumer = CDCConsumer(config, group_id="cdc-test")
    consumer.subscribe(["cdc.public.customers", "cdc.public.orders"])

    # Start DB changes in background
    db_thread = threading.Thread(target=make_db_changes)
    db_thread.start()

    print("\n[Consumer] Waiting for CDC events...\n")

    try:
        for event in consumer.consume(timeout=2.0, max_messages=6):
            op_icon = {"c": "➕", "u": "📝", "d": "❌", "r": "📷"}.get(
                event["op"], "❓"
            )

            print(
                f"{op_icon} {event['op_name']:8} | {event['table']:12} | {event['data']}"
            )

    except KeyboardInterrupt:
        print("\n\nStopping...")
    finally:
        consumer.close()
        db_thread.join(timeout=5)

    print("\n✓ CDC test complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
