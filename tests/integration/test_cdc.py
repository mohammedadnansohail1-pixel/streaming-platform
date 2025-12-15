"""Integration tests for CDC with Debezium."""

import pytest
import time
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")


def is_cdc_available():
    """Check if CDC (Debezium + PostgreSQL + Kafka) is available."""
    try:
        import httpx
        import psycopg2

        # Check Debezium
        resp = httpx.get("http://localhost:8083/connectors", timeout=5)
        if resp.status_code != 200:
            return False

        # Check PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="streaming",
            user="postgres",
            password="streaming123",
        )
        conn.close()

        return True
    except Exception:
        return False


# Skip all tests if CDC is not available
pytestmark = pytest.mark.skipif(
    not is_cdc_available(), reason="CDC (Debezium/PostgreSQL/Kafka) not available"
)


class TestCDCIntegration:
    """Integration tests for CDC pipeline."""

    def setup_method(self):
        """Setup test database state."""
        import psycopg2

        self.conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="streaming",
            user="postgres",
            password="streaming123",
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

        # Clean up test data
        self.cursor.execute("DELETE FROM orders WHERE customer_id > 1000")
        self.cursor.execute("DELETE FROM customers WHERE id > 1000")

    def teardown_method(self):
        """Cleanup after tests."""
        self.cursor.execute("DELETE FROM orders WHERE customer_id > 1000")
        self.cursor.execute("DELETE FROM customers WHERE id > 1000")
        self.cursor.close()
        self.conn.close()

    def test_debezium_connector_running(self):
        """Should have Debezium connector running."""
        import httpx

        resp = httpx.get("http://localhost:8083/connectors/postgres-connector/status")
        status = resp.json()

        assert status["connector"]["state"] == "RUNNING"
        assert status["tasks"][0]["state"] == "RUNNING"

    def test_cdc_captures_insert(self):
        """Should capture INSERT events."""
        import uuid
        from core.config.loader import ConfigLoader
        from cdc.consumer import CDCConsumer

        loader = ConfigLoader()
        config = loader.load(domain="ecommerce")

        # Use unique ID for this test run
        unique_id = 1001 + int(time.time()) % 1000
        unique_email = f"test{unique_id}@example.com"

        # Insert a test record
        self.cursor.execute(
            f"""
            INSERT INTO customers (id, name, email)
            VALUES ({unique_id}, 'Test User', '{unique_email}')
        """
        )

        # Consume CDC event with unique group
        consumer = CDCConsumer(config, group_id=f"test-cdc-insert-{uuid.uuid4()}")
        consumer.subscribe(["cdc.public.customers"])

        insert_event = None
        timeout = time.time() + 10

        for event in consumer.consume(timeout=1.0, max_messages=100):
            if event["data"].get("email") == unique_email and event["op"] == "c":
                insert_event = event
                break
            if time.time() > timeout:
                break

        consumer.close()

        # Cleanup
        self.cursor.execute(f"DELETE FROM customers WHERE id = {unique_id}")

        assert insert_event is not None, "No INSERT event found"
        assert insert_event["op"] == "c"  # Create
        assert insert_event["data"]["name"] == "Test User"

    def test_cdc_captures_update(self):
        """Should capture UPDATE events."""
        from core.config.loader import ConfigLoader
        from cdc.consumer import CDCConsumer

        loader = ConfigLoader()
        config = loader.load(domain="ecommerce")

        # Insert and update
        self.cursor.execute(
            """
            INSERT INTO customers (id, name, email)
            VALUES (1002, 'Before Update', 'test1002@example.com')
        """
        )
        time.sleep(1)

        self.cursor.execute(
            """
            UPDATE customers SET name = 'After Update' WHERE id = 1002
        """
        )

        # Consume CDC events
        consumer = CDCConsumer(config, group_id="test-cdc-update")
        consumer.subscribe(["cdc.public.customers"])

        update_event = None
        timeout = time.time() + 10

        for event in consumer.consume(timeout=1.0, max_messages=20):
            if event["data"].get("id") == 1002 and event["op"] == "u":
                update_event = event
                break
            if time.time() > timeout:
                break

        consumer.close()

        assert update_event is not None
        assert update_event["op"] == "u"  # Update
        assert update_event["data"]["name"] == "After Update"
        assert update_event["before"]["name"] == "Before Update"

    def test_cdc_captures_delete(self):
        """Should capture DELETE events."""
        from core.config.loader import ConfigLoader
        from cdc.consumer import CDCConsumer

        loader = ConfigLoader()
        config = loader.load(domain="ecommerce")

        # Insert and delete
        self.cursor.execute(
            """
            INSERT INTO customers (id, name, email)
            VALUES (1003, 'To Delete', 'test1003@example.com')
        """
        )
        time.sleep(1)

        self.cursor.execute("DELETE FROM customers WHERE id = 1003")

        # Consume CDC events
        consumer = CDCConsumer(config, group_id="test-cdc-delete")
        consumer.subscribe(["cdc.public.customers"])

        delete_event = None
        timeout = time.time() + 10

        for event in consumer.consume(timeout=1.0, max_messages=20):
            if (event.get("before") or {}).get("id") == 1003 and event["op"] == "d":
                delete_event = event
                break
            if event["data"].get("id") == 1003 and event["op"] == "d":
                delete_event = event
                break
            if time.time() > timeout:
                break

        consumer.close()

        assert delete_event is not None
        assert delete_event["op"] == "d"  # Delete

    def test_cdc_captures_order_with_customer(self):
        """Should capture related table changes."""
        from core.config.loader import ConfigLoader
        from cdc.consumer import CDCConsumer

        loader = ConfigLoader()
        config = loader.load(domain="ecommerce")

        # Insert customer and order
        self.cursor.execute(
            """
            INSERT INTO customers (id, name, email)
            VALUES (1004, 'Order Customer', 'test1004@example.com')
        """
        )
        self.cursor.execute(
            """
            INSERT INTO orders (customer_id, amount_cents, status)
            VALUES (1004, 5000, 'pending')
        """
        )

        # Consume CDC events from orders topic
        consumer = CDCConsumer(config, group_id="test-cdc-orders")
        consumer.subscribe(["cdc.public.orders"])

        order_event = None
        timeout = time.time() + 10

        for event in consumer.consume(timeout=1.0, max_messages=20):
            if event["data"].get("customer_id") == 1004:
                order_event = event
                break
            if time.time() > timeout:
                break

        consumer.close()

        assert order_event is not None
        assert order_event["data"]["amount_cents"] == 5000
        assert order_event["data"]["status"] == "pending"
