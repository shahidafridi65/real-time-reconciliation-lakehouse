from src.producers.clickstream_producer import EVENT_TYPES, generate_event
from src.producers.server_log_producer import generate_log_line
from src.simulators import postgres_seeder


class FakeConnection:
    def __init__(self):
        self.statements = []

    def execute(self, statement, params=None):
        self.statements.append((str(statement), params or {}))


def test_clickstream_producer_output_contract():
    event = generate_event()

    assert event["event_type"] in EVENT_TYPES
    assert event["event_id"]
    assert event["event_time"]
    assert isinstance(event["user_id"], int)
    assert isinstance(event["product_id"], int)
    assert event["quantity"] >= 0


def test_server_log_producer_output_contract():
    log_line = generate_log_line()
    parts = log_line.split()

    assert len(parts) == 7
    assert parts[5].isdigit()
    assert parts[6].endswith("ms")


def test_postgres_seeder_writes_expected_tables():
    fake_conn = FakeConnection()

    postgres_seeder.seed_users(fake_conn)
    postgres_seeder.seed_products(fake_conn)
    postgres_seeder.seed_initial_orders(fake_conn)

    rendered_sql = "\n".join(statement for statement, _ in fake_conn.statements)

    assert "INSERT INTO users" in rendered_sql
    assert "INSERT INTO products" in rendered_sql
    assert "INSERT INTO orders" in rendered_sql
    assert len(fake_conn.statements) == 122
