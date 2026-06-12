from src.ingestion.bronze.runner import parse_args, run_bronze_ingestion


def test_parse_args_defaults(monkeypatch):
    monkeypatch.setattr('sys.argv', ['bronze_runner.py'])

    args = parse_args()

    assert args.bucket is None
    assert args.max_messages == 5


def test_run_bronze_ingestion_invokes_all_sources(monkeypatch):
    calls = []

    class FakeKafka:
        def consume_topic(self, topic_name, max_messages, bucket_name=None):
            calls.append(("kafka", topic_name, max_messages, bucket_name))
            return f"s3://bucket/{topic_name}.json"

    class FakePostgres:
        def fetch_table_rows(self, table_name, limit):
            calls.append(("postgres_fetch", table_name, limit))
            return [{"id": 1}]

        def persist_rows(self, table_name, rows, bucket_name=None):
            calls.append(("postgres_persist", table_name, rows, bucket_name))
            return f"s3://bucket/{table_name}.json"

    class FakeShipping:
        def fetch_shipping_payload(self):
            calls.append(("shipping_fetch",))
            return {"shipments": [{"id": "s-1"}]}

        def persist_payload(self, payload, bucket_name=None):
            calls.append(("shipping_persist", payload, bucket_name))
            return "s3://bucket/shipping.json"

    fake_kafka = FakeKafka()
    fake_postgres = FakePostgres()
    fake_shipping = FakeShipping()

    monkeypatch.setattr('src.ingestion.bronze.runner.consume_topic', fake_kafka.consume_topic)
    monkeypatch.setattr('src.ingestion.bronze.runner.fetch_table_rows', fake_postgres.fetch_table_rows)
    monkeypatch.setattr('src.ingestion.bronze.runner.persist_rows', fake_postgres.persist_rows)
    monkeypatch.setattr('src.ingestion.bronze.runner.fetch_shipping_payload', fake_shipping.fetch_shipping_payload)
    monkeypatch.setattr('src.ingestion.bronze.runner.persist_payload', fake_shipping.persist_payload)

    summary = run_bronze_ingestion(bucket='demo-bronze-bucket', max_messages=3)

    assert summary['kafka_topics'] == 2
    assert summary['postgres_tables'] == 3
    assert calls[0] == ('kafka', 'clickstream_events', 3, 'demo-bronze-bucket')
    assert any(call[0] == 'shipping_persist' for call in calls)
