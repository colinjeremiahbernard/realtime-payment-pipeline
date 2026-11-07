"""
Layer 3 — Ingestion Tests
Validates PostgreSQL WAL CDC setup, init.sql definitions, and Debezium connector configuration.
"""
import os
import json
import re
import pytest

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def test_docker_compose_postgres_wal():
    dc_path = os.path.join(REPO_DIR, "docker", "postgres", "docker-compose.yml")
    assert os.path.exists(dc_path)
    with open(dc_path, "r", encoding="utf-8") as f:
        content = f.read()
    assert "wal_level=logical" in content, "Postgres must be started with wal_level=logical"
    assert "max_replication_slots" in content
    assert "max_wal_senders" in content

def test_postgres_init_sql():
    sql_path = os.path.join(REPO_DIR, "docker", "postgres", "init.sql")
    assert os.path.exists(sql_path)
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    
    assert "CREATE TABLE IF NOT EXISTS accounts" in sql
    assert "CREATE TABLE IF NOT EXISTS transactions" in sql
    assert "pg_create_logical_replication_slot('debezium_slot', 'pgoutput')" in sql
    assert "CREATE PUBLICATION debezium_pub" in sql
    assert "FOR TABLE accounts, transactions" in sql
    
    # Check that synthetic seed data contains accounts and transactions
    assert "INSERT INTO accounts" in sql
    assert "INSERT INTO transactions" in sql
    assert "Alice Souza" in sql
    assert "**** **** **** 1001" in sql
    assert "Loja Suspeita A" in sql
    assert "Loja Suspeita B" in sql
    assert "Loja Suspeita C" in sql

def test_connector_config_smt_and_converters():
    conn_path = os.path.join(REPO_DIR, "connectors", "postgres-cdc.json")
    with open(conn_path, "r", encoding="utf-8") as f:
        raw = f.read()
    
    # Strip template variables for json parse check
    dummy_json = re.sub(r"\{\{[A-Z0-9_]+\}\}", "test-value", raw)
    config_obj = json.loads(dummy_json)
    cfg = config_obj["config"]
    
    assert cfg["connector.class"] == "PostgresSource"
    assert cfg["plugin.name"] == "pgoutput"
    assert cfg["slot.name"] == "debezium_slot"
    assert cfg["publication.name"] == "debezium_pub"
    assert cfg["transforms"] == "unwrap"
    assert cfg["transforms.unwrap.type"] == "io.debezium.transforms.ExtractNewRecordState"
    assert cfg["transforms.unwrap.delete.handling.mode"] == "rewrite"
    assert "op,ts_ms" in cfg["transforms.unwrap.add.fields"]
    assert "AvroConverter" in cfg["key.converter"]
    assert "AvroConverter" in cfg["value.converter"]
