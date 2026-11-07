"""
Layer 2 — Contract Tests
Validates Avro schemas syntax, structure, and Schema Registry BACKWARD compatibility.
"""
import os
import json
import pytest

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def load_schema(filename):
    path = os.path.join(REPO_DIR, "schemas", filename)
    assert os.path.exists(path), f"Schema file {filename} does not exist"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def test_account_v1_schema():
    schema = load_schema("account-v1.avsc")
    assert schema["type"] == "record"
    assert schema["name"] == "Account"
    assert schema["namespace"] == "com.payments"
    
    field_names = [f["name"] for f in schema["fields"]]
    expected_fields = [
        "account_id", "holder_name", "email", "card_number",
        "card_brand", "credit_limit", "status", "created_at"
    ]
    for ef in expected_fields:
        assert ef in field_names, f"Missing field {ef} in account-v1.avsc"

def test_transaction_v1_schema():
    schema = load_schema("transaction-v1.avsc")
    assert schema["type"] == "record"
    assert schema["name"] == "Transaction"
    assert schema["namespace"] == "com.payments"
    
    field_names = [f["name"] for f in schema["fields"]]
    expected_fields = [
        "transaction_id", "account_id", "card_number", "merchant_name",
        "merchant_category", "amount", "currency", "status", "created_at"
    ]
    for ef in expected_fields:
        assert ef in field_names, f"Missing field {ef} in transaction-v1.avsc"

def test_transaction_v2_backward_compatibility():
    v1 = load_schema("transaction-v1.avsc")
    v2 = load_schema("transaction-v2.avsc")
    
    v1_fields = {f["name"]: f for f in v1["fields"]}
    v2_fields = {f["name"]: f for f in v2["fields"]}
    
    # In BACKWARD compatibility:
    # 1. Old consumers can read data written with the new schema (or new consumers can read old data).
    # Specifically in Avro / Schema Registry:
    # A new schema is BACKWARD compatible if all fields in the old schema are in the new schema,
    # and all new fields added have a DEFAULT value so readers with the new schema can read old records.
    for name, f1 in v1_fields.items():
        assert name in v2_fields, f"Field {name} in v1 was removed in v2, breaking backward compatibility!"
    
    new_field_names = set(v2_fields.keys()) - set(v1_fields.keys())
    assert "risk_score" in new_field_names, "risk_score should be added in v2"
    
    for nf in new_field_names:
        field_def = v2_fields[nf]
        assert "default" in field_def, f"New field {nf} must specify a default value for BACKWARD compatibility!"
        assert field_def["default"] is None or field_def["default"] == "" or field_def["default"] == 0
        assert "null" in field_def["type"], f"New field {nf} should be nullable"
