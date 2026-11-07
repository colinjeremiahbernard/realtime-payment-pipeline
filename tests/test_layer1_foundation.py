"""
Layer 1 — Foundation Tests
Validates environment configuration, topic specifications, and ACL contracts.
"""
import os
import re
import pytest

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def test_env_example_contains_all_required_keys():
    env_example_path = os.path.join(REPO_DIR, ".env.example")
    assert os.path.exists(env_example_path), ".env.example must exist"
    
    with open(env_example_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    required_keys = [
        "CONFLUENT_CLOUD_API_KEY",
        "CONFLUENT_CLOUD_API_SECRET",
        "CONFLUENT_ENV_ID",
        "CONFLUENT_CLUSTER_ID",
        "CONFLUENT_BOOTSTRAP",
        "SCHEMA_REGISTRY_URL",
        "SCHEMA_REGISTRY_API_KEY",
        "SCHEMA_REGISTRY_API_SECRET",
        "WRITER_SA_ID",
        "WRITER_API_KEY",
        "WRITER_API_SECRET",
        "READER_SA_ID",
        "READER_API_KEY",
        "READER_API_SECRET",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD"
    ]
    
    for key in required_keys:
        assert re.search(rf"^{key}=", content, re.MULTILINE), f"Missing key {key} in .env.example"

def test_gitignore_protects_secrets():
    gitignore_path = os.path.join(REPO_DIR, ".gitignore")
    assert os.path.exists(gitignore_path), ".gitignore must exist"
    with open(gitignore_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    assert ".env" in lines, ".env must be ignored in .gitignore"

def test_topic_definitions_in_setup_script():
    setup_path = os.path.join(REPO_DIR, "scripts", "01_setup_confluent.sh")
    with open(setup_path, "r", encoding="utf-8") as f:
        script = f.read()
        
    expected_topics = [
        "payments.public.transactions",
        "payments.public.accounts",
        "payments.enriched",
        "payments.fraud_alerts"
    ]
    for topic in expected_topics:
        assert topic in script, f"Topic {topic} must be defined in 01_setup_confluent.sh"
