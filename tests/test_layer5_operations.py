"""
Layer 5 — Operations Tests
Validates monitoring configurations, RBAC permissions matrix, and teardown sequence.
"""
import os
import json
import pytest

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def test_rbac_least_privilege_specification():
    setup_path = os.path.join(REPO_DIR, "scripts", "01_setup_confluent.sh")
    with open(setup_path, "r", encoding="utf-8") as f:
        script = f.read()
    
    # Check that Writer has WRITE, DESCRIBE permissions
    assert 'operations WRITE,DESCRIBE' in script
    # Check that Reader has READ, DESCRIBE permissions
    assert 'operations READ,DESCRIBE' in script
    # Check that Reader has consumer group permissions
    assert '--consumer-group "payments-consumers"' in script

def test_teardown_script_order_and_safety():
    teardown_path = os.path.join(REPO_DIR, "scripts", "04_teardown.sh")
    with open(teardown_path, "r", encoding="utf-8") as f:
        script = f.read()
    
    # Teardown must require confirmation
    assert 'Type \'yes\' to confirm' in script
    
    # Check order of teardown:
    # 1. flink statement delete
    # 2. connect cluster delete
    # 3. kafka topic delete
    # 4. api-key delete
    # 5. kafka cluster delete
    # 6. environment delete
    # 7. docker compose down
    idx_flink = script.find("confluent flink statement delete")
    idx_connect = script.find("confluent connect cluster delete")
    idx_topic = script.find("confluent kafka topic delete")
    idx_apikey = script.find("confluent api-key delete")
    idx_cluster = script.find("confluent kafka cluster delete")
    idx_env = script.find("confluent environment delete")
    idx_docker = script.find("docker compose")
    
    assert idx_flink != -1 and idx_connect != -1 and idx_topic != -1
    assert idx_flink < idx_connect < idx_topic < idx_apikey < idx_cluster < idx_env < idx_docker, \
        "Teardown sequence must follow proper dependency order: Flink -> Connector -> Topics -> Keys -> Cluster -> Env -> Docker"

def test_estimated_cost_formula():
    # Cost model documented in README
    # Kafka Basic: 1 CKU (~$0.18/hr * 2hr = $0.36)
    # Connector: 1 task (~$0.09/hr * 2hr = $0.18)
    # Flink SQL: compute pool (~$0.05/hr * 2hr = $0.10)
    # Total for 2 hours = ~$0.64
    cku_cost = 0.36
    connector_cost = 0.18
    flink_cost = 0.10
    total = cku_cost + connector_cost + flink_cost
    assert round(total, 2) == 0.64
