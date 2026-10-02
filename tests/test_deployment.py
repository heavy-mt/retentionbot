from pathlib import Path

import yaml


def test_private_broker_no_matrix_identity_or_crypto_secrets():
    compose = yaml.safe_load(Path("compose.yaml").read_text())
    assert compose["networks"]["broker"]["internal"]
    connected = {
        name
        for name, service in compose["services"].items()
        if "broker" in service.get("networks", [])
    }
    assert connected == {"observer", "worker", "rabbitmq"}
    assert not any(service.get("ports") for service in compose["services"].values())
    assert compose["services"]["postgres"]["image"] == "postgres:18.6-bookworm"
    assert "postgres-data:/var/lib/postgresql" in compose["services"]["postgres"]["volumes"]
    assert set(compose["secrets"]) == {"synapse_module_secret", "postgres_admin_password"}
    assert "matrix-nio" not in str(compose)
