from pathlib import Path

import yaml


def test_broker_network_contains_only_bot_services():
    compose = yaml.safe_load(Path("compose.yaml").read_text())
    assert compose["networks"]["broker"]["internal"] is True
    connected = {
        name
        for name, service in compose["services"].items()
        if "broker" in service.get("networks", [])
    }
    assert connected == {"observer", "worker", "rabbitmq"}
    assert not compose["services"]["rabbitmq"].get("ports")
    assert not compose["services"]["postgres"].get("ports")
    assert compose["services"]["postgres"]["image"] == "postgres:18.6-bookworm"
    assert "postgres-data:/var/lib/postgresql" in compose["services"]["postgres"]["volumes"]
