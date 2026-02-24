# tests/test_docker_compose.py
"""
Static verification that docker-compose.yml is valid YAML and defines all
required services, networks, volumes, and safety constraints.
"""

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).parent.parent
COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"
ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"


def _compose() -> dict:
    return yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))


def test_compose_file_exists() -> None:
    assert COMPOSE_PATH.exists()


def test_compose_is_valid_yaml() -> None:
    assert isinstance(_compose(), dict)


def test_compose_has_all_services() -> None:
    services = _compose().get("services", {})
    for svc in ["db", "jupyter", "streamlit", "pipeline"]:
        assert svc in services, f"Missing service '{svc}'"


def test_compose_db_uses_pgvector_image() -> None:
    assert "pgvector" in _compose()["services"]["db"]["image"]


def test_compose_db_has_healthcheck() -> None:
    assert "healthcheck" in _compose()["services"]["db"]


def test_compose_pipeline_has_pipeline_profile() -> None:
    profiles = _compose()["services"]["pipeline"].get("profiles", [])
    assert "pipeline" in profiles


def test_compose_has_psalms_net_network() -> None:
    assert "psalms_net" in _compose().get("networks", {})


def test_compose_has_pg_data_volume() -> None:
    assert "pg_data" in _compose().get("volumes", {})


def test_compose_no_hardcoded_passwords() -> None:
    """Credentials must use ${VAR:-default} syntax, not bare literals."""
    raw = COMPOSE_PATH.read_text(encoding="utf-8")
    # Reject POSTGRES_PASSWORD: <bare_value> not wrapped in ${}
    bad = re.findall(r"POSTGRES_PASSWORD:\s+(?!\$\{)[^\s#]", raw)
    assert not bad, f"Hard-coded password found: {bad}"


def test_compose_jupyter_depends_on_db() -> None:
    depends = _compose()["services"]["jupyter"].get("depends_on", {})
    assert "db" in depends


def test_env_example_exists() -> None:
    assert ENV_EXAMPLE_PATH.exists()


def test_env_example_documents_required_vars() -> None:
    content = ENV_EXAMPLE_PATH.read_text()
    for var in ["POSTGRES_PASSWORD", "POSTGRES_USER", "POSTGRES_DB", "LLM_PROVIDER"]:
        assert var in content, f"'{var}' missing from .env.example"
