"""Utilitaires JSON partagés par les consumers Kafka."""

import json
from typing import Any


def safe_json(raw: bytes) -> Any:
    """Désérialise un message Kafka ; renvoie None si le contenu est invalide (au lieu de lever)."""
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
        return None
