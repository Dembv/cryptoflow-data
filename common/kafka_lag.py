"""Retard (lag) des consumer groups Kafka — utilisé par /v1/status."""

from typing import Dict, Iterable


def consumer_lag(bootstrap_servers: str, group_id: str, timeout_ms: int = 5000) -> Dict[str, int]:
    """Retourne {topic: lag cumulé} pour un consumer group. Lève en cas d'erreur Kafka."""
    from kafka import KafkaAdminClient, KafkaConsumer

    servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
    admin = KafkaAdminClient(
        bootstrap_servers=servers,
        request_timeout_ms=timeout_ms,
        api_version_auto_timeout_ms=timeout_ms,
    )
    consumer = None
    try:
        committed = admin.list_consumer_group_offsets(group_id)   # {TopicPartition: OffsetAndMetadata}
        if not committed:
            return {}
        consumer = KafkaConsumer(
            bootstrap_servers=servers,
            request_timeout_ms=30_000,                  # doit dépasser session_timeout_ms
            api_version_auto_timeout_ms=timeout_ms,
        )
        end_offsets = consumer.end_offsets(list(committed))
        lag: Dict[str, int] = {}
        for tp, meta in committed.items():
            behind = max(end_offsets.get(tp, meta.offset) - meta.offset, 0)
            lag[tp.topic] = lag.get(tp.topic, 0) + behind
        return lag
    finally:
        if consumer is not None:
            consumer.close()
        admin.close()


def lag_report(bootstrap_servers: str, groups: Iterable[str]) -> Dict[str, dict]:
    """{groupe: {"lag": {topic: n}}} ou {groupe: {"error": "..."}} — ne lève jamais."""
    report: Dict[str, dict] = {}
    for group in groups:
        try:
            report[group] = {"lag": consumer_lag(bootstrap_servers, group)}
        except Exception as exc:  # Kafka indisponible, groupe inconnu...
            report[group] = {"error": str(exc)[:200]}
    return report
