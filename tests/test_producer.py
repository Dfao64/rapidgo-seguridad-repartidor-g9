import json
import math
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from producer.main import config, encode, events, main, publish
from spark.reglas import es_evento_valido


def args(*extra):
    return config(["--rounds", "4", "--seed", "42", "--start-time", "2026-10-30T14:00:00Z", "--run-id", "test", *extra])


def test_replay_contract_ids_and_order():
    first = list(events(args()))
    assert first == list(events(args()))
    assert len(first) == len({e["event_id"] for e in first}) == 12
    assert all(es_evento_valido(json.loads(encode(e))) for e in first)
    assert first[3]["timestamp"] == "2026-10-30T14:00:05Z"
    assert first[0]["event_id"] != next(events(args("--run-id", "other")))["event_id"]
    assert first[0]["gps"] != next(events(args("--seed", "7")))["gps"]


def test_many_events_cover_states_and_alert_scenarios():
    sample = list(events(args("--rounds", "1000")))
    assert all(es_evento_valido(e) for e in sample)
    assert {e["estado"] for e in sample} == {"EN_RUTA", "PAUSADO", "ENTREGADO"}
    assert any(e["velocidad_kmh"] > 60 for e in sample)
    assert any(e["bateria_pct"] <= 15 for e in sample)
    assert any(e["desvio_metros"] > 300 for e in sample)
    assert all(e["velocidad_kmh"] == 0 for e in sample if e["estado"] != "EN_RUTA")


@pytest.mark.parametrize("option,value", [("--interval", "nan"), ("--interval", "inf"), ("--interval", "0"), ("--interval", "1.5"), ("--riders", "0"), ("--rounds", "-1"), ("--start-time", "bad"), ("--run-id", "")])
def test_invalid_config(option, value):
    with pytest.raises(SystemExit):
        args(option, value)


def test_environment_and_cli_precedence(monkeypatch):
    monkeypatch.setenv("PRODUCER_INTERVAL", "9")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    assert args().interval == 9
    assert args().bootstrap_servers == "broker:9092"
    assert args("--interval", "2").interval == 2


def test_publish_ack_key_json_and_frequency(capsys):
    client = Mock()
    client.send.return_value.get.return_value = SimpleNamespace(topic="topic", partition=1, offset=7)
    sleep = Mock()
    assert publish(args(), client, sleep=sleep, clock=lambda: 0) == 12
    assert [c.args[0] for c in sleep.call_args_list] == [5, 10, 15]
    for call in client.send.call_args_list:
        assert call.args == ("rapidgo.seguridad.repartidor.v1",)
        assert call.kwargs["key"].decode() == json.loads(call.kwargs["value"])["repartidor_id"]
    assert len(capsys.readouterr().out.splitlines()) == 12
    client.flush.assert_called_once_with(timeout=30)


def test_failure_is_not_reported_as_delivery(capsys):
    client = Mock()
    client.send.return_value.get.side_effect = TimeoutError("not acknowledged")
    with pytest.raises(TimeoutError):
        publish(args(), client)
    assert capsys.readouterr().out == ""


def test_cli_dry_run_without_kafka(capsys):
    assert main(["--dry-run", "--rounds", "1", "--riders", "1"]) == 0
    assert es_evento_valido(json.loads(capsys.readouterr().out))


def test_deviation_matches_reference_meridian():
    for event in events(args()):
        lat = math.radians(event["gps"]["latitud"])
        delta = math.radians(event["gps"]["longitud"] + 74.264)
        # Independent spherical cross-track identity.
        expected = 6371000 * math.atan2(abs(math.cos(lat) * math.sin(delta)),
                                       math.sqrt(1 - (math.cos(lat) * math.sin(delta)) ** 2))
        assert abs(event["desvio_metros"] - expected) <= 0.0051


def test_cli_connection_failure_returns_nonzero(monkeypatch, capsys):
    import kafka
    constructor = Mock(side_effect=RuntimeError("broker unavailable"))
    monkeypatch.setattr(kafka, "KafkaProducer", constructor)
    assert main(["--rounds", "1"]) == 1
    assert "broker unavailable" in capsys.readouterr().err


def test_cli_closes_client_on_delivery_failure(monkeypatch, capsys):
    import kafka
    client = Mock()
    client.send.return_value.get.side_effect = TimeoutError("ACK timeout")
    constructor = Mock(return_value=client)
    monkeypatch.setattr(kafka, "KafkaProducer", constructor)
    assert main(["--rounds", "1"]) == 1
    client.close.assert_called_once_with(timeout=30)
    assert constructor.call_args.kwargs["acks"] == "all"
    assert constructor.call_args.kwargs["max_in_flight_requests_per_connection"] == 1
    assert capsys.readouterr().out == ""
