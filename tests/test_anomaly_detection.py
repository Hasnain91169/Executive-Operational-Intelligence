from __future__ import annotations

from ai.anomaly import recompute_anomalies


def test_anomaly_detection_flags_injected_scenarios(conn):
    anomalies = recompute_anomalies(conn, threshold=3.0, window_days=14, min_history=14)

    scenario_rows = conn.execute(
        "SELECT scenario_tag, scenario_date, kpi_name FROM scenario_registry ORDER BY scenario_tag"
    ).fetchall()

    flagged = {(item.get("scenario_tag"), item["date"], item["kpi_name"]) for item in anomalies if item.get("scenario_tag")}

    for scenario in scenario_rows:
        expected = (scenario["scenario_tag"], scenario["scenario_date"], scenario["kpi_name"])
        assert expected in flagged

    scenario_scores = [item["score"] for item in anomalies if item.get("scenario_tag")]
    assert scenario_scores
    assert min(scenario_scores) > 3.0
