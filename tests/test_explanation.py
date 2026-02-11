from __future__ import annotations

from ai.anomaly import recompute_anomalies
from ai.explain import explain_kpi


def test_explanation_driver_ranking_for_scenarios_a_b(conn):
    recompute_anomalies(conn, threshold=3.0, window_days=14, min_history=14)

    scenarios = conn.execute(
        """
        SELECT scenario_tag, scenario_date, kpi_name, expected_driver_dimension, expected_driver_value
        FROM scenario_registry
        WHERE scenario_tag IN ('Scenario A', 'Scenario B')
        ORDER BY scenario_tag
        """
    ).fetchall()

    for scenario in scenarios:
        result = explain_kpi(
            conn,
            kpi_name=scenario["kpi_name"],
            target_date=scenario["scenario_date"],
            request_id=f"test-{scenario['scenario_tag'].lower().replace(' ', '-')}",
        )

        assert result["drivers"], f"No drivers returned for {scenario['scenario_tag']}"
        top_driver = result["drivers"][0]

        assert str(top_driver["dimension"]).lower() == str(scenario["expected_driver_dimension"]).lower()
        assert str(top_driver["segment"]).lower() == str(scenario["expected_driver_value"]).lower()
