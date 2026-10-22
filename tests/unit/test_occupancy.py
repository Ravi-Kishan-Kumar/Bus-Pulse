from ridesense.stream.core.occupancy import OccupancyLedger


def test_boarding_fills_seats_then_supported_standing():
    ledger = OccupancyLedger(capacity=6, seat_capacity=4, stand_capacity=2, standing_support_capacity=2)
    result = ledger.apply_flow(5, 0)
    assert result["boarded"] == 5
    assert ledger.seated_count == 4
    assert ledger.standing_count == 1
    assert ledger.supported_standing_count == 1
    assert ledger.unsupported_standing_count == 0


def test_crush_load_is_bounded_and_unheld_standees_are_visible():
    ledger = OccupancyLedger(capacity=6, seat_capacity=4, stand_capacity=2, standing_support_capacity=2)
    ledger.apply_flow(20, 0)
    assert ledger.onboard_count == 7
    assert ledger.occ_est_pct == 700 / 6
    assert ledger.unsupported_standing_count == 1


def test_alighting_removes_unsupported_then_supported_then_seated():
    ledger = OccupancyLedger(capacity=6, seat_capacity=4, stand_capacity=2, standing_support_capacity=2)
    ledger.apply_flow(7, 0)
    result = ledger.apply_flow(0, 2)
    assert result["alighted"] == 2
    assert ledger.unsupported_standing_count == 0
    assert ledger.supported_standing_count == 1
    assert ledger.seated_count == 4


def test_terminal_reset_clears_every_occupancy_class():
    ledger = OccupancyLedger()
    ledger.apply_flow(48, 0)
    result = ledger.apply_flow(0, 0, terminal=True)
    assert result["terminal_reset"] is True
    assert ledger.onboard_count == 0
    assert ledger.supported_standing_count == 0


def test_over_alighting_is_clamped_to_current_load():
    ledger = OccupancyLedger(capacity=10, seat_capacity=6, stand_capacity=4, standing_support_capacity=4)
    ledger.apply_flow(3, 0)
    result = ledger.apply_flow(0, 99)
    assert result["alighted"] == 3
    assert ledger.onboard_count == 0
