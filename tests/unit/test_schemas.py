from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from ridesense.common.schemas import ControlCommand, PassengerFlow


def test_passenger_flow_contract_accepts_seating_and_support_capacities():
    event = PassengerFlow(
        event_id="event-000001",
        vehicle_id="V001",
        route_id="244",
        direction_id=0,
        event_ts=datetime.now(timezone.utc),
        produced_ts_ms=1,
        stop_id="S1",
        boarding_count=4,
        alighting_count=1,
        door_open_ms=18000,
        capacity=70,
        seat_capacity=40,
        stand_capacity=30,
        standing_support_capacity=24,
    )
    assert event.standing_support_capacity == 24


def test_passenger_flow_rejects_more_supports_than_standing_spaces():
    with pytest.raises(ValidationError):
        PassengerFlow(
            event_id="event-000001",
            vehicle_id="V001",
            route_id="244",
            direction_id=0,
            event_ts=datetime.now(timezone.utc),
            produced_ts_ms=1,
            stop_id="S1",
            boarding_count=4,
            alighting_count=1,
            door_open_ms=18000,
            capacity=70,
            seat_capacity=40,
            stand_capacity=20,
            standing_support_capacity=24,
        )


def test_control_command_carries_per_route_bus_counts():
    command = ControlCommand(
        command_id="cmd-000001",
        action="set_fleet",
        issued_at=datetime.now(timezone.utc),
        requested_by="admin",
        route_bus_counts={"R1": 12, "R2": 5},
    )
    assert command.route_bus_counts == {"R1": 12, "R2": 5}
