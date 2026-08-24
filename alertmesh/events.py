"""Alert event types and priorities.

Every alert carries a globally unique idempotency key (alert_id) minted by
its emitter. The key is what makes at-least-once transport safe: the
gateway and the triage queue both dedupe on it.
"""

from dataclasses import dataclass

HELP_BUTTON = "help_button"
PULL_CORD = "pull_cord"
BATTERY_LOW = "battery_low"
HEARTBEAT_LOSS = "heartbeat_loss"

# Lower number = higher priority. Help calls outrank pull cords, which
# outrank maintenance-grade alerts (battery, lost heartbeat).
PRIORITY = {
    HELP_BUTTON: 0,
    PULL_CORD: 1,
    BATTERY_LOW: 2,
    HEARTBEAT_LOSS: 2,
}

ALL_KINDS = (HELP_BUTTON, PULL_CORD, BATTERY_LOW, HEARTBEAT_LOSS)


@dataclass(frozen=True)
class Alert:
    alert_id: str
    device_id: str
    resident_id: str
    kind: str
    emitted_at: float

    def to_record(self):
        return {
            "alert_id": self.alert_id,
            "device_id": self.device_id,
            "resident_id": self.resident_id,
            "kind": self.kind,
            "emitted_at": self.emitted_at,
        }

    @staticmethod
    def from_record(rec):
        return Alert(
            alert_id=rec["alert_id"],
            device_id=rec["device_id"],
            resident_id=rec["resident_id"],
            kind=rec["kind"],
            emitted_at=rec["emitted_at"],
        )
