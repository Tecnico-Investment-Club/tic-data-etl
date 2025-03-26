"""Entity data model."""

from enum import Enum


class Entity(str, Enum):
    """Type of Entity."""

    SPOT_1H = "SPOT_1H"
    SPOT_1D = "SPOT_1D"

    def __repr__(self) -> str:
        return str(self.value)
