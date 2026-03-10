from typing import Any, Literal, Optional, Union

from flamesdk import FlameCoreSDK
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Node:
    id: str
    role: Literal["default", "aggregator"]
    finished: bool
    latest_result: Optional[Any]
    partner_node_ids: list[str]
    num_iterations: int
    flame: Union[FlameCoreSDK, MockFlameCoreSDK]

    def __init__(self, flame: Union[FlameCoreSDK, MockFlameCoreSDK]):
        self.flame = flame

        self.id = self.flame.get_id()
        self.role = self.flame.get_role()
        self.finished = False
        self.latest_result = None
        self.partner_node_ids = self.flame.get_participant_ids()
        self.num_iterations: int = 0

    def node_finished(self):
        self.finished = True
