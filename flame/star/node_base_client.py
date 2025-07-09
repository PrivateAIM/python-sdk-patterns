from typing import Any, Literal, Optional
from enum import Enum

from flamesdk import FlameCoreSDK


class NodeStatus(Enum):
    STARTED = "Analysis/Aggregation undergoing"
    FINISHED = "Results sent"


class Node:
    id: str
    role: Literal["default", "aggregator"]
    status: str
    latest_result: Optional[Any]
    partner_node_ids: list[str]
    num_iterations: int
    flame: FlameCoreSDK

    def __init__(self, flame: FlameCoreSDK):
        self.flame = flame

        self.id = self.flame.config.node_id
        self.role = self.flame.config.node_role
        self.status = NodeStatus.STARTED.value
        self.latest_result = None
        self.partner_node_ids = self.flame.get_participant_ids()
        self.num_iterations: int = 0

    def node_finished(self):
        self.status = NodeStatus.FINISHED.value
