from typing import Any, Literal, Optional

from flamesdk import FlameCoreSDK


class Node:
    id: str
    role: Literal["default", "aggregator"]
    finished: bool
    latest_result: Optional[Any]
    partner_node_ids: list[str]
    num_iterations: int
    flame: FlameCoreSDK

    def __init__(self, flame: FlameCoreSDK):
        self.flame = flame

        self.id = self.flame.get_id()
        self.role = self.flame.get_role()
        self.finished = False
        self.latest_result = None
        self.partner_node_ids = self.flame.get_participant_ids()
        self.num_iterations: int = 0

    def node_finished(self):
        self.finished = True
        self.flame._flame_logger.set_runstatus('finished')
        self.flame._node_finished()

    def set_num_iterations(self, num_iterations: int) -> None:
        """
        This method sets the number of iterations completed by the aggregator.
        :param num_iterations: Number of iterations to set.
        """
        self.num_iterations = num_iterations

    def set_latest_result(self, latest_result: Any) -> None:
        """
        This method sets the latest result of the aggregator.
        :param latest_result: Latest result to set.
        """
        self.latest_result = latest_result