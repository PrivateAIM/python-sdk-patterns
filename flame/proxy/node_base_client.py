"""Common state shared by every node taking part in a proxy analysis."""

from typing import Any

from flamesdk import FlameCoreSDK


class Node:
    """Base class for the analyzer, proxy and aggregator ends of the topology.

    It holds the bookkeeping all three roles need: who am I, who are my
    partners, am I done, and what did I produce last.

    :ivar id: Id of the node this code is executing on.
    :ivar role: Role assigned to this node by the FLAME hub; one of "default"
        (an analyzer), "proxy" or "aggregator".
    :ivar finished: Whether this node has left its analysis loop.
    :ivar latest_result: Result of the most recent round on this node.
    :ivar partner_node_ids: Ids of all other nodes in the analysis.
    :ivar num_iterations: Number of rounds completed so far.
    :ivar flame: The FLAME Core SDK (or its mock, during local testing).
    """

    id: str
    role: str  # Can be "default", "proxy" or "aggregator"
    finished: bool
    latest_result: Any | None
    partner_node_ids: list[str]
    num_iterations: int
    flame: FlameCoreSDK

    def __init__(self, flame: FlameCoreSDK):
        """Read the node's own identity and partners off the SDK.

        :param flame: An initialized FLAME Core SDK, or its testing mock.
        """
        self.flame = flame

        self.id = self.flame.get_id()
        self.role = self.flame.get_role()
        self.finished = False
        self.latest_result = None
        self.partner_node_ids = self.flame.get_participant_ids()
        self.num_iterations: int = 0

    def node_finished(self):
        """Mark this node as done, which breaks the enclosing analysis loop."""
        self.finished = True

    def set_num_iterations(self, num_iterations: int) -> None:
        """
        This method sets the number of iterations completed by the node.
        :param num_iterations: Number of iterations to set.
        """
        self.num_iterations = num_iterations

    def set_latest_result(self, latest_result: Any) -> None:
        """
        This method sets the latest result of the node.
        :param latest_result: Latest result to set.
        """
        self.latest_result = latest_result
