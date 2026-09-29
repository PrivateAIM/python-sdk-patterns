"""Analyzer end of a proxy analysis: the node that sees the local data."""

from abc import abstractmethod
from typing import Any

from flamesdk import FlameCoreSDK

from flame.proxy.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Analyzer(Node):
    """Runs the local analysis and reports it to this node's assigned proxy.

    Subclass this and implement :meth:`analysis_method`. Unlike in the star
    topology, results are not sent to the aggregator but to the one proxy node
    the aggregator assigned to this analyzer during the ready check.

    :ivar proxy_id: Id of the proxy node this analyzer reports to.
    """

    proxy_id: str

    def __init__(self, flame: FlameCoreSDK | MockFlameCoreSDK) -> None:
        """Bind the analyzer to the SDK and assert this node can be an analyzer.

        An analyzer is a node with the "default" role *and* access to data; a
        node without data access is a proxy instead.

        :param flame: An initialized FLAME Core SDK, or its testing mock.
        :raises ValueError: If this node has a different role, or has no data.
        """
        super().__init__(flame)
        if self.role != "default":
            raise ValueError(
                f"Attempted to initialize analyzer node with mismatching configuration "
                f'(expected: node_mode="default", received="{self.role}").'
            )
        # Verify this is an analyzer node (has data)
        if hasattr(flame, "node_has_data") and not flame.node_has_data():
            raise ValueError("Attempted to initialize analyzer node on a node without data access.")

    def analyze(self, data: list[Any]) -> Any:
        """Run one analysis round and record its result.

        :param data: Local data, one list entry per registered data source.
        :return: The analysis result of this round.
        """
        result = self.analysis_method(data, self.latest_result)

        self.latest_result = result
        self.num_iterations += 1

        return self.latest_result

    def set_proxy_id(self, proxy_id: str) -> None:
        """Record which proxy node the aggregator assigned to this analyzer.

        :param proxy_id: Id of the assigned proxy node.
        """
        self.proxy_id = proxy_id

    @abstractmethod
    def analysis_method(self, data: list[Any], aggregator_results: Any | None) -> Any:
        """
        This method will be used to analyze the data. It has to be overwritten.

        The parameter data will be formatted like this:
            [list element for every registered data source
                {query: dict for fhir, or str for s3}
            ]

        :param data: Local data, one list entry per registered data source.
        :param aggregator_results: Result the aggregator broadcast after the
            previous round, or None in the very first round.
        :return: analysis_result
        """
