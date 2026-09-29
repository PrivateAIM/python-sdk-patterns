"""Proxy end of a proxy analysis: pre-aggregates a subset of the analyzers."""

from abc import abstractmethod
from typing import Any

from flamesdk import FlameCoreSDK

from flame.proxy.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Proxy(Node):
    """Combines the results of the analyzers assigned to this proxy node.

    Subclass this and implement :meth:`proxy_aggregation_method`. A proxy sits
    between the analyzers and the aggregator: it never touches data itself, it
    only pre-aggregates, so that the aggregator sees group results rather than
    individual node contributions.

    :ivar analyzer_ids: Ids of the analyzers assigned to this proxy.
    :ivar latest_aggregator_result: The aggregate the aggregator last broadcast.
    """

    analyzer_ids: list[str]
    latest_aggregator_result: Any | None

    def __init__(self, flame: FlameCoreSDK | MockFlameCoreSDK) -> None:
        """Bind the proxy to the SDK and assert this node can be a proxy.

        A proxy must have the "proxy" role and, as it never analyzes anything
        itself, must *not* have data access.

        :param flame: An initialized FLAME Core SDK, or its testing mock.
        :raises ValueError: If this node has a different role, or has data.
        """
        super().__init__(flame)
        if self.role != "proxy":
            raise ValueError(
                f"Attempted to initialize proxy node with mismatching configuration "
                f'(expected: node_mode="proxy", received="{self.role}").'
            )
        # Verify this is a proxy node (no data access)
        if hasattr(flame, "node_has_data") and flame.node_has_data():
            raise ValueError("Attempted to initialize proxy node on a node with data access.")

    def proxy_aggregate(self, analyzer_results: list[Any]) -> Any:
        """
        Aggregate results from assigned analyzer nodes.

        :param analyzer_results: List of results from analyzer nodes
        :return: aggregated_result
        """
        result = self.proxy_aggregation_method(analyzer_results)

        self.latest_result = result
        self.num_iterations += 1

        return self.latest_result

    def set_analyzer_ids(self, analyzer_ids: list[str]) -> None:
        """Record which analyzers the aggregator assigned to this proxy.

        :param analyzer_ids: Ids of the assigned analyzer nodes.
        """
        self.analyzer_ids = analyzer_ids

    def set_latest_aggregator_result(self, latest_aggregator_result: Any | None) -> None:
        """Store the aggregate the aggregator broadcast after the last round.

        :param latest_aggregator_result: The aggregator's latest result.
        """
        self.latest_aggregator_result = latest_aggregator_result

    @abstractmethod
    def proxy_aggregation_method(self, analysis_results: list[Any]) -> Any:
        """
        This method will be used to aggregate the analyzer results at the proxy level.
        It has to be overwritten.

        :param analysis_results: List of results from analyzer nodes
        :return: aggregated_result
        """
