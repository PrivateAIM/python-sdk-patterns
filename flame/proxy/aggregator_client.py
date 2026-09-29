"""Aggregator end of a proxy analysis: combines the proxy pre-aggregates."""

from abc import abstractmethod
from typing import Any

from flamesdk import FlameCoreSDK

from flame.proxy.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Aggregator(Node):
    """Combines the proxy results of one round and decides when to stop.

    Subclass this and implement :meth:`aggregation_method` and
    :meth:`has_converged`. Note that the aggregator only ever sees results that
    a proxy has already combined, never a single analyzer's contribution.

    Besides aggregating, the aggregator is also what assigns analyzers to
    proxies during the ready check; see
    :meth:`flame.proxy.proxy_model.ProxyModel._surjective_analyzer_to_proxy_mapping`.

    :cvar delta_criteria: Whether the last round satisfied :meth:`has_converged`.
    :ivar proxy_ids: Ids of all proxy nodes in the analysis.
    :ivar analyzer_ids: Ids of all analyzer nodes in the analysis.
    """

    delta_criteria: bool = False

    proxy_ids: list[str]
    analyzer_ids: list[str]

    def __init__(self, flame: FlameCoreSDK | MockFlameCoreSDK) -> None:
        """Bind the aggregator to the SDK and assert this node is the aggregator.

        :param flame: An initialized FLAME Core SDK, or its testing mock.
        :raises ValueError: If the hub assigned this node a non-aggregator role.
        """
        super().__init__(flame)
        if self.role != "aggregator":
            raise ValueError(
                f"Attempted to initialize aggregator node with mismatching configuration "
                f'(expected: node_role="aggregator", received="{self.role}").'
            )

    def aggregate(self, proxy_results: list[Any], simple_analysis: bool = True) -> tuple[Any | list[Any], bool]:
        """
        Aggregate results from proxy nodes.

        Note that convergence is never signalled after the very first round of a
        multi-round analysis, since there is no previous result to compare to.

        :param proxy_results: List of aggregated results from proxy nodes
        :param simple_analysis: Whether this is a simple (one-shot) analysis
        :return: Tuple of (final_result, converged, delta_criteria)
        """
        result = self.aggregation_method(proxy_results)
        self.delta_criteria = self.has_converged(result, self.latest_result)
        self.latest_result = result

        if not simple_analysis:
            converged = self.delta_criteria if self.num_iterations != 0 else False
        else:
            converged = True

        self.num_iterations += 1

        return self.latest_result, converged

    def set_analyzer_and_proxy_ids(self, sorted_partner_ids: tuple[list[str], list[str]]) -> None:
        """Record which partner nodes are analyzers and which are proxies.

        :param sorted_partner_ids: Tuple of (analyzer ids, proxy ids), as
            returned by
            :meth:`flame.proxy.proxy_model.ProxyModel._wait_until_partners_ready`.
        """
        self.analyzer_ids, self.proxy_ids = sorted_partner_ids

    @abstractmethod
    def aggregation_method(self, proxy_results: list[Any]) -> Any | list[Any]:
        """
        This method will be used to aggregate the proxy results. It has to be overwritten.

        :param proxy_results: List of aggregated results from proxy nodes
        :return: final_aggregated_result - can be a single result or a list of results
        """

    @abstractmethod
    def has_converged(self, result: Any, last_result: Any | None) -> bool:
        """
        This method will be used to check if the aggregator has converged. It has to be overwritten.

        :param result: Current aggregation result
        :param last_result: Previous aggregation result
        :return: True if converged, False otherwise
        """
