from abc import abstractmethod
from typing import Any, Optional, Union

from flamesdk import FlameCoreSDK
from flame.proxy.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Aggregator(Node):
    delta_criteria: bool = False

    proxy_ids: list[str]
    analyzer_ids: list[str]


    def __init__(self, flame: Union[FlameCoreSDK, MockFlameCoreSDK]) -> None:
        super().__init__(flame)
        if self.role != 'aggregator':
            raise ValueError(f'Attempted to initialize aggregator node with mismatching configuration '
                             f'(expected: node_role="aggregator", received="{self.role}").')

    def aggregate(self, proxy_results: list[Any], simple_analysis: bool = True) -> tuple[Union[Any, list[Any]], bool]:
        """
        Aggregate results from proxy nodes.

        :param proxy_results: List of aggregated results from proxy nodes
        :param simple_analysis: Whether this is a simple (one-shot) analysis
        :return: Tuple of (final_result, converged, delta_criteria)
        """
        result = self.aggregation_method(proxy_results)

        self.delta_criteria = self.has_converged(result, self.latest_result)
        if not simple_analysis:
            converged = self.delta_criteria if self.num_iterations != 0 else False
        else:
            converged = True

        self.latest_result = result
        self.num_iterations += 1

        return self.latest_result, converged

    def set_analyzer_and_proxy_ids(self, sorted_partner_ids: tuple[list[str], list[str]]) -> None:
        self.analyzer_ids, self.proxy_ids = sorted_partner_ids

    @abstractmethod
    def aggregation_method(self, proxy_results: list[Any]) -> Union[Any, list[Any]]:
        """
        This method will be used to aggregate the proxy results. It has to be overwritten.

        :param proxy_results: List of aggregated results from proxy nodes
        :return: final_aggregated_result - can be a single result or a list of results
        """
        pass

    @abstractmethod
    def has_converged(self, result: Any, last_result: Optional[Any]) -> bool:
        """
        This method will be used to check if the aggregator has converged. It has to be overwritten.

        :param result: Current aggregation result
        :param last_result: Previous aggregation result
        :return: True if converged, False otherwise
        """
        pass
