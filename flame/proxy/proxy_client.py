from abc import abstractmethod
from typing import Any, Optional, Union

from flamesdk import FlameCoreSDK
from flame.proxy.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Proxy(Node):
    analyzer_ids: list[str]
    latest_aggregator_result: Optional[Any]

    def __init__(self, flame: Union[FlameCoreSDK, MockFlameCoreSDK]) -> None:
        super().__init__(flame)
        if self.role != 'proxy':
            raise ValueError(f'Attempted to initialize proxy node with mismatching configuration '
                             f'(expected: node_mode="proxy", received="{self.role}").')
        # Verify this is a proxy node (no data access)
        if hasattr(flame, 'node_has_data') and flame.node_has_data():
            raise ValueError(f'Attempted to initialize proxy node on a node with data access.')

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
        self.analyzer_ids = analyzer_ids

    def set_latest_aggregator_result(self, latest_aggregator_result: Optional[Any]) -> None:
        self.latest_aggregator_result = latest_aggregator_result

    @abstractmethod
    def proxy_aggregation_method(self, analysis_results: list[Any]) -> Any:
        """
        This method will be used to aggregate the analyzer results at the proxy level.
        It has to be overwritten.

        :param analysis_results: List of results from analyzer nodes
        :return: aggregated_result
        """
        pass


