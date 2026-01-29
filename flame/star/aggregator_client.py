from abc import abstractmethod
from typing import Any, Optional, Union

from flamesdk import FlameCoreSDK
from flame.star.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Aggregator(Node):

    def __init__(self, flame: Union[FlameCoreSDK, MockFlameCoreSDK]) -> None:
        super().__init__(flame)
        if self.role != 'aggregator':
            raise ValueError(f'Attempted to initialize aggregator node with mismatching configuration '
                             f'(expected: node_role="aggregator", received="{self.role}").')

    def aggregate(self, node_results: list[Any], simple_analysis: bool = True) -> tuple[Union[Any, list[Any]], bool, bool]:
        result = self.aggregation_method(node_results)

        delta_criteria = self.has_converged(result, self.latest_result)
        if not simple_analysis:
            converged = delta_criteria if self.num_iterations != 0 else False
        else:
            converged = True

        self.latest_result = result
        self.num_iterations += 1

        return self.latest_result, converged, delta_criteria

    @abstractmethod
    def aggregation_method(self, analysis_results: list[Any]) -> Union[Any, list[Any]]:
        """
        This method will be used to aggregate the data. It has to be overwritten.
        :return: aggregated_result - can be a single result or a list of results
        """
        pass

    @abstractmethod
    def has_converged(self, result: Any, last_result: Optional[Any]) -> bool:
        """
        This method will be used to check if the aggregator has converged. It has to be overwritten.
        :return: converged
        """
        pass
