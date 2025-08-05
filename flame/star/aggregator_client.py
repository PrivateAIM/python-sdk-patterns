from abc import abstractmethod
from typing import Any, Optional

from flamesdk import FlameCoreSDK
from flame.star.node_base_client import Node


class Aggregator(Node):

    def __init__(self, flame: FlameCoreSDK) -> None:
        if flame.config.node_role != 'aggregator':
            raise ValueError(f'Attempted to initialize aggregator node with mismatching configuration '
                             f'(expected: node_role="aggregator", received="{flame.config.node_role}").')
        super().__init__(flame)

    def aggregate(self, node_results: list[Any], simple_analysis: bool = True) -> tuple[Any, bool]:
        result = self.aggregation_method(node_results)

        if not simple_analysis:
            if self.latest_result is None:
                converged = False
            else:
                converged = self.has_converged(result, self.latest_result, self.num_iterations)
        else:
            converged = True

        self.latest_result = result
        self.num_iterations += 1

        return self.latest_result, converged

    @abstractmethod
    def aggregation_method(self, analysis_results: list[Any]) -> Any:
        """
        This method will be used to aggregate the data. It has to be overridden.
        :return: aggregated_result
        """
        pass

    @abstractmethod
    def has_converged(self, result: Any, last_result: Optional[Any], num_iterations: int) -> bool:
        """
        This method will be used to check if the aggregator has converged. It has to be overridden.
        :return: converged
        """
        pass
