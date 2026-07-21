from abc import abstractmethod
from typing import Any, Optional, Union

from flamesdk import FlameCoreSDK
from flamesdk.resources.utils.constants import LogTypeLiteral

from flame.star.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Aggregator(Node):
    delta_criteria: bool = False

    def __init__(self, flame: Union[FlameCoreSDK, MockFlameCoreSDK]) -> None:
        super().__init__(flame)
        if self.role != 'aggregator':
            raise ValueError(f'Attempted to initialize aggregator node with mismatching configuration '
                             f'(expected: node_role="aggregator", received="{self.role}").')

    def aggregate(self,
                  node_results: list[Any],
                  simple_analysis: bool = True,
                  checkpoint_filter: Optional[list[str]] = None) -> tuple[Any, bool]:
        try:
            result = self.aggregation_method(node_results)

            self.delta_criteria = self.has_converged(result, self.latest_result)
        except Exception as e:
            self.flame.flame_log("An Error occured during execution of the given 'aggregation_method' or "
                                 "'has_converged' function (details available at the executing node)",
                                 log_type=LogTypeLiteral.ERROR.value)
            raise e

        if not simple_analysis:
            converged = self.delta_criteria if self.num_iterations != 0 else False
        else:
            converged = True

        self.latest_result = result
        self.num_iterations += 1

        if self.should_checkpoint():
            self.set_checkpoint(checkpoint_filter)

        return self.latest_result, converged

    @abstractmethod
    def aggregation_method(self, analysis_results: list[Any]) -> Any:
        """
        This method will be used to aggregate the data. It has to be overwritten.
        :return: aggregated_result
        """
        pass

    @abstractmethod
    def has_converged(self, result: Any, last_result: Optional[Any]) -> bool:
        """
        This method will be used to check if the aggregator has converged. It has to be overwritten.
        :return: converged
        """
        pass
