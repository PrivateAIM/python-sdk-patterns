from abc import abstractmethod
from typing import Any, Optional

from flamesdk import FlameCoreSDK
from flame.star.node_base_client import Node


class Analyzer(Node):

    def __init__(self, flame: FlameCoreSDK) -> None:
        if flame.config.node_role != 'default':
            raise ValueError(f'Attempted to initialize analyzer node with mismatching configuration '
                             f'(expected: node_mode="default", received="{flame.config.node_role}").')
        super().__init__(flame)

    def analyze(self,
                data: list[Any],
                aggregator_results: Optional[str],
                simple_analysis: bool = True) -> tuple[Any, bool]:
        result = self.analysis_method(data, aggregator_results)

        self.latest_result = result
        self.num_iterations += 1

        return self.latest_result, simple_analysis

    @abstractmethod
    def analysis_method(self, data: list[Any], aggregator_results: Optional[Any]) -> Any:
        """
        This method will be used to analyze the data. It has to be overwritten.

        The parameter data will be formatted like this:
            [list element for every registered data source
                {query: dict for fhir, or str for s3}
            ]

        :return: analysis_result
        """
        pass
