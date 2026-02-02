from abc import abstractmethod
from typing import Any, Optional, Union

from flamesdk import FlameCoreSDK
from flame.proxy.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Analyzer(Node):

    def __init__(self, flame: Union[FlameCoreSDK, MockFlameCoreSDK]) -> None:
        super().__init__(flame)
        if self.role != 'default':
            raise ValueError(f'Attempted to initialize analyzer node with mismatching configuration '
                             f'(expected: node_mode="default", received="{self.role}").')
        # Verify this is an analyzer node (has data)
        if hasattr(flame, 'node_has_data') and not flame.node_has_data():
            raise ValueError(f'Attempted to initialize analyzer node on a node without data access.')

    def analyze(self, data: list[Any]) -> Any:
        result = self.analysis_method(data, self.latest_result)

        self.latest_result = result
        self.num_iterations += 1

        return self.latest_result

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
