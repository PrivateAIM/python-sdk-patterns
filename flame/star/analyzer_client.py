from abc import abstractmethod
from typing import Any, Optional, Union

from flamesdk import FlameCoreSDK
from flamesdk.resources.utils.constants import LogTypeLiteral

from flame.star.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Analyzer(Node):

    def __init__(self, flame: Union[FlameCoreSDK, MockFlameCoreSDK]) -> None:
        super().__init__(flame)
        if self.role != 'default':
            raise ValueError(f'Attempted to initialize analyzer node with mismatching configuration '
                             f'(expected: node_mode="default", received="{self.role}").')

    def analyze(self, data: list[Any], checkpoint_filter: Optional[list[str]] = None) -> Any:
        try:
            result = self.analysis_method(data, self.latest_result)
        except Exception as e:
            self.flame.flame_log("An Error occured during execution of the given 'analysis_method' function "
                                 "(details available at the executing node)",
                                 log_type=LogTypeLiteral.ERROR.value,
                                 hidden_error_msg=repr(e))

        self.latest_result = result
        self.num_iterations += 1

        if self.should_checkpoint():
            self.set_checkpoint(checkpoint_filter)

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
