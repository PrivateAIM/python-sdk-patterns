"""Analyzer end of a star analysis: the node that sees the local data."""

import traceback
from abc import abstractmethod
from typing import Any

from flamesdk import FlameCoreSDK
from flamesdk.resources.utils.constants import LogTypeLiteral

from flame.star.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Analyzer(Node):
    """Runs the local analysis on one node's data and reports it upstream.

    Subclass this and implement :meth:`analysis_method`; the surrounding
    :class:`flame.star.star_model.StarModel` takes care of fetching the data,
    calling :meth:`analyze` and shipping the result to the aggregator.
    """

    def __init__(self, flame: FlameCoreSDK | MockFlameCoreSDK) -> None:
        """Bind the analyzer to the SDK and assert this node is an analyzer.

        :param flame: An initialized FLAME Core SDK, or its testing mock.
        :raises ValueError: If the hub assigned this node a non-analyzer role.
        """
        super().__init__(flame)
        if self.role != "default":
            raise ValueError(
                f"Attempted to initialize analyzer node with mismatching configuration "
                f'(expected: node_mode="default", received="{self.role}").'
            )

    def analyze(self, data: list[Any], checkpoint_filter: list[str] | None = None) -> Any:
        """Run one analysis round and record its result.

        Failures in the user-supplied :meth:`analysis_method` are caught and
        logged rather than raised: the details stay on the executing node, so
        that a crash cannot leak local data through the error message. In that
        case the previous result is kept and returned unchanged.

        :param data: Local data, one list entry per registered data source.
        :param checkpoint_filter: Attribute names to restrict a checkpoint to,
            see :meth:`~flame.star.node_base_client.Node.set_checkpoint`.
        :return: The latest analysis result.
        """
        try:
            result = self.analysis_method(data, self.latest_result)
            self.latest_result = result
        except Exception:
            self.flame.flame_log(
                "An Error occured during execution of the given 'analysis_method' function "
                "(details available at the executing node)",
                log_type=LogTypeLiteral.ERROR.value,
                hidden_error_msg=traceback.format_exc(),
            )

        self.num_iterations += 1

        if self.should_checkpoint():
            self.set_checkpoint(checkpoint_filter)

        return self.latest_result

    @abstractmethod
    def analysis_method(self, data: list[Any], aggregator_results: Any | None) -> Any:
        """
        This method will be used to analyze the data. It has to be overwritten.

        The parameter data will be formatted like this:
            [list element for every registered data source
                {query: dict for fhir, or str for s3}
            ]

        :param data: Local data, one list entry per registered data source.
        :param aggregator_results: Result the aggregator broadcast after the
            previous round, or None in the very first round.
        :return: analysis_result
        """
