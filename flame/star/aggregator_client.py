"""Aggregator end of a star analysis: the node that combines all results."""

import traceback
from abc import abstractmethod
from typing import Any

from flamesdk import FlameCoreSDK
from flamesdk.resources.utils.constants import LogTypeLiteral

from flame.star.node_base_client import Node
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Aggregator(Node):
    """Combines the analyzer results of one round and decides when to stop.

    Subclass this and implement :meth:`aggregation_method` and
    :meth:`has_converged`; the surrounding
    :class:`flame.star.star_model.StarModel` collects the analyzer results,
    calls :meth:`aggregate` and either broadcasts the aggregate back for another
    round or submits it as the final result.

    :cvar delta_criteria: Whether the last round satisfied :meth:`has_converged`.
    """

    delta_criteria: bool = False

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

    def aggregate(
        self,
        node_results: list[Any],
        simple_analysis: bool = True,
        checkpoint_filter: list[str] | None = None,
    ) -> tuple[Any, bool]:
        """Combine one round of analyzer results and report whether to stop.

        Failures in the user-supplied :meth:`aggregation_method` or
        :meth:`has_converged` are caught and logged rather than raised, so that
        the details stay on the executing node. In that case the previous
        aggregate is kept.

        Note that convergence is never signalled after the very first round of a
        multi-round analysis, since there is no previous result to compare to.

        :param node_results: One result per analyzer node.
        :param simple_analysis: True for a one-shot analysis, which always stops
            after a single round; False to iterate until convergence.
        :param checkpoint_filter: Attribute names to restrict a checkpoint to,
            see :meth:`~flame.star.node_base_client.Node.set_checkpoint`.
        :return: Tuple of (latest aggregated result, whether to stop).
        """
        try:
            result = self.aggregation_method(node_results)
            self.delta_criteria = self.has_converged(result, self.latest_result)
            self.latest_result = result
        except Exception:
            self.flame.flame_log(
                "An Error occurred during execution of the given 'aggregation_method' or "
                "'has_converged' function (details available at the executing node)",
                log_type=LogTypeLiteral.ERROR.value,
                hidden_error_msg=traceback.format_exc(),
            )

        if not simple_analysis:
            converged = self.delta_criteria if self.num_iterations != 0 else False
        else:
            converged = True

        self.num_iterations += 1

        if self.should_checkpoint():
            self.set_checkpoint(checkpoint_filter)

        return self.latest_result, converged

    @abstractmethod
    def aggregation_method(self, analysis_results: list[Any]) -> Any:
        """
        This method will be used to aggregate the data. It has to be overwritten.

        :param analysis_results: One result per analyzer node.
        :return: aggregated_result
        """

    @abstractmethod
    def has_converged(self, result: Any, last_result: Any | None) -> bool:
        """
        This method will be used to check if the aggregator has converged. It has to be overwritten.

        :param result: The aggregate just produced for this round.
        :param last_result: The aggregate of the previous round, or None in the
            very first round.
        :return: converged
        """
