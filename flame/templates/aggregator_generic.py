"""Skeleton for the aggregator half of a hand-written FLAME analysis."""

from collections.abc import Callable
from typing import Any

from flamesdk.resources.node_config import NodeConfig


class Pattern_Aggregator:
    """Placeholder aggregator: replace the two methods with real work.

    The constructor signature sketches what an aggregation step usually needs -
    a model, its parameters, the collected weights or gradients - but none of it
    is wired up here.

    :cvar last_result: Previous round's aggregate, used by :meth:`has_converged`.
    """

    last_result: Any = None

    def __init__(
        self,
        node_config: NodeConfig | None = None,
        base_model: Any | None = None,
        model_params: dict[str, str | float | int | bool] | None = None,
        weights: list[Any] | None = None,
        gradients: list[list[float]] | None = None,
        aggr_method: Callable | None = None,
    ) -> None:
        """Accept the usual aggregator inputs; does nothing in this skeleton.

        :param node_config: Configuration of the executing node.
        :param base_model: Model the aggregate is applied to.
        :param model_params: Hyperparameters of the model.
        :param weights: Collected model weights.
        :param gradients: Collected gradients.
        :param aggr_method: Callable implementing the aggregation step.
        """

    def aggregate(self, results_or_modelparams=list[Any]) -> Any:
        """Combine one round of analyzer results.

        The stand-in implementation just sums them; replace it with the real
        aggregation.

        :param results_or_modelparams: One result (or parameter set) per
            analyzer node.
        :return: The aggregate for this round.
        """
        return sum(results_or_modelparams)  # here: summation example

    def has_converged(self, result: Any) -> bool:
        """Report whether the analysis has settled, and remember this round.

        The stand-in criterion is equality with the previous round's aggregate;
        replace it with a real convergence test (e.g. a delta threshold).

        :param result: The aggregate just produced for this round.
        :return: True if the analysis should stop.
        """
        are_identical = result == self.last_result
        self.last_result = result
        return are_identical  # here: identity test with earlier result as convergence criteria
