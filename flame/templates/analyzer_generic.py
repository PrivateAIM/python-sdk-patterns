"""Skeleton for the analyzer half of a hand-written FLAME analysis."""

from collections.abc import Callable
from typing import Any

from flamesdk.resources.node_config import NodeConfig


class Pattern_Analyzer:
    """Placeholder analyzer: replace :meth:`analyze_or_train` with real work.

    The constructor signature sketches what a node-local analysis or training
    step usually needs - a model, its parameters, the current weights or
    gradients - but none of it is wired up here.
    """

    def __init__(
        self,
        node_config: NodeConfig | None = None,
        base_model: Any | None = None,
        model_params: dict[str, str | float | int | bool] | None = None,
        weights: list[Any] | None = None,
        gradients: list[list[float]] | None = None,
        analz_method: Callable | None = None,
    ) -> None:
        """Accept the usual analyzer inputs; does nothing in this skeleton.

        :param node_config: Configuration of the executing node.
        :param base_model: Model to train or apply locally.
        :param model_params: Hyperparameters of the model.
        :param weights: Current model weights.
        :param gradients: Current gradients.
        :param analz_method: Callable implementing the analysis step.
        """

    def analyze_or_train(self, data: Any, aggr_result: Any | None = None) -> Any:
        """Run one local analysis or training step.

        The stand-in implementation just sums the data together with the
        aggregator's previous result; replace it with the real analysis.

        :param data: This node's local data.
        :param aggr_result: The aggregator's result from the previous round.
        :return: This round's local result.
        """
        return sum(data) + aggr_result  # here: summation example
