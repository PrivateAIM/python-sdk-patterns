"""Star analysis whose final result is perturbed with local differential privacy."""

from typing import Any, Literal

from flamesdk import FlameCoreSDK
from flamesdk.resources.utils.constants import LogTypeLiteral

from flame.star.aggregator_client import Aggregator
from flame.star.analyzer_client import Analyzer
from flame.star.star_model import _ERROR_MESSAGES, StarModel
from flame.utils.mock_flame_core import MockFlameCoreSDK


class StarLocalDPModel(StarModel):
    """A :class:`~flame.star.star_model.StarModel` with a DP-protected result.

    The analyzer side is unchanged. On the aggregator side, the final result is
    passed through a Laplace mechanism parameterized by ``epsilon`` and
    ``sensitivity`` before it is submitted, so that the published value does not
    disclose any single node's contribution.

    The mechanism is only applied when all of the following hold: both
    ``epsilon`` and ``sensitivity`` were given, and the aggregator's own
    ``has_converged`` reported True for the final round (``delta_criteria``).
    Otherwise the result is submitted unperturbed.

    :ivar epsilon: Privacy budget of the Laplace mechanism.
    :ivar sensitivity: Maximum influence a single node can have on the result.
    """

    flame: FlameCoreSDK | MockFlameCoreSDK

    data: list[dict[str, Any]] | None = None
    test_mode: bool = False

    epsilon: float | None
    sensitivity: float | None

    def __init__(
        self,
        analyzer: type[Analyzer],
        aggregator: type[Aggregator],
        data_type: Literal["fhir", "s3"],
        query: str | list[str] | None = None,
        simple_analysis: bool = True,
        output_type: Literal["str", "bytes", "pickle"] | list = "str",
        multiple_results: bool = False,
        filename: str | list[str] | None = None,
        stream_log_level: int = 20,
        analyzer_kwargs: dict | None = None,
        aggregator_kwargs: dict | None = None,
        load_checkpoint: int | None = None,
        checkpoint_filter: list[str] | None = None,
        epsilon: float | None = None,
        sensitivity: float | None = None,
        test_mode: bool = False,
        test_kwargs: dict | None = None,
    ) -> None:
        """Start this node, applying local DP if this node is the aggregator.

        All parameters other than ``epsilon`` and ``sensitivity`` are forwarded
        unchanged; see :meth:`flame.star.star_model.StarModel.__init__`.

        :param epsilon: Privacy budget of the Laplace mechanism. Lower means
            more noise and stronger privacy. DP is skipped when this is None.
        :param sensitivity: Maximum influence a single node can have on the
            result. DP is skipped when this is None.
        """
        self.epsilon = epsilon
        self.sensitivity = sensitivity
        super().__init__(
            analyzer=analyzer,
            aggregator=aggregator,
            data_type=data_type,
            query=query,
            simple_analysis=simple_analysis,
            output_type=output_type,
            multiple_results=multiple_results,
            filename=filename,
            stream_log_level=stream_log_level,
            analyzer_kwargs=analyzer_kwargs,
            aggregator_kwargs=aggregator_kwargs,
            load_checkpoint=load_checkpoint,
            checkpoint_filter=checkpoint_filter,
            test_mode=test_mode,
            test_kwargs=test_kwargs,
        )

    def _start_aggregator(
        self,
        aggregator: type[Aggregator],
        simple_analysis: bool = True,
        output_type: Literal["str", "bytes", "pickle"] | list = "str",
        multiple_results: bool = False,
        filename: str | list[str] | None = None,
        aggregator_kwargs: dict | None = None,
        load_checkpoint: int | None = None,
        checkpoint_filter: list[str] | None = None,
    ) -> None:
        """Run the aggregator loop, submitting the final result under local DP.

        Identical to
        :meth:`flame.star.star_model.StarModel._start_aggregator`, except that
        the DP parameters are handed to ``submit_final_result``, which applies
        the Laplace mechanism.

        See :meth:`flame.star.star_model.StarModel.__init__` for the parameters.

        :raises BrokenPipeError: If ``aggregator`` does not inherit from
            :class:`~flame.star.aggregator_client.Aggregator`.
        """
        if issubclass(aggregator, Aggregator):
            # init custom aggregator subclass
            if aggregator_kwargs is None:
                aggregator = aggregator(flame=self.flame)
            else:
                aggregator = aggregator(flame=self.flame, **aggregator_kwargs)
            if load_checkpoint is not None:
                aggregator.load_checkpoint(load_checkpoint)

            # Ready Check
            self._wait_until_partners_ready()

            # Get analyzer ids
            analyzers = aggregator.partner_node_ids

            while not aggregator.finished:  # (**)
                # Await intermediate results
                result_dict = self.flame.await_intermediate_data(analyzers)

                # Aggregate results
                agg_res, converged = aggregator.aggregate(
                    node_results=list(result_dict.values()),
                    simple_analysis=simple_analysis,
                    checkpoint_filter=checkpoint_filter,
                )

                if converged:
                    if not self.test_mode:
                        self.flame.flame_log(
                            "Submitting final results using differential privacy...",
                            log_type=LogTypeLiteral.INFO.value,
                            halt_submission=True,
                        )
                    if aggregator.delta_criteria and (self.epsilon is not None) and (self.sensitivity is not None):
                        local_dp = {"epsilon": self.epsilon, "sensitivity": self.sensitivity}
                    else:
                        local_dp = None
                    if self.test_mode and (local_dp is not None):
                        self.flame.flame_log(
                            f"\tTest mode: Would apply local DP with epsilon={local_dp['epsilon']} "
                            f"and sensitivity={local_dp['sensitivity']}",
                            log_type=LogTypeLiteral.INFO.value,
                        )
                    response = self.flame.submit_final_result(
                        agg_res,
                        output_type,
                        multiple_results,
                        local_dp=local_dp,
                        filename=filename,
                    )
                    if not self.test_mode:
                        self.flame.flame_log(f"success (response={response})", log_type=LogTypeLiteral.INFO.value)
                    self.flame.analysis_finished()
                    aggregator.node_finished()  # LOOP BREAK
                else:
                    # Send aggregated result to analyzers
                    self.flame.send_intermediate_data(analyzers, agg_res)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)
