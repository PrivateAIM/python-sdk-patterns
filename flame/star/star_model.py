"""Orchestration of a star-topology analysis.

:class:`StarModel` is the entry point of an analysis image: it detects which
role the FLAME hub assigned to the executing node and then drives either the
analyzer or the aggregator loop until the analysis has converged.
"""

from enum import Enum
from typing import Any, Literal

from flamesdk import FlameCoreSDK
from flamesdk.resources.utils.constants import LogTypeLiteral

from flame.star.aggregator_client import Aggregator
from flame.star.analyzer_client import Analyzer
from flame.utils.mock_flame_core import MockFlameCoreSDK


class _ERROR_MESSAGES(Enum):
    """Messages for the failure modes shared by both node roles."""

    IS_ANALYZER = "Node is configured as analyzer. Unable to execute command associated to aggregator."
    IS_AGGREGATOR = "Node is configured as aggregator. Unable to execute command associated to analyzer."
    IS_INCORRECT_CLASS = "The object/class given is incorrect, e.g. is not correctly implementing/inheriting the intended template class."


class StarModel:
    """Runs one node of a star analysis, in whichever role it was assigned.

    Instantiating the class *is* the analysis: the constructor starts the node,
    runs it to completion and, outside of test mode, then parks the process so
    the FLAME hub can shut the node down in an orderly fashion.

    The analyzer side fetches the local data, hands it to the ``analyzer`` class
    and sends each result to the aggregator. The aggregator side awaits one
    result per analyzer, hands them to the ``aggregator`` class and either
    broadcasts the aggregate back for another round or submits it as the final
    result.

    :ivar flame: The FLAME Core SDK (or its mock, during local testing).
    :ivar data: Data fetched for this node; None on the aggregator.
    :ivar test_mode: Whether this node runs against the mock SDK.
    """

    flame: FlameCoreSDK | MockFlameCoreSDK

    data: list[dict[str, Any]] | None = None
    test_mode: bool = False

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
        test_mode: bool = False,
        test_kwargs: dict | None = None,
    ) -> None:
        """Start this node and run the analysis to completion.

        :param analyzer: Subclass of :class:`~flame.star.analyzer_client.Analyzer`
            to run if this node is an analyzer.
        :param aggregator: Subclass of :class:`~flame.star.aggregator_client.Aggregator`
            to run if this node is the aggregator.
        :param data_type: Which data source to read on the analyzer nodes.
        :param query: FHIR query/queries or S3 key(s) to fetch. An empty value
            means "everything available".
        :param simple_analysis: True for a one-shot analysis (a single round);
            False to iterate until the aggregator reports convergence.
        :param output_type: Serialization of the final result. A list applies
            one entry per result when ``multiple_results`` is set.
        :param multiple_results: Whether the final result is a collection to be
            submitted as several separate results.
        :param filename: Filename(s) to submit the final result(s) under.
        :param stream_log_level: Log level handed to the real SDK.
        :param analyzer_kwargs: Extra keyword arguments for the ``analyzer``.
        :param aggregator_kwargs: Extra keyword arguments for the ``aggregator``.
        :param load_checkpoint: Index of a checkpoint to resume from, if any.
        :param checkpoint_filter: Attribute names to restrict checkpoints to.
        :param test_mode: Run against :class:`MockFlameCoreSDK` instead of the
            real SDK. Used by :class:`~flame.star.star_model_tester.StarModelTester`.
        :param test_kwargs: Mock SDK configuration; required in test mode.
        :raises BrokenPipeError: If the node has neither role, or if the given
            analyzer/aggregator does not inherit from the expected base class.
        """
        self.test_mode = test_mode
        if self.test_mode:
            self.test_kwargs = test_kwargs
            self.flame = MockFlameCoreSDK(test_kwargs=test_kwargs)
        else:
            self.test_kwargs = None
            self.flame = FlameCoreSDK(stream_log_level=stream_log_level)

        if self._is_analyzer():
            self.flame.flame_log(
                f"Analyzer {test_kwargs['node_id'] + ' ' if self.test_mode else ''}started",
                log_type=LogTypeLiteral.INFO.value,
            )
            self._start_analyzer(
                analyzer,
                data_type=data_type,
                query=query,
                simple_analysis=simple_analysis,
                analyzer_kwargs=analyzer_kwargs,
                load_checkpoint=load_checkpoint,
                checkpoint_filter=checkpoint_filter,
            )
        elif self._is_aggregator():
            self.flame.flame_log("Aggregator started", log_type=LogTypeLiteral.INFO.value)
            self._start_aggregator(
                aggregator,
                simple_analysis=simple_analysis,
                output_type=output_type,
                multiple_results=multiple_results,
                filename=filename,
                aggregator_kwargs=aggregator_kwargs,
                load_checkpoint=load_checkpoint,
                checkpoint_filter=checkpoint_filter,
            )
        else:
            raise BrokenPipeError("Has to be either analyzer or aggregator")
        if not self.test_mode:
            self.flame.flame_log("Analysis finished!", log_type=LogTypeLiteral.INFO.value)
            while True:
                pass  # keep the node alive to allow for orderly shutdown

    def _is_aggregator(self) -> bool:
        """:return: True if the hub assigned this node the aggregator role."""
        return self.flame.get_role() == "aggregator"

    def _is_analyzer(self) -> bool:
        """:return: True if the hub assigned this node the analyzer role."""
        return self.flame.get_role() == "default"

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
        """Run the aggregator loop until the analysis converges.

        Each round awaits one intermediate result per analyzer and aggregates
        them. On convergence the aggregate is submitted as the final result and
        the loop ends; otherwise the aggregate is broadcast back to the
        analyzers, which starts the next round.

        See :meth:`__init__` for the shared parameters.

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
                self.flame.flame_log("Awaiting intermediate results...", log_type=LogTypeLiteral.INFO.value)
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
                            "Submitting final results...",
                            log_type=LogTypeLiteral.INFO.value,
                            halt_submission=True,
                        )
                    response = self.flame.submit_final_result(agg_res, output_type, multiple_results, filename=filename)
                    if not self.test_mode:
                        self.flame.flame_log(
                            f"success (response={response})",
                            log_type=LogTypeLiteral.INFO.value,
                            append=True,
                        )
                    self.flame.analysis_finished()
                    aggregator.node_finished()  # LOOP BREAK
                else:
                    # Send aggregated result to analyzers
                    self.flame.flame_log("Sending aggregated results...", log_type=LogTypeLiteral.INFO.value)
                    self.flame.send_intermediate_data(analyzers, agg_res)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

    def _start_analyzer(
        self,
        analyzer: type[Analyzer],
        data_type: Literal["fhir", "s3"],
        query: str | list[str] | None = None,
        simple_analysis: bool = True,
        analyzer_kwargs: dict | None = None,
        load_checkpoint: int | None = None,
        checkpoint_filter: list[str] | None = None,
    ) -> None:
        """Run the analyzer loop until the aggregator declares the analysis done.

        The local data is fetched once, before the first round. Each round then
        analyzes it and sends the result to the aggregator. In a multi-round
        analysis the node additionally waits for the aggregate to come back,
        which becomes the ``aggregator_results`` input of the next round.

        See :meth:`__init__` for the shared parameters.

        :raises BrokenPipeError: If ``analyzer`` does not inherit from
            :class:`~flame.star.analyzer_client.Analyzer`.
        """
        if issubclass(analyzer, Analyzer):
            # init custom analyzer subclass
            if analyzer_kwargs is None:
                analyzer = analyzer(flame=self.flame)
            else:
                analyzer = analyzer(flame=self.flame, **analyzer_kwargs)
            if load_checkpoint is not None:
                analyzer.load_checkpoint(load_checkpoint)

            aggregator_id = self.flame.get_aggregator_id()

            # Ready Check
            self._wait_until_partners_ready()

            # Get data
            self._get_data(query=query, data_type=data_type)

            # Check converged status on Hub
            while not analyzer.finished:  # (**)
                # Analyze data
                analyzer_res = analyzer.analyze(data=self.data, checkpoint_filter=checkpoint_filter)
                # Send intermediate result to aggregator
                self.flame.flame_log("Sending intermediate results...", log_type=LogTypeLiteral.INFO.value)
                self.flame.send_intermediate_data([aggregator_id], analyzer_res)

                # If not converged await aggregated result, loop back to (**)
                if not simple_analysis:
                    analyzer.latest_result = self.flame.await_intermediate_data([aggregator_id])[aggregator_id]
                    if self.flame.config.finished:
                        analyzer.node_finished()
                else:
                    analyzer.node_finished()
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

    def _wait_until_partners_ready(self) -> None:
        """Block until every node this one needs to talk to is reachable.

        Analyzers only need the aggregator; the aggregator needs every analyzer.

        :raises BrokenPipeError: If a required partner could not be contacted.
        """
        if self._is_analyzer():
            aggregator_id = self.flame.get_aggregator_id()
            if not self.test_mode:
                self.flame.flame_log(
                    "Awaiting contact with aggregator node...",
                    log_type=LogTypeLiteral.INFO.value,
                    halt_submission=True,
                )
            ready_check_dict = self.flame.ready_check([aggregator_id])

            if not ready_check_dict[aggregator_id]:
                self.flame.flame_log("failed", log_type=LogTypeLiteral.ERROR.value, append=True)
                raise BrokenPipeError("Could not contact aggregator")

            if not self.test_mode:
                self.flame.flame_log("success", log_type=LogTypeLiteral.INFO.value, append=True)
        else:
            analyzer_ids = self.flame.get_participant_ids()
            if not self.test_mode:
                self.flame.flame_log(
                    "Awaiting contact with analyzer nodes...",
                    log_type=LogTypeLiteral.INFO.value,
                    halt_submission=True,
                )
            ready_check_dict = self.flame.ready_check(analyzer_ids)
            if not all(ready_check_dict.values()):
                self.flame.flame_log("failed", log_type=LogTypeLiteral.ERROR.value, append=True)
                raise BrokenPipeError("Could not contact all analyzers")
            if not self.test_mode:
                self.flame.flame_log("success", log_type=LogTypeLiteral.INFO.value, append=True)

    def _get_data(self, data_type: Literal["fhir", "s3"], query: str | list[str] | None = None) -> None:
        """Fetch this node's local data into :attr:`data`.

        A single query is wrapped into a list, and any empty/None query becomes
        an empty list, which the SDK reads as "everything available".

        :param data_type: Which data source to read from.
        :param query: FHIR query/queries or S3 key(s) to fetch.
        """
        if isinstance(query, str) and query:
            query = [query]
        elif not query:
            query = []

        if data_type == "fhir":
            self.data = self.flame.get_fhir_data(query)
        else:
            self.data = self.flame.get_s3_data(query)
