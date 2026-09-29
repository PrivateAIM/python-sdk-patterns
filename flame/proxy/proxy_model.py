"""Orchestration of a proxy-topology analysis.

:class:`ProxyModel` is the entry point of an analysis image: it detects which of
the three roles (analyzer, proxy, aggregator) the FLAME hub assigned to the
executing node and then drives the corresponding loop.

Unlike the star topology, the roles are not fully known up front: the hub only
distinguishes "default" and "aggregator" nodes, and whether a "default" node is
an analyzer or a proxy follows from whether it has data access. The aggregator
therefore collects self-reported roles during the ready check, computes the
analyzer-to-proxy mapping and tells every node about its counterpart(s).
"""

from collections.abc import Callable
from enum import Enum
from typing import Any, Literal

from flamesdk import FlameCoreSDK
from flamesdk.resources.utils.constants import LogTypeLiteral

from flame.proxy.aggregator_client import Aggregator
from flame.proxy.analyzer_client import Analyzer
from flame.proxy.mapping_methods import round_robin_analyzer_to_proxy_mapping
from flame.proxy.proxy_client import Proxy
from flame.utils.mock_flame_core import MockFlameCoreSDK


class _ERROR_MESSAGES(Enum):
    """Messages for the failure modes shared by the three node roles."""

    IS_ANALYZER = "Node is configured as analyzer. Unable to execute command associated to proxy/aggregator."
    IS_PROXY = "Node is configured as proxy. Unable to execute command associated to analyzer/aggregator."
    IS_AGGREGATOR = "Node is configured as aggregator. Unable to execute command associated to analyzer/proxy."
    IS_INCORRECT_CLASS = "The object/class given is incorrect, e.g. is not correctly implementing/inheriting the intended template class."


class ProxyModel:
    """Runs one node of a proxy analysis, in whichever role it was assigned.

    Instantiating the class *is* the analysis: the constructor starts the node,
    runs it to completion and, outside of test mode, then parks the process so
    the FLAME hub can shut the node down in an orderly fashion.

    Data flows analyzer -> proxy -> aggregator: analyzers send their results to
    the one proxy assigned to them, each proxy pre-aggregates the analyzers it
    owns and forwards that to the aggregator, and the aggregator combines the
    proxy results and either broadcasts the aggregate back for another round or
    submits it as the final result.

    :ivar flame: The FLAME Core SDK (or its mock, during local testing).
    :ivar data: Data fetched for this node; None on proxies and the aggregator.
    :ivar test_mode: Whether this node runs against the mock SDK.
    """

    flame: FlameCoreSDK | MockFlameCoreSDK

    data: list[dict[str, Any]] | None = None
    test_mode: bool = False

    def __init__(
        self,
        analyzer: type[Analyzer],
        proxy: type[Proxy],
        aggregator: type[Aggregator],
        data_type: Literal["fhir", "s3"],
        query: str | list[str] | None = None,
        num_proxy_nodes: int = 1,
        simple_analysis: bool = True,
        output_type: Literal["str", "bytes", "pickle"] | list = "str",
        multiple_results: bool = False,
        filename: str | list[str] | None = None,
        stream_log_level: int = 20,
        mapping_method: Callable[[list[str], list[str]], dict[str, str]] = round_robin_analyzer_to_proxy_mapping,
        analyzer_kwargs: dict | None = None,
        proxy_kwargs: dict | None = None,
        aggregator_kwargs: dict | None = None,
        test_mode: bool = False,
        test_kwargs: dict | None = None,
    ) -> None:
        """Start this node and run the analysis to completion.

        :param analyzer: Subclass of :class:`~flame.proxy.analyzer_client.Analyzer`
            to run if this node is an analyzer.
        :param proxy: Subclass of :class:`~flame.proxy.proxy_client.Proxy` to
            run if this node is a proxy.
        :param aggregator: Subclass of :class:`~flame.proxy.aggregator_client.Aggregator`
            to run if this node is the aggregator.
        :param data_type: Which data source to read on the analyzer nodes.
        :param query: FHIR query/queries or S3 key(s) to fetch. An empty value
            means "everything available".
        :param num_proxy_nodes: How many proxy nodes to expect. The aggregator
            refuses to start if a different number reports in.
        :param simple_analysis: True for a one-shot analysis (a single round);
            False to iterate until the aggregator reports convergence.
        :param output_type: Serialization of the final result. A list applies
            one entry per result when ``multiple_results`` is set.
        :param multiple_results: Whether the final result is a collection to be
            submitted as several separate results.
        :param filename: Filename(s) to submit the final result(s) under.
        :param stream_log_level: Log level handed to the real SDK.
        :param mapping_method: Strategy assigning analyzers to proxies; see
            :mod:`flame.proxy.mapping_methods`.
        :param analyzer_kwargs: Extra keyword arguments for the ``analyzer``.
        :param proxy_kwargs: Extra keyword arguments for the ``proxy``.
        :param aggregator_kwargs: Extra keyword arguments for the ``aggregator``.
        :param test_mode: Run against :class:`MockFlameCoreSDK` instead of the
            real SDK. Used by :class:`~flame.proxy.proxy_model_tester.ProxyModelTester`.
        :param test_kwargs: Mock SDK configuration; required in test mode.
        :raises BrokenPipeError: If the node has none of the three roles, or if
            a given class does not inherit from the expected base class.
        """
        self.num_proxy_nodes = num_proxy_nodes
        self.mapping_method = mapping_method
        self.test_mode = test_mode
        if self.test_mode:
            self.test_kwargs = test_kwargs
            self.flame = MockFlameCoreSDK(test_kwargs=test_kwargs)  # TODO: default_requires_data=False
        else:
            self.test_kwargs = None
            self.flame = FlameCoreSDK(default_requires_data=False, stream_log_level=stream_log_level)

        # Determine node type based on role and data access
        if self._is_analyzer():
            self.flame.flame_log("Analyzer started", log_type=LogTypeLiteral.INFO.value)
            self._start_analyzer(
                analyzer,
                data_type=data_type,
                query=query,
                simple_analysis=simple_analysis,
                analyzer_kwargs=analyzer_kwargs,
            )
        elif self._is_proxy():
            self.flame.flame_log("Proxy started", log_type=LogTypeLiteral.INFO.value)
            self._start_proxy(proxy, simple_analysis=simple_analysis, proxy_kwargs=proxy_kwargs)
        elif self._is_aggregator():
            self.flame.flame_log("Aggregator started", log_type=LogTypeLiteral.INFO.value)
            self._start_aggregator(
                aggregator,
                simple_analysis=simple_analysis,
                output_type=output_type,
                multiple_results=multiple_results,
                filename=filename,
                aggregator_kwargs=aggregator_kwargs,
            )
        else:
            raise BrokenPipeError("Node has to be either analyzer, proxy, or aggregator")
        if not self.test_mode:
            self.flame.flame_log("Analysis finished!", log_type=LogTypeLiteral.INFO.value)
            while True:
                pass  # keep the node alive to allow for orderly shutdown

    def _is_analyzer(self) -> bool:
        """Check if this is an analyzer node (default role with data access)"""
        return self.flame.get_role() == "default"

    def _is_proxy(self) -> bool:
        """Check if this is a proxy node (default role without data access)"""
        return self.flame.get_role() == "proxy"

    def _is_aggregator(self) -> bool:
        """Check if this is an aggregator node (aggregator role with final submission privileges)"""
        return self.flame.get_role() == "aggregator"

    def _start_analyzer(
        self,
        analyzer: type[Analyzer],
        data_type: Literal["fhir", "s3"],
        query: str | list[str] | None = None,
        simple_analysis: bool = True,
        analyzer_kwargs: dict | None = None,
    ) -> None:
        """Run the analyzer loop until the aggregator declares the analysis done.

        The local data is fetched once, before the first round. Each round then
        analyzes it and sends the result to this node's assigned proxy.

        See :meth:`__init__` for the shared parameters.

        :raises BrokenPipeError: If ``analyzer`` does not inherit from
            :class:`~flame.proxy.analyzer_client.Analyzer`.
        """
        if issubclass(analyzer, Analyzer):
            # init custom analyzer subclass
            if analyzer_kwargs is None:
                analyzer = analyzer(flame=self.flame)
            else:
                analyzer = analyzer(flame=self.flame, **analyzer_kwargs)

            # Ready Check
            # the return of _wait_until_partners_ready is tuple(list[analyzer_id], list[proxy_id])
            analyzer.set_proxy_id(self._wait_until_partners_ready()[1][0])
            aggregator_id = self.flame.get_aggregator_id()

            # Get data
            self._get_data(query=query, data_type=data_type)
            self.flame.flame_log(f"\tData extracted: {str(self.data)[:100]}", log_type=LogTypeLiteral.INFO.value)

            while not analyzer.finished:
                # Analyze data
                analyzer_res = analyzer.analyze(data=self.data)

                # Send intermediate result to assigned proxy node
                self.flame.flame_log("Sending intermediate results...", log_type=LogTypeLiteral.INFO.value)
                self.flame.send_intermediate_data([analyzer.proxy_id], analyzer_res)

                # If not converged await aggregated result from proxy
                if not simple_analysis:
                    analyzer.latest_result = list(self.flame.await_intermediate_data([aggregator_id]).values())
                    if self.flame.config.finished:
                        analyzer.node_finished()
                else:
                    analyzer.node_finished()
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

    def _start_proxy(
        self,
        proxy: type[Proxy],
        simple_analysis: bool = True,
        proxy_kwargs: dict | None = None,
    ) -> None:
        """Run the proxy loop until the aggregator declares the analysis done.

        Each round awaits one result per assigned analyzer, pre-aggregates them
        and forwards that single value to the aggregator.

        See :meth:`__init__` for the shared parameters.

        :raises BrokenPipeError: If ``proxy`` does not inherit from
            :class:`~flame.proxy.proxy_client.Proxy`.
        """
        if issubclass(proxy, Proxy):
            # init custom proxy subclass
            if proxy_kwargs is None:
                proxy = proxy(flame=self.flame)
            else:
                proxy = proxy(flame=self.flame, **proxy_kwargs)

            # Ready Check
            # the return of _wait_until_partners_ready is tuple(list[analyzer_id], list[proxy_id])
            proxy.set_analyzer_ids(self._wait_until_partners_ready()[0])
            aggregator_id = self.flame.get_aggregator_id()

            while not proxy.finished:
                # Await intermediate results from assigned analyzer nodes
                result_dict = self.flame.await_intermediate_data(proxy.analyzer_ids)

                # Aggregate results from analyzers
                proxy_res = proxy.proxy_aggregate(list(result_dict.values()))

                # Send aggregated result to aggregator
                self.flame.flame_log("Sending intermediate results...", log_type=LogTypeLiteral.INFO.value)
                self.flame.send_intermediate_data([aggregator_id], proxy_res)

                # If not converged, await aggregated result from aggregator
                if not simple_analysis:
                    aggregator_result = list(self.flame.await_intermediate_data([aggregator_id]).values())
                    proxy.set_latest_aggregator_result(aggregator_result)
                    if self.flame.config.finished:
                        proxy.node_finished()
                else:
                    proxy.node_finished()
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

    def _start_aggregator(
        self,
        aggregator: type[Aggregator],
        simple_analysis: bool = True,
        output_type: Literal["str", "bytes", "pickle"] | list = "str",
        multiple_results: bool = False,
        filename: str | list[str] | None = None,
        aggregator_kwargs: dict | None = None,
    ) -> None:
        """Run the aggregator loop until the analysis converges.

        Each round awaits one pre-aggregate per proxy node and combines them. On
        convergence the aggregate is submitted as the final result and the loop
        ends; otherwise it is broadcast back to the partner nodes, which starts
        the next round.

        See :meth:`__init__` for the shared parameters.

        :raises BrokenPipeError: If ``aggregator`` does not inherit from
            :class:`~flame.proxy.aggregator_client.Aggregator`.
        """
        if issubclass(aggregator, Aggregator):
            # init custom aggregator subclass
            if aggregator_kwargs is None:
                aggregator = aggregator(flame=self.flame)
            else:
                aggregator = aggregator(flame=self.flame, **aggregator_kwargs)

            # Ready Check - wait for all proxy nodes
            # the return of _wait_until_partners_ready is tuple(list[analyzer_id], list[proxy_id])
            aggregator.set_analyzer_and_proxy_ids(self._wait_until_partners_ready())

            while not aggregator.finished:
                # Await intermediate results from proxy nodes
                result_dict = self.flame.await_intermediate_data(aggregator.proxy_ids)

                # Aggregate results from proxies
                agg_res, converged = aggregator.aggregate(list(result_dict.values()), simple_analysis)

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
                    aggregator.node_finished()
                else:
                    # Send aggregated result back to proxy nodes
                    self.flame.flame_log("Sending aggregated results...", log_type=LogTypeLiteral.INFO.value)
                    self.flame.send_intermediate_data(aggregator.partner_node_ids, agg_res)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

    def _wait_until_partners_ready(self) -> tuple[list[str], list[str]]:
        """Block until all partners are reachable and the roles are settled.

        Analyzers and proxies report their own role to the aggregator and then
        wait to be told their counterpart(s). The aggregator collects those
        reports and computes the mapping. All three therefore end up with the
        same shape of answer.

        :return: Tuple of (analyzer ids, proxy ids). On an analyzer or proxy
            node this contains only that node's own counterpart(s).
        :raises BrokenPipeError: If a partner could not be contacted, or if the
            aggregator never sent the assignment.
        """
        if self._is_analyzer() or self._is_proxy():
            aggregator_id = self.flame.get_aggregator_id()
            ready_check_dict = self.flame.ready_check([aggregator_id])
            if not all(ready_check_dict.values()):
                raise BrokenPipeError("Could not contact all nodes")

            if self._is_analyzer():  # Analyzer
                self.flame.send_message(
                    receivers=[self.flame.get_aggregator_id()],
                    message_category="self_roles",
                    message={"role": "default"},
                )
                await_response = self.flame.await_messages(
                    senders=[self.flame.get_aggregator_id()],
                    message_category="assigned_proxy",
                )
                if await_response is not None:
                    return await_response[self.flame.get_aggregator_id()][-1].body["proxy_id"]
                else:
                    raise BrokenPipeError("Could not retrieve assigned proxy from aggregator")

            else:  # Proxy
                self.flame.send_message(
                    receivers=[self.flame.get_aggregator_id()],
                    message_category="self_roles",
                    message={"role": "proxy"},
                )
                await_response = self.flame.await_messages(
                    senders=[self.flame.get_aggregator_id()],
                    message_category="assigned_analyzers",
                )
                if await_response is not None:
                    return await_response[self.flame.get_aggregator_id()][-1].body["analyzer_ids"]
                else:
                    raise BrokenPipeError("Could not retrieve assigned analyzers from aggregator")

        else:  # Aggregator
            # Aggregator needs to contact all nodes
            partner_ids = self.flame.get_participant_ids()
            ready_check_dict = self.flame.ready_check(partner_ids)
            if not all(ready_check_dict.values()):
                raise BrokenPipeError("Could not contact all nodes")

            return self._surjective_analyzer_to_proxy_mapping(partner_ids)

    def _surjective_analyzer_to_proxy_mapping(self, partner_ids: list[str]) -> tuple[list[str], list[str]]:
        """Sort the partners into analyzers and proxies, then assign the former.

        The mapping is surjective onto the proxies: every proxy is guaranteed at
        least one analyzer, which is why there must be at least as many
        analyzers as proxies.

        :param partner_ids: Ids of all nodes other than the aggregator.
        :return: Tuple of (analyzer ids, proxy ids).
        :raises BrokenPipeError: If the number of self-reported proxies differs
            from ``num_proxy_nodes``, or if proxies outnumber analyzers.
        """
        response_dict = self.flame.await_messages(partner_ids, message_category="self_roles")
        proxy_ids = []
        analyzer_ids = []
        if (response_dict is not None) and all(val is not None for val in response_dict.values()):
            for node_id, messages in response_dict.items():
                if messages[-1].body["role"] == "proxy":
                    proxy_ids.append(node_id)
                elif messages[-1].body["role"] == "default":
                    analyzer_ids.append(node_id)

            if len(proxy_ids) != self.num_proxy_nodes:
                raise BrokenPipeError(
                    f"Number of proxies ({len(proxy_ids)}) does not match expected ({self.num_proxy_nodes})"
                )
            elif len(proxy_ids) > len(analyzer_ids):
                raise BrokenPipeError(
                    f"Number of analyzers ({len(analyzer_ids)}) must be at least equal to "
                    f"number of proxies ({len(proxy_ids)})"
                )
            # Map analyzer to proxy ids -> return as dict {analyzer ID: proxy ID, ...}
            mapping = self.mapping_method(proxy_ids, analyzer_ids)

            # Inform all analyzers of their proxies, and proxies of their analyzers
            self._inform_analyzer_proxy_mapping(mapping)

        return analyzer_ids, proxy_ids

    def _inform_analyzer_proxy_mapping(self, mapping: dict[str, str]) -> None:
        """Tell each analyzer its proxy, and each proxy its analyzers.

        Both messages carry the same (analyzer ids, proxy ids) tuple shape that
        :meth:`_wait_until_partners_ready` returns, with the irrelevant half
        left empty.

        :param mapping: Analyzer id -> proxy id, from the mapping method.
        """
        # Inform analyzers of their assigned proxies
        proxy_to_analyzers: dict[str, list[str]] = {}
        for analyzer_id, proxy_id in mapping.items():
            self.flame.send_message(
                receivers=[analyzer_id],
                message_category="assigned_proxy",
                message={"proxy_id": ([], [proxy_id])},
            )
            if proxy_id not in proxy_to_analyzers:
                proxy_to_analyzers[proxy_id] = []
            proxy_to_analyzers[proxy_id].append(analyzer_id)

        for proxy_id, analyzer_ids in proxy_to_analyzers.items():
            self.flame.send_message(
                receivers=[proxy_id],
                message_category="assigned_analyzers",
                message={"analyzer_ids": (analyzer_ids, [])},
            )

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
