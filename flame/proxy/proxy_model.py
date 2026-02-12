from enum import Enum
from typing import Callable, Optional, Type, Literal, Union, Any

from flamesdk import FlameCoreSDK
from flame.proxy.aggregator_client import Aggregator
from flame.proxy.analyzer_client import Analyzer
from flame.proxy.proxy_client import Proxy
from flame.proxy.mapping_methods import round_robin_analyzer_to_proxy_mapping
from flame.utils.mock_flame_core import MockFlameCoreSDK


class _ERROR_MESSAGES(Enum):
    IS_ANALYZER = 'Node is configured as analyzer. Unable to execute command associated to proxy/aggregator.'
    IS_PROXY = 'Node is configured as proxy. Unable to execute command associated to analyzer/aggregator.'
    IS_AGGREGATOR = 'Node is configured as aggregator. Unable to execute command associated to analyzer/proxy.'
    IS_INCORRECT_CLASS = 'The object/class given is incorrect, e.g. is not correctly implementing/inheriting the ' \
                         'intended template class.'


class ProxyModel:
    flame: Union[FlameCoreSDK, MockFlameCoreSDK]

    data: Optional[list[dict[str, Any]]] = None
    test_mode: bool = False

    def __init__(self,
                 analyzer: Type[Analyzer],
                 proxy: Type[Proxy],
                 aggregator: Type[Aggregator],
                 data_type: Literal['fhir', 's3'],
                 query: Optional[Union[str, list[str]]] = [],
                 num_proxy_nodes: int = 1,
                 simple_analysis: bool = True,
                 output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                 multiple_results: bool = False,
                 mapping_method: Callable[[list[str], list[str]], dict[str, str]] = round_robin_analyzer_to_proxy_mapping,
                 analyzer_kwargs: Optional[dict] = None,
                 proxy_kwargs: Optional[dict] = None,
                 aggregator_kwargs: Optional[dict] = None) -> None:
        self.num_proxy_nodes = num_proxy_nodes
        self.mapping_method = mapping_method
        self.flame = FlameCoreSDK(default_requires_data=False)

        # Determine node type based on role and data access
        if self._is_analyzer():
            self.flame.flame_log(f"Analyzer started", log_type='info')
            self._start_analyzer(analyzer,
                                 data_type=data_type,
                                 query=query,
                                 simple_analysis=simple_analysis,
                                 analyzer_kwargs=analyzer_kwargs)
        elif self._is_proxy():
            self.flame.flame_log(f"Proxy started", log_type='info')
            self._start_proxy(proxy,
                              simple_analysis=simple_analysis,
                              proxy_kwargs=proxy_kwargs)
        elif self._is_aggregator():
            self.flame.flame_log("Aggregator started", log_type='info')
            self._start_aggregator(aggregator,
                                   simple_analysis=simple_analysis,
                                   output_type=output_type,
                                   multiple_results=multiple_results,
                                   aggregator_kwargs=aggregator_kwargs)
        else:
            raise BrokenPipeError("Node has to be either analyzer, proxy, or aggregator")
        self.flame.flame_log("Analysis finished!", log_type='info')
        while True:
            pass  # keep the node alive to allow for orderly shutdown

    def _is_analyzer(self) -> bool:
        """Check if this is an analyzer node (default role with data access)"""
        return self.flame.get_role() == 'default'

    def _is_proxy(self) -> bool:
        """Check if this is a proxy node (default role without data access)"""
        return self.flame.get_role() == 'proxy'

    def _is_aggregator(self) -> bool:
        """Check if this is an aggregator node (aggregator role with final submission privileges)"""
        return self.flame.get_role() == 'aggregator'

    def _start_analyzer(self,
                        analyzer: Type[Analyzer],
                        data_type: Literal['fhir', 's3'],
                        query: Optional[Union[str, list[str]]] = None,
                        simple_analysis: bool = True,
                        analyzer_kwargs: Optional[dict] = None) -> None:
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
            self.flame.flame_log(f"\tData extracted: {str(self.data)[:100]}", log_type='info')

            while not analyzer.finished:
                # Analyze data
                analyzer_res = analyzer.analyze(data=self.data)

                # Send intermediate result to assigned proxy node
                self.flame.send_intermediate_data([analyzer.proxy_id], analyzer_res)

                # If not converged await aggregated result from proxy
                if not simple_analysis:
                    analyzer.latest_result = list(self.flame.await_intermediate_data([aggregator_id]).values())
                else:
                    analyzer.node_finished()
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

    def _start_proxy(self,
                     proxy: Type[Proxy],
                     simple_analysis: bool = True,
                     proxy_kwargs: Optional[dict] = None) -> None:
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
                self.flame.send_intermediate_data([aggregator_id], proxy_res)

                # If not converged, await aggregated result from aggregator
                if not simple_analysis:
                    aggregator_result = list(self.flame.await_intermediate_data([aggregator_id]).values())
                    proxy.set_latest_aggregator_result(aggregator_result)
                else:
                    proxy.node_finished()
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

    def _start_aggregator(self,
                          aggregator: Type[Aggregator],
                          simple_analysis: bool = True,
                          output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                          multiple_results: bool = False,
                          aggregator_kwargs: Optional[dict] = None) -> None:
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
                agg_res, converged, _ = aggregator.aggregate(list(result_dict.values()), simple_analysis)

                if converged:
                    response = self.flame.submit_final_result(agg_res, output_type, multiple_results)
                    self.flame.flame_log(f"success (response={response})", log_type='info')
                    self.flame.analysis_finished()
                    aggregator.node_finished()
                else:
                    # Send aggregated result back to proxy nodes
                    self.flame.send_intermediate_data(aggregator.partner_node_ids, agg_res)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

    def _wait_until_partners_ready(self) -> tuple[list[str], list[str]]:
        if self._is_analyzer() or self._is_proxy():
            aggregator_id = self.flame.get_aggregator_id()
            ready_check_dict = self.flame.ready_check([aggregator_id])
            if not all(ready_check_dict.values()):
                raise BrokenPipeError("Could not contact all nodes")

            if self._is_analyzer(): # Analyzer
                self.flame.send_message(
                    receivers=[self.flame.get_aggregator_id()],
                    message_category='self_roles',
                    message={'role': 'default'}
                )
                await_response = self.flame.await_messages(
                    senders=[self.flame.get_aggregator_id()],
                    message_category='assigned_proxy'
                )
                if await_response is not None:
                    return await_response[self.flame.get_aggregator_id()][0].body['proxy_id']
                else:
                    raise BrokenPipeError("Could not retrieve assigned proxy from aggregator")

            else: # Proxy
                self.flame.send_message(
                    receivers=[self.flame.get_aggregator_id()],
                    message_category='self_roles',
                    message={'role': 'proxy'}
                )
                await_response = self.flame.await_messages(
                    senders=[self.flame.get_aggregator_id()],
                    message_category='assigned_analyzers'
                )
                if await_response is not None:
                    return await_response[self.flame.get_aggregator_id()][0].body['analyzer_ids']
                else:
                    raise BrokenPipeError("Could not retrieve assigned analyzers from aggregator")

        else: # Aggregator
            # Aggregator needs to contact all nodes
            partner_ids = self.flame.get_participant_ids()
            ready_check_dict = self.flame.ready_check(partner_ids)
            if not all(ready_check_dict.values()):
                raise BrokenPipeError("Could not contact all nodes")

            return self._surjective_analyzer_to_proxy_mapping(partner_ids)

    def _surjective_analyzer_to_proxy_mapping(self, partner_ids: list[str]) -> tuple[list[str], list[str]]:
        response_dict = self.flame.await_messages(partner_ids, message_category='self_roles')
        proxy_ids = []
        analyzer_ids = []
        if all([val is not None for val in response_dict.values()]):
            for node_id, message in response_dict.items():
                if message[0].body['role'] == 'proxy':
                    proxy_ids.append(node_id)
                elif message[0].body['role'] == 'default':
                    analyzer_ids.append(node_id)

            if len(proxy_ids) != self.num_proxy_nodes:
                raise BrokenPipeError(f"Number of proxies ({len(proxy_ids)}) does not match expected "
                                      f"({self.num_proxy_nodes})")
            elif len(proxy_ids) > len(analyzer_ids):
                raise BrokenPipeError(f"Number of analyzers ({len(analyzer_ids)}) must be at least equal to "
                                      f"number of proxies ({len(proxy_ids)})")
            # Create round-robin mapping
            mapping = self.mapping_method(proxy_ids, analyzer_ids)

            # Inform all analyzers of their proxies, and proxies of their analyzers
            self._inform_analyzer_proxy_mapping(mapping)

        return analyzer_ids, proxy_ids

    def _inform_analyzer_proxy_mapping(self, mapping: dict[str, str]) -> None:
        # Inform analyzers of their assigned proxies
        proxy_to_analyzers: dict[str, list[str]] = {}
        for analyzer_id, proxy_id in mapping.items():
            self.flame.send_message(
                receivers=[analyzer_id],
                message_category='assigned_proxy',
                message={'proxy_id': ([], [proxy_id])}
            )
            if proxy_id not in proxy_to_analyzers.keys():
                proxy_to_analyzers[proxy_id] = []
            proxy_to_analyzers[proxy_id].append(analyzer_id)

        for proxy_id, analyzer_ids in proxy_to_analyzers.items():
            self.flame.send_message(
                receivers=[proxy_id],
                message_category='assigned_analyzers',
                message={'analyzer_ids': (analyzer_ids, [])}
            )

    def _get_data(self,
                  data_type: Literal['fhir', 's3'],
                  query: Optional[Union[str, list[str]]] = None) -> None:
        if type(query) == str:
            query = [query]

        if data_type == 'fhir':
            self.data = self.flame.get_fhir_data(query)
        else:
            self.data = self.flame.get_s3_data(query)
