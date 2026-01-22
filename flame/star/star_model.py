from enum import Enum
from typing import Optional, Type, Literal, Union, Any

from flamesdk import FlameCoreSDK
from flame.star.aggregator_client import Aggregator
from flame.star.analyzer_client import Analyzer
from flame.utils.mock_flame_core import MockFlameCoreSDK


class _ERROR_MESSAGES(Enum):
    IS_ANALYZER = 'Node is configured as analyzer. Unable to execute command associated to aggregator.'
    IS_AGGREGATOR = 'Node is configured as aggregator. Unable to execute command associated to analyzer.'
    IS_INCORRECT_CLASS = 'The object/class given is incorrect, e.g. is not correctly implementing/inheriting the ' \
                         'intended template class.'


class StarModel:
    flame: Union[FlameCoreSDK, MockFlameCoreSDK]

    data: Optional[list[dict[str, Any]]] = None
    test_mode: bool = False

    def __init__(self,
                 analyzer: Type[Analyzer],
                 aggregator: Type[Aggregator],
                 data_type: Literal['fhir', 's3'],
                 query: Optional[Union[str, list[str]]] = None,
                 simple_analysis: bool = True,
                 output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                 analyzer_kwargs: Optional[dict] = None,
                 aggregator_kwargs: Optional[dict] = None,
                 test_mode: bool = False,
                 test_kwargs: Optional[dict] = None) -> None:
        self.test_mode = test_mode
        if self.test_mode:
            self.flame = MockFlameCoreSDK(test_kwargs=test_kwargs)
            test_node_kwargs = {'num_iterations': test_kwargs['num_iterations'],
                                'latest_result': test_kwargs['latest_result']}
        else:
            self.flame = FlameCoreSDK()
            test_node_kwargs = None

        if self._is_analyzer():
            self.flame.flame_log(f"Analyzer {test_kwargs['node_id'] + ' ' if self.test_mode else ''}started",
                                 log_type='info')
            self._start_analyzer(analyzer,
                                 data_type=data_type,
                                 query=query,
                                 simple_analysis=simple_analysis,
                                 analyzer_kwargs=analyzer_kwargs,
                                 test_node_kwargs=test_node_kwargs)
        elif self._is_aggregator():
            self.flame.flame_log("Aggregator started", log_type='info')
            self._start_aggregator(aggregator,
                                   simple_analysis=simple_analysis,
                                   output_type=output_type,
                                   aggregator_kwargs=aggregator_kwargs,
                                   test_node_kwargs=test_node_kwargs)
        else:
            raise BrokenPipeError("Has to be either analyzer or aggregator")
        if not self.test_mode:
            self.flame.flame_log("Analysis finished!", log_type='info')
            while True:
                pass  # keep the node alive to allow for orderly shutdown

    def _is_aggregator(self) -> bool:
        return self.flame.get_role() == 'aggregator'

    def _is_analyzer(self) -> bool:
        return self.flame.get_role() == 'default'

    def _start_aggregator(self,
                          aggregator: Type[Aggregator],
                          simple_analysis: bool = True,
                          output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                          aggregator_kwargs: Optional[dict] = None,
                          test_node_kwargs: Optional[dict[str, Any]] = None) -> None:
        if issubclass(aggregator, Aggregator):
            # init custom aggregator subclass
            if aggregator_kwargs is None:
                aggregator = aggregator(flame=self.flame)
            else:
                aggregator = aggregator(flame=self.flame, **aggregator_kwargs)

            if test_node_kwargs is not None:
                aggregator.set_num_iterations(test_node_kwargs['num_iterations'])
                aggregator.set_latest_result(test_node_kwargs['latest_result'])

            # Ready Check
            self._wait_until_partners_ready()

            # Get analyzer ids
            analyzers = aggregator.partner_node_ids

            while not aggregator.finished:  # (**)
                # Await intermediate results
                result_dict = self.flame.await_intermediate_data(analyzers)

                # Aggregate results
                agg_res, converged = aggregator.aggregate(list(result_dict.values()), simple_analysis)

                if converged:
                    if not self.test_mode:
                        self.flame.flame_log("Submitting final results...", log_type='info', end='')
                    response = self.flame.submit_final_result(agg_res, output_type)
                    if not self.test_mode:
                        self.flame.flame_log(f"success (response={response})", log_type='info')
                    self.flame.analysis_finished()
                    aggregator.node_finished()      # LOOP BREAK
                else:
                    # Send aggregated result to analyzers
                    self.flame.send_intermediate_data(analyzers, agg_res)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

    def _start_analyzer(self,
                        analyzer: Type[Analyzer],
                        data_type: Literal['fhir', 's3'],
                        query: Optional[Union[str, list[str]]] = None,
                        simple_analysis: bool = True,
                        analyzer_kwargs: Optional[dict] = None,
                        test_node_kwargs: Optional[dict[str, Any]] = None) -> None:
        if issubclass(analyzer, Analyzer):
            # init custom analyzer subclass
            if analyzer_kwargs is None:
                analyzer = analyzer(flame=self.flame)
            else:
                analyzer = analyzer(flame=self.flame, **analyzer_kwargs)

            if test_node_kwargs is not None:
                analyzer.set_num_iterations(test_node_kwargs['num_iterations'])
                analyzer.set_latest_result(test_node_kwargs['latest_result'])

            aggregator_id = self.flame.get_aggregator_id()

            # Ready Check
            self._wait_until_partners_ready()

            # Get data
            self._get_data(query=query, data_type=data_type)
            self.flame.flame_log(f"\tData extracted: {str(self.data)[:100]}", log_type='info')

            # Check converged status on Hub
            while not analyzer.finished:  # (**)
                # Analyze data
                analyzer_res = analyzer.analyze(data=self.data)
                # Send intermediate result to aggregator
                self.flame.send_intermediate_data([aggregator_id], analyzer_res)

                # If not converged await aggregated result, loop back to (**)
                if not simple_analysis:
                    analyzer.latest_result = list(self.flame.await_intermediate_data([aggregator_id]).values())
                else:
                    analyzer.node_finished()
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

    def _wait_until_partners_ready(self) -> None:
        if self._is_analyzer():
            aggregator_id = self.flame.get_aggregator_id()
            if not self.test_mode:
                self.flame.flame_log("Awaiting contact with aggregator node...", log_type='info')
            ready_check_dict = self.flame.ready_check([aggregator_id])

            if not ready_check_dict[aggregator_id]:
                raise BrokenPipeError("Could not contact aggregator")

            if not self.test_mode:
                self.flame.flame_log("Awaiting contact with aggregator node...success", log_type='info')
        else:
            analyzer_ids = self.flame.get_participant_ids()
            if not self.test_mode:
                self.flame.flame_log("Awaiting contact with analyzer nodes...", log_type='info')
            ready_check_dict = self.flame.ready_check(analyzer_ids)
            if not all(ready_check_dict.values()):
                raise BrokenPipeError("Could not contact all analyzers")
            if not self.test_mode:
                self.flame.flame_log("Awaiting contact with analyzer nodes...success", log_type='info')

    def _get_data(self,
                  data_type: Literal['fhir', 's3'],
                  query: Optional[Union[str, list[str]]] = None) -> None:
        if type(query) == str:
            query = [query]

        if data_type == 'fhir':
            self.data = self.flame.get_fhir_data(query)
        else:
            self.data = self.flame.get_s3_data(query)
