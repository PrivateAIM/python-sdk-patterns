from enum import Enum
from typing import Optional, Type, Literal, Union, Any

from flamesdk import FlameCoreSDK
from flamesdk.resources.utils.constants import LogTypeLiteral
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
                 output_type: Union[Literal['str', 'bytes', 'pickle'], list] = 'str',
                 multiple_results: bool = False,
                 filename: Optional[Union[str, list[str]]] = None,
                 stream_log_level: int = 20,
                 analyzer_kwargs: Optional[dict] = None,
                 aggregator_kwargs: Optional[dict] = None,
                 test_mode: bool = False,
                 test_kwargs: Optional[dict] = None) -> None:
        self.test_mode = test_mode
        if self.test_mode:
            self.test_kwargs = test_kwargs
            self.flame = MockFlameCoreSDK(test_kwargs=test_kwargs)
        else:
            self.test_kwargs = None
            self.flame = FlameCoreSDK(stream_log_level=stream_log_level)

        if self._is_analyzer():
            self.flame.flame_log(f"Analyzer {test_kwargs['node_id'] + ' ' if self.test_mode else ''}started",
                                 log_type=LogTypeLiteral.INFO.value)
            self._start_analyzer(analyzer,
                                 data_type=data_type,
                                 query=query,
                                 simple_analysis=simple_analysis,
                                 analyzer_kwargs=analyzer_kwargs)
        elif self._is_aggregator():
            self.flame.flame_log("Aggregator started", log_type=LogTypeLiteral.INFO.value)
            self._start_aggregator(aggregator,
                                   simple_analysis=simple_analysis,
                                   output_type=output_type,
                                   multiple_results=multiple_results,
                                   filename=filename,
                                   aggregator_kwargs=aggregator_kwargs)
        else:
            raise BrokenPipeError("Has to be either analyzer or aggregator")
        if not self.test_mode:
            self.flame.flame_log("Analysis finished!", log_type=LogTypeLiteral.INFO.value)
            while True:
                pass  # keep the node alive to allow for orderly shutdown

    def _is_aggregator(self) -> bool:
        return self.flame.get_role() == 'aggregator'

    def _is_analyzer(self) -> bool:
        return self.flame.get_role() == 'default'

    def _start_aggregator(self,
                          aggregator: Type[Aggregator],
                          simple_analysis: bool = True,
                          output_type: Union[Literal['str', 'bytes', 'pickle'], list] = 'str',
                          multiple_results: bool = False,
                          filename: Optional[Union[str, list[str]]] = None,
                          aggregator_kwargs: Optional[dict] = None) -> None:
        if issubclass(aggregator, Aggregator):
            # init custom aggregator subclass
            if aggregator_kwargs is None:
                aggregator = aggregator(flame=self.flame)
            else:
                aggregator = aggregator(flame=self.flame, **aggregator_kwargs)

            # Ready Check
            self._wait_until_partners_ready()

            # Get analyzer ids
            analyzers = aggregator.partner_node_ids

            while not aggregator.finished:  # (**)
                # Await intermediate results
                self.flame.flame_log(f"Awaiting intermediate results...", log_type=LogTypeLiteral.INFO.value)
                result_dict = self.flame.await_intermediate_data(analyzers)

                # Aggregate results
                agg_res, converged = aggregator.aggregate(list(result_dict.values()), simple_analysis)

                if converged:
                    if not self.test_mode:
                        self.flame.flame_log("Submitting final results...",
                                             log_type=LogTypeLiteral.INFO.value,
                                             halt_submission=True)
                    response = self.flame.submit_final_result(agg_res, output_type, multiple_results,
                                                              filename=filename)
                    if not self.test_mode:
                        self.flame.flame_log(f"success (response={response})", log_type=LogTypeLiteral.INFO.value)
                    self.flame.analysis_finished()
                    aggregator.node_finished()      # LOOP BREAK
                else:
                    # Send aggregated result to analyzers
                    self.flame.flame_log(f"Sending aggregated results...", log_type=LogTypeLiteral.INFO.value)
                    self.flame.send_intermediate_data(analyzers, agg_res)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)

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

            aggregator_id = self.flame.get_aggregator_id()

            # Ready Check
            self._wait_until_partners_ready()

            # Get data
            self._get_data(query=query, data_type=data_type)

            # Check converged status on Hub
            while not analyzer.finished:  # (**)
                # Analyze data
                analyzer_res = analyzer.analyze(data=self.data)
                # Send intermediate result to aggregator
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
        if self._is_analyzer():
            aggregator_id = self.flame.get_aggregator_id()
            if not self.test_mode:
                self.flame.flame_log("Awaiting contact with aggregator node...",
                                     log_type=LogTypeLiteral.INFO.value)
            ready_check_dict = self.flame.ready_check([aggregator_id])

            if not ready_check_dict[aggregator_id]:
                raise BrokenPipeError("Could not contact aggregator")

            if not self.test_mode:
                self.flame.flame_log("Awaiting contact with aggregator node...success",
                                     log_type=LogTypeLiteral.INFO.value)
        else:
            analyzer_ids = self.flame.get_participant_ids()
            if not self.test_mode:
                self.flame.flame_log("Awaiting contact with analyzer nodes...",
                                     log_type=LogTypeLiteral.INFO.value)
            ready_check_dict = self.flame.ready_check(analyzer_ids)
            if not all(ready_check_dict.values()):
                raise BrokenPipeError("Could not contact all analyzers")
            if not self.test_mode:
                self.flame.flame_log("Awaiting contact with analyzer nodes...success",
                                     log_type=LogTypeLiteral.INFO.value)

    def _get_data(self,
                  data_type: Literal['fhir', 's3'],
                  query: Optional[Union[str, list[str]]] = None) -> None:
        if type(query) == str:
            query = [query]

        if data_type == 'fhir':
            self.data = self.flame.get_fhir_data(query)
        else:
            self.data = self.flame.get_s3_data(query)
