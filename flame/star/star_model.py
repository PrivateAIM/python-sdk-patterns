from enum import Enum
from typing import Optional, Type, Literal, Union

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
    data: Optional[list[dict[str, Union[dict, str]]]] = None

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
        if not test_mode:
            if FlameCoreSDK is None:
                raise ModuleNotFoundError("flamesdk is required when test_mode is False")
            self.flame = FlameCoreSDK()
        else:
            self.flame = MockFlameCoreSDK(test_kwargs=test_kwargs)

        if self._is_analyzer():
            self.flame.flame_log("Analyzer started")
            self._start_analyzer(analyzer,
                                 data_type=data_type,
                                 query=query,
                                 simple_analysis=simple_analysis,
                                 analyzer_kwargs=analyzer_kwargs)
        elif self._is_aggregator():
            self.flame.flame_log("Aggregator started")
            self._start_aggregator(aggregator,
                                   simple_analysis=simple_analysis,
                                   output_type=output_type,
                                   aggregator_kwargs=aggregator_kwargs)
        else:
            raise BrokenPipeError("Has to be either analyzer or aggregator")
        self.flame.flame_log("Analysis finished!")

    def _is_aggregator(self) -> bool:
        return self.flame.get_role() == 'aggregator'

    def _is_analyzer(self) -> bool:
        return self.flame.get_role() == 'default'

    def _start_aggregator(self,
                          aggregator: Type[Aggregator],
                          simple_analysis: bool = True,
                          output_type: Literal['str', 'bytes', 'pickle'] = 'str',
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
                result_dict = self.flame.await_intermediate_data(analyzers)

                # Aggregate results
                agg_res, converged = aggregator.aggregate(list(result_dict.values()), simple_analysis)
                self.flame.flame_log(f"Aggregated results: {str(agg_res)[:100]}")

                if converged:
                    self.flame.flame_log("Submitting final results...", end='')
                    response = self.flame.submit_final_result(agg_res, output_type)
                    self.flame.flame_log(f"success (response={response})")
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
            self.flame.flame_log(f"Data extracted: {str(self.data)[:100]}")

            agg_res = None
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

    def _wait_until_partners_ready(self):
        if self._is_analyzer():
            aggregator_id = self.flame.get_aggregator_id()
            self.flame.flame_log("Awaiting contact with aggregator node...")
            ready_check_dict = self.flame.ready_check([aggregator_id])

            if not ready_check_dict[aggregator_id]:
                raise BrokenPipeError("Could not contact aggregator")

            self.flame.flame_log("Awaiting contact with aggregator node...success")
        else:
            analyzer_ids = self.flame.get_participant_ids()
            self.flame.flame_log("Awaiting contact with analyzer nodes...")
            ready_check_dict = self.flame.ready_check(analyzer_ids)
            if not all(ready_check_dict.values()):
                raise BrokenPipeError("Could not contact all analyzers")
            self.flame.flame_log("Awaiting contact with analyzer nodes...success")

    def _get_data(self,
                  data_type: Literal['fhir', 's3'],
                  query: Optional[Union[str, list[str]]] = None) -> None:
        if type(query) == str:
            query = [query]

        if data_type == 'fhir':
            self.data = self.flame.get_fhir_data(query)
        else:
            self.data = self.flame.get_s3_data(query)
