from typing import Optional, Type, Literal, Union, Any

from flamesdk import FlameCoreSDK
from flame.star.aggregator_client import Aggregator
from flame.star.analyzer_client import Analyzer
from flame.star.star_model import StarModel, _ERROR_MESSAGES
from flame.utils.mock_flame_core import MockFlameCoreSDK


class StarLocalDPModel(StarModel):
    flame: Union[FlameCoreSDK, MockFlameCoreSDK]

    data: Optional[list[dict[str, Any]]] = None
    test_mode: bool = False

    epsilon: Optional[float]
    sensitivity: Optional[float]

    def __init__(self,
                 analyzer: Type[Analyzer],
                 aggregator: Type[Aggregator],
                 data_type: Literal['fhir', 's3'],
                 query: Optional[Union[str, list[str]]] = None,
                 simple_analysis: bool = True,
                 output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                 analyzer_kwargs: Optional[dict] = None,
                 aggregator_kwargs: Optional[dict] = None,
                 epsilon: Optional[float] = None,
                 sensitivity: Optional[float] = None,
                 test_mode: bool = False,
                 test_kwargs: Optional[dict] = None) -> None:
        self.epsilon = epsilon
        self.sensitivity = sensitivity
        super().__init__(analyzer=analyzer,
                         aggregator=aggregator,
                         data_type=data_type,
                         query=query,
                         simple_analysis=simple_analysis,
                         output_type=output_type,
                         analyzer_kwargs=analyzer_kwargs,
                         aggregator_kwargs=aggregator_kwargs,
                         test_mode=test_mode,
                         test_kwargs=test_kwargs)

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
                agg_res, converged, delta_crit = aggregator.aggregate(list(result_dict.values()), simple_analysis)
                self.flame.flame_log(f"Aggregated results: {str(agg_res)[:100]}")

                if converged:
                    if not self.test_mode:
                        self.flame.flame_log("Submitting final results using differential privacy...",
                                             log_type='info',
                                             end='')
                    if delta_crit and (self.epsilon is not None) and (self.sensitivity is not None):
                        local_dp = {"epsilon": self.epsilon, "sensitivity": self.sensitivity}
                    else:
                        local_dp = None
                    if self.test_mode and (local_dp is not None):
                        self.flame.flame_log(f"\tTest mode: Would apply local DP with epsilon={local_dp['epsilon']} "
                                             f"and sensitivity={local_dp['sensitivity']}",
                                             log_type='info')
                    response = self.flame.submit_final_result(agg_res, output_type, local_dp=local_dp)
                    if not self.test_mode:
                        self.flame.flame_log(f"success (response={response})", log_type='info')
                    self.flame.analysis_finished()
                    aggregator.node_finished()  # LOOP BREAK
                else:
                    # Send aggregated result to analyzers
                    self.flame.send_intermediate_data(analyzers, agg_res)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)
