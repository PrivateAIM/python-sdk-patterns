from typing import Optional, Type, Literal, Union

from flamesdk import FlameCoreSDK
from flame.star.aggregator_client import Aggregator
from flame.star.analyzer_client import Analyzer
from flame.star.star_model import StarModel, _ERROR_MESSAGES


class StarLocalDPModel(StarModel):
    flame: FlameCoreSDK

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
                 sensitivity: Optional[float] = None) -> None:
        super().__init__(analyzer=analyzer,
                         aggregator=aggregator,
                         data_type=data_type,
                         query=query,
                         simple_analysis=simple_analysis,
                         output_type=output_type,
                         analyzer_kwargs=analyzer_kwargs,
                         aggregator_kwargs=aggregator_kwargs)
        self.epsilon = epsilon
        self.sensitivity = sensitivity

    def _start_aggregator(self,
                          aggregator: Type[Aggregator],
                          simple_analysis: bool = True,
                          output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                          aggregator_kwargs: Optional[dict] = None) -> None:
        if self._is_aggregator():
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

                while not self._converged():  # (**)
                    # Await intermediate results
                    result_dict = self.flame.await_intermediate_data(analyzers)

                    # Aggregate results
                    agg_res, converged = aggregator.aggregate(list(result_dict.values()), simple_analysis)
                    self.flame.flame_log(f"Aggregated results: {str(agg_res)[:100]}")

                    if converged:
                        self.flame.flame_log("Submitting final results using differential privacy...", end='')
                        if self.epsilon and self.sensitivity:
                            localdp = {"epsilon": self.epsilon, "sensitivity": self.sensitivity}
                        else:
                            localdp = None
                        response = self.flame.submit_final_result(agg_res, output_type, localdp=localdp)
                        self.flame.flame_log(f"success (response={response})")
                        self.flame.analysis_finished()  # LOOP BREAK
                    else:
                        # Send aggregated result to analyzers
                        self.flame.send_intermediate_data(analyzers, agg_res)

                aggregator.node_finished()
            else:
                raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_ANALYZER.value)