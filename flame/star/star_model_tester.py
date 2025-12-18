import pickle
from typing import Any, Type, Literal, Optional, Union

from flame.star import StarModel, StarAnalyzer, StarAggregator
from flame.utils.mock_flame_core import MockFlameCoreSDK


class StarModelTester:
    agg_index: int

    latest_result: Optional[Any] = None
    num_iterations: int = 0
    converged: bool = False

    def __init__(self,
                 data_splits: list[Any],
                 analyzer: Type[StarAnalyzer],
                 aggregator: Type[StarAggregator],
                 data_type: Literal['fhir', 's3'],
                 query: Optional[Union[str, list[str]]] = None,
                 simple_analysis: bool = True,
                 output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                 analyzer_kwargs: Optional[dict] = None,
                 aggregator_kwargs: Optional[dict] = None,
                 result_filepath: str = None) -> None:
        self.agg_index = len(data_splits)
        while not self.converged:
            print(f"--- Starting Iteration {self.num_iterations} ---")

            result, agg_kwargs = self.simulate_nodes(data_splits,
                                                     analyzer,
                                                     aggregator,
                                                     data_type,
                                                     query,
                                                     output_type,
                                                     analyzer_kwargs,
                                                     aggregator_kwargs)
            if simple_analysis:
                self.write_result(result, output_type, result_filepath)
                self.converged = True
            else:
                self.converged = self.check_convergence(aggregator, agg_kwargs, result)
                if self.converged:
                    self.write_result(result, output_type, result_filepath)
                else:
                    self.latest_result = result

            print(f"--- Ending Iteration {self.num_iterations} ---\n")
            self.num_iterations += 1

    def simulate_nodes(self,
                       data_splits: list[Any],
                       analyzer: Type[StarAnalyzer],
                       aggregator: Type[StarAggregator],
                       data_type: Literal['fhir', 's3'],
                       query: Optional[Union[str, list[str]]],
                       output_type: Literal['str', 'bytes', 'pickle'],
                       analyzer_kwargs: Optional[dict] = None,
                       aggregator_kwargs: Optional[dict] = None) -> tuple[Any, dict[str, Any]]:
        sim_nodes = {}
        agg_kwargs = None
        for i in range(len(data_splits) + 1):
            node_id = f"node_{i}"
            test_kwargs = {f'{data_type}_data': data_splits[i] if i < self.agg_index else None,
                           'node_id': node_id,
                           'aggregator': f"node_{len(data_splits)}",
                           'participant_ids': [f"node_{j}" for j in range(len(data_splits) + 1) if i != j],
                           'role': 'default' if i < self.agg_index else 'aggregator',
                           'analysis_id': "analysis_id",
                           'project_id': "project_id",
                           'num_iterations': self.num_iterations,
                           'latest_result': self.latest_result}
            if i == self.agg_index:
                agg_kwargs = test_kwargs

            sim_nodes[node_id] = StarModel(analyzer,
                                           aggregator,
                                           data_type,
                                           query,
                                           True,
                                           output_type,
                                           analyzer_kwargs,
                                           aggregator_kwargs,
                                           test_mode=True,
                                           test_kwargs=test_kwargs)
        return sim_nodes[f"node_{self.agg_index}"].flame.final_results_storage, agg_kwargs

    @staticmethod
    def check_convergence(aggregator: Type[StarAggregator],
                          agg_kwargs: dict[str, Any],
                          result: Any) -> bool:
        if all(k in agg_kwargs.keys() for k in ('num_iterations', 'latest_result')):
            agg = aggregator(MockFlameCoreSDK(test_kwargs=agg_kwargs))
            agg.set_num_iterations(agg_kwargs['num_iterations'])
            if agg.num_iterations != 0:
                return agg.has_converged(result=result, last_result=agg_kwargs['latest_result'])
            else:
                return False
        else:
            return False

    @staticmethod
    def write_result(result: Any,
                     output_type: Literal['str', 'bytes', 'pickle'],
                     result_filepath: str) -> None:
        if result_filepath is not None:
            if output_type == 'str':
                with open(result_filepath, 'w') as f:
                    f.write(str(result))
            elif output_type == 'pickle':
                with open(result_filepath, 'wb') as f:
                    f.write(pickle.dumps(result))
            else:
                with open(result_filepath, 'wb') as f:
                    f.write(result)
            print(f"Final result (simple analysis) written to {result_filepath}")
        else:
            print(f"Final result (simple analysis): {result}")
