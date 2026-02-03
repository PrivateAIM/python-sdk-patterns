import pickle
from typing import Any, Type, Literal, Optional, Union

from flame.star import StarModel, StarLocalDPModel, StarAnalyzer, StarAggregator
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
                 multiple_results: bool = False,
                 analyzer_kwargs: Optional[dict] = None,
                 aggregator_kwargs: Optional[dict] = None,
                 epsilon: Optional[float] = None,
                 sensitivity: Optional[float] = None,
                 result_filepath: Optional[Union[str, list[str]]] = None) -> None:
        self.agg_index = len(data_splits)
        while not self.converged:
            print(f"--- Starting Iteration {self.num_iterations} ---")

            result, test_agg_kwargs = self.simulate_nodes(data_splits,
                                                          analyzer,
                                                          aggregator,
                                                          data_type,
                                                          query,
                                                          output_type,
                                                          multiple_results,
                                                          analyzer_kwargs,
                                                          aggregator_kwargs,
                                                          epsilon,
                                                          sensitivity)
            if simple_analysis:
                self.write_result(result, output_type, result_filepath, multiple_results)
                self.converged = True
            else:
                self.converged = self.check_convergence(aggregator, test_agg_kwargs, result, aggregator_kwargs)
                if self.converged:
                    self.write_result(result, output_type, result_filepath, multiple_results)
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
                       multiple_results: bool = False,
                       analyzer_kwargs: Optional[dict] = None,
                       aggregator_kwargs: Optional[dict] = None,
                       epsilon: Optional[float] = None,
                       sensitivity: Optional[float] = None) -> tuple[Any, dict[str, Any]]:
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

            if (epsilon is None) or (sensitivity is None):
                sim_nodes[node_id] = StarModel(analyzer,
                                               aggregator,
                                               data_type,
                                               query,
                                               True,
                                               output_type,
                                               multiple_results,
                                               analyzer_kwargs,
                                               aggregator_kwargs,
                                               test_mode=True,
                                               test_kwargs=test_kwargs)
            else:
                sim_nodes[node_id] = StarLocalDPModel(analyzer,
                                                      aggregator,
                                                      data_type,
                                                      query,
                                                      True,
                                                      output_type,
                                                      multiple_results,
                                                      analyzer_kwargs,
                                                      aggregator_kwargs,
                                                      epsilon=epsilon,
                                                      sensitivity=sensitivity,
                                                      test_mode=True,
                                                      test_kwargs=test_kwargs)
        return sim_nodes[f"node_{self.agg_index}"].flame.final_results_storage, agg_kwargs

    @staticmethod
    def check_convergence(aggregator: Type[StarAggregator],
                          test_agg_kwargs: dict[str, Any],
                          result: Any,
                          aggregator_kwargs: Optional[dict] = None) -> bool:
        if all(k in test_agg_kwargs.keys() for k in ('num_iterations', 'latest_result')):
            if aggregator_kwargs is None:
                agg = aggregator(MockFlameCoreSDK(test_kwargs=test_agg_kwargs))
            else:
                agg = aggregator(MockFlameCoreSDK(test_kwargs=test_agg_kwargs), **aggregator_kwargs)
            agg.set_num_iterations(test_agg_kwargs['num_iterations'])

            if agg.num_iterations != 0:
                return agg.has_converged(result=result, last_result=test_agg_kwargs['latest_result'])
            else:
                return False
        else:
            return False

    @staticmethod
    def write_result(result: Any,
                     output_type: Literal['str', 'bytes', 'pickle'],
                     result_filepath: Optional[Union[str, list[str]]] = None,
                     multiple_results: bool = False) -> None:
        if multiple_results:
            if isinstance(result, list) or isinstance(result, tuple):
                if isinstance(result_filepath, list) and (len(result_filepath) != len(result)):
                    print(f"Warning! Inconsistent number of result_filepaths (len={result_filepath}) "
                          f"and results (len={len(result)}) -> multiple_results will be ignored.")
                    multi_iterable_results = False
                else:
                    multi_iterable_results = True
            else:
                print(f"Warning! Given multiple_results={multiple_results}, but result is neither of type "
                      f"'list' nor 'tuple' (found {type(result)} instead) -> multiple_results will be ignored.")
                multi_iterable_results = False
        else:
            multi_iterable_results = False

        if result_filepath is not None:
            if not multi_iterable_results:
                result = [result]
                result_filepath = [result_filepath]

            for i, res in enumerate(result):
                if isinstance(result_filepath, list):
                    current_path = result_filepath[i]
                else:
                    if '.' in result_filepath:
                        result_filename, result_extension = result_filepath.rsplit('.', 1)
                        current_path = f"{result_filename}_{i + 1}.{result_extension}"
                    else:
                        current_path = f"{result_filepath}_{i + 1}"
                if output_type == 'str':
                    with open(current_path, 'w') as f:
                        f.write(str(res))
                elif output_type == 'pickle':
                    with open(current_path, 'wb') as f:
                        f.write(pickle.dumps(res))
                else:
                    with open(current_path, 'wb') as f:
                        f.write(res)
                print(f"Final result{f'_{i + 1}' if multi_iterable_results else ''} written to {current_path}")
        else:
            if multi_iterable_results:
                for i, res in enumerate(result):
                    print(f"Final result_{i + 1}: {res}")
            else:
                print(f"Final result: {result}")
