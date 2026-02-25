import pickle
from typing import Any, Type, Literal, Optional, Union

from flame.star import StarModel, StarLocalDPModel, StarAnalyzer, StarAggregator


class StarModelTester:
    latest_result: Optional[Any] = None
    num_iterations: int = 0

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
        test_kwargs_list = None
        converged = False
        while not converged:
            print(f"--- Starting Iteration {self.num_iterations} ---")

            result, test_kwargs_list = self.sim_iter(data_splits,
                                                     analyzer,
                                                     aggregator,
                                                     data_type,
                                                     query,
                                                     output_type,
                                                     multiple_results,
                                                     analyzer_kwargs,
                                                     aggregator_kwargs,
                                                     epsilon,
                                                     sensitivity,
                                                     test_kwargs_list)

            test_agg_kwargs = test_kwargs_list[-1]
            if simple_analysis:
                self.write_result(result, output_type, result_filepath, multiple_results)
                converged = True
            else:
                converged = test_agg_kwargs['attributes']['delta_criteria']
                if converged:
                    self.write_result(result, output_type, result_filepath, multiple_results)
                else:
                    self.latest_result = result

            print(f"--- Ending Iteration {self.num_iterations} ---\n")
            self.num_iterations += 1

    def sim_iter(self,
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
                 sensitivity: Optional[float] = None,
                 test_kwargs_list: Optional[list[dict]] = None) -> tuple[Any, list[dict[str, Any]]]:
        sim_nodes = {}
        num_splits = len(data_splits)
        for i in range(num_splits + 1):
            node_id = f"node_{i}"
            if test_kwargs_list is None:
                test_kwargs = {f'{data_type}_data': data_splits[i] if i < num_splits else None,
                               'node_id': node_id,
                               'aggregator': f"node_{num_splits}",
                               'participant_ids': [f"node_{j}" for j in range(num_splits + 1) if i != j],
                               'role': 'default' if i < num_splits else 'aggregator',
                               'analysis_id': "analysis_id",
                               'project_id': "project_id",
                               'attributes': {}
                               }
            else:
                test_kwargs = test_kwargs_list[i]
            test_kwargs['attributes']['num_iterations'] = self.num_iterations
            test_kwargs['attributes']['latest_result'] = self.latest_result

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
        return sim_nodes[f"node_{num_splits}"].flame.final_results_storage, [v.test_kwargs for v in sim_nodes.values()]

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
