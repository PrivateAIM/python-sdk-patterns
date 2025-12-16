from typing import Any, Type, Literal, Optional, Union
import pickle
from flame.star import StarModel, StarAnalyzer, StarAggregator
from flame.utils.mock_flame_core import MockFlameCoreSDK


def test_star_model(data_splits: list[Any],
                    analyzer: Type[StarAnalyzer],
                    aggregator: Type[StarAggregator],
                    data_type: Literal['fhir', 's3'],
                    query: Optional[Union[str, list[str]]] = None,
                    simple_analysis: bool = True,
                    output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                    analyzer_kwargs: dict = {},
                    aggregator_kwargs: dict = {},
                    result_filepath: str = None) -> None:
    latest_result = None
    num_iterations = 0
    converged = False
    while not converged:
        num_iterations += 1
        print(f"--- Starting Iteration {num_iterations} ---")

        analyzer_kwargs['latest_result'] = latest_result
        aggregator_kwargs['latest_result'] = latest_result

        result, agg_kwargs = simulate_nodes(data_splits,
                                            analyzer,
                                            aggregator,
                                            data_type,
                                            query,
                                            output_type,
                                            analyzer_kwargs,
                                            aggregator_kwargs)

        if simple_analysis:
            write_result(result, output_type, result_filepath)
            converged = True
        else:
            converged = check_convergence(aggregator, agg_kwargs, result, latest_result)
            if converged:
                write_result(result, output_type, result_filepath)
            else:
                latest_result = result

        print(f"--- Ending Iteration {num_iterations} ---\n")


def simulate_nodes(data_splits: list[Any],
                    analyzer: Type[StarAnalyzer],
                    aggregator: Type[StarAggregator],
                    data_type: Literal['fhir', 's3'],
                    query: Optional[Union[str, list[str]]],
                    output_type: Literal['str', 'bytes', 'pickle'],
                    analyzer_kwargs: dict,
                    aggregator_kwargs: dict):
    agg_index = len(data_splits)
    sim_nodes = {}
    agg_kwargs = None
    for i in range(len(data_splits) + 1):
        node_id = f"node_{i}"
        test_kwargs = {f'{data_type}_data': data_splits[i] if i < agg_index else None,
                       'node_id': node_id,
                       'aggregator': f"node_{len(data_splits)}",
                       'participant_ids': [f"node_{j}" for j in range(len(data_splits) + 1) if i != j],
                       'role': 'default' if i < agg_index else 'aggregator',
                       'analysis_id': "analysis_id",
                       'project_id': "project_id",
                       }
        if i == agg_index:
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
                                       test_kwargs=test_kwargs
                                       )
    return sim_nodes[f"node_{agg_index}"].flame.final_results_storage, agg_kwargs


def check_convergence(aggregator: Type[StarAggregator],
                      agg_kwargs: dict[str, str],
                      result: Any,
                      latest_result: Any) -> bool:
    if latest_result is not None:
        return aggregator(MockFlameCoreSDK(agg_kwargs)).has_converged(result=result,
                                                                      last_result=latest_result)
    else:
        return False


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
