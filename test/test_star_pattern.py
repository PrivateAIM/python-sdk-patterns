import math
from typing import Any, Type, Literal, Optional, Union

from flame.star import StarModel, StarAnalyzer, StarAggregator
from flamesdk import FlameCoreSDK
from flame.utils.mock_flame_core import MockFlameCoreSDK


class MyAnalyzer(StarAnalyzer):
    def __init__(self, flame, latest_result=None):
        super().__init__(flame)
        self.latest_result = latest_result

    def analysis_method(self, data, aggregator_results):
        print(f"data in MyAnalyzer: {data}")
        print(f"aggregator_results in MyAnalyzer: {aggregator_results}")
        if aggregator_results is None:
            return sum(data) / len(data)
        else:
            return (sum(data) / len(data) + aggregator_results) / 2

class MyAggregator(StarAggregator):
    def __init__(self, flame, latest_result=None):
        super().__init__(flame)
        self.latest_result = latest_result

    def aggregation_method(self, analysis_results: list[Any]) -> Any:
        print(f"analysis_results in MyAggregator: {analysis_results}")
        result = sum(analysis_results) / len(analysis_results)
        print(f"result: {result}")
        return result

    def has_converged(self, result: Any, last_result: Optional[Any]) -> bool:
        return math.fabs(result - last_result) < 0.01

def main(data_splits: list[Any],
         analyzer: Type[StarAnalyzer],
         aggregator: Type[StarAggregator],
         data_type: Literal['fhir', 's3'],
         query: Optional[Union[str, list[str]]] = None,
         simple_analysis: bool = True,
         output_type: Literal['str', 'bytes', 'pickle'] = 'str',
         analyzer_kwargs: dict = {},
         aggregator_kwargs: dict = {}) -> None:

    print("Simple analysis: " + str(simple_analysis))
    latest_result = None
    agg_index = len(data_splits)
    while True:
        sim_nodes = {}
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
            analyzer_kwargs['latest_result'] = latest_result
            aggregator_kwargs['latest_result'] = latest_result

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
        if simple_analysis:
            print("Final result (simple analysis): " + sim_nodes[f"node_{agg_index}"].flame.final_results_storage)
            break
        else:
            result =  sim_nodes[f"node_{agg_index}"].flame.final_results_storage
            if latest_result is not None:
                converged = aggregator(MockFlameCoreSDK(test_kwargs)).has_converged(result=result, last_result=latest_result)
            else:
                converged = False

            if converged:
                print("Final result (multi analysis): " + str(result))
                break
            else:
                latest_result = result
            print("")

if __name__ == "__main__":
    data_1 = [1, 2, 3, 4]
    data_2 = [5, 6, 7, 8]
    data_splits = [data_1, data_2]

    main(data_splits,       # TODO: Insert your data fragments in a list
         MyAnalyzer,        # TODO: Replace with your custom Analyzer class
         MyAggregator,      # TODO: Replace with your custom Aggregator class
         's3',      # TODO: Specify data type ('fhir' or 's3')
         simple_analysis=False)

