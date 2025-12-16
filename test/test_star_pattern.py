import math
from typing import Any, Optional
from flame.star import  StarAnalyzer, StarAggregator
from flame.star.test_star_model import test_star_model


class MyAnalyzer(StarAnalyzer):
    def __init__(self, flame, latest_result=None):
        super().__init__(flame)
        self.latest_result = latest_result

    def analysis_method(self, data, aggregator_results):
        self.flame.flame_log(f"aggregator_results in MyAnalyzer: {aggregator_results}", log_type='debug')
        analysis_result = sum(data) / len(data) \
            if aggregator_results is None \
            else (sum(data) / len(data) + aggregator_results) / 2
        self.flame.flame_log(f"MyAnalysis result ({self.id}): {analysis_result}", log_type='notice')
        return analysis_result


class MyAggregator(StarAggregator):
    def __init__(self, flame, latest_result=None):
        super().__init__(flame)
        self.latest_result = latest_result

    def aggregation_method(self, analysis_results: list[Any]) -> Any:
        result = sum(analysis_results) / len(analysis_results)
        self.flame.flame_log(f"MyAggregator result ({self.id}): {result}", log_type='notice')
        return result

    def has_converged(self, result: Any, last_result: Optional[Any]) -> bool:
        return math.fabs(result - last_result) < 0.01


if __name__ == "__main__":
    data_1 = [1, 2, 3, 4]
    data_2 = [5, 6, 7, 8]
    data_splits = [data_1, data_2]

    test_star_model(data_splits,                # TODO: Insert your data fragments in a list
                    MyAnalyzer,                 # TODO: Replace with your custom Analyzer class
                    MyAggregator,               # TODO: Replace with your custom Aggregator class
                    's3',               # TODO: Specify data type ('fhir' or 's3')
                    simple_analysis=False)

