from typing import Any, Optional
from flame.star import StarModelTester, StarAnalyzer, StarAggregator


class MyAnalyzer(StarAnalyzer):
    def __init__(self, flame):
        super().__init__(flame)
        # Custom instance variable — persists across all iterations because nodes are reused
        self.local_history: list[float] = []

    def analysis_method(self, data, aggregator_results):
        self.flame.flame_log(f"\tAggregator results in MyAnalyzer: {aggregator_results}", log_type='debug')

        local_mean = sum(data) / len(data)

        # Append current local mean to history — self.local_history grows each iteration
        self.local_history.append(local_mean)
        self.flame.flame_log(
            f"\tLocal history so far ({self.id}): {self.local_history}", log_type='debug'
        )

        if aggregator_results is None:
            analysis_result = local_mean
        else:
            # Blend local mean with aggregator feedback, weighted by how many rounds of
            # history we have accumulated locally
            weight = min(len(self.local_history) / 10, 0.9)
            analysis_result = (1 - weight) * aggregator_results + weight * local_mean

        self.flame.flame_log(f"MyAnalysis result ({self.id}): {analysis_result}", log_type='notice')
        return analysis_result


class MyAggregator(StarAggregator):
    def __init__(self, flame):
        super().__init__(flame)

    def aggregation_method(self, analysis_results: list[Any]) -> Any:
        self.flame.flame_log(f"\tAnalysis results in MyAggregator: {analysis_results}", log_type='notice')
        result = sum(analysis_results) / len(analysis_results)
        self.flame.flame_log(f"MyAggregator result ({self.id}): {result}", log_type='notice')
        return result

    def has_converged(self, result: Any, last_result: Optional[Any]) -> bool:
        self.flame.flame_log(f"\tLast result: {last_result}, Current result: {result}", log_type="notice")
        self.flame.flame_log(f"\tChecking convergence at iteration {self.num_iterations}", log_type="notice")
        return self.num_iterations >= 5  # Limit to 5 iterations for testing


if __name__ == "__main__":
    data_1 = [1, 2, 3, 4]
    data_2 = [5, 6, 7, 8]
    data_splits = [data_1, data_2]

    StarModelTester(data_splits=data_splits,                # TODO: Insert your data fragments in a list
                    analyzer=MyAnalyzer,                    # TODO: Replace with your custom Analyzer class
                    aggregator=MyAggregator,                # TODO: Replace with your custom Aggregator class
                    data_type='s3',                         # TODO: Specify data type ('fhir' or 's3')
                    simple_analysis=False)
