from typing import Any, Optional

from flame.proxy import ProxyModel, ProxyAnalyzer, Proxy, ProxyAggregator


class MyAnalyzer(ProxyAnalyzer):
    proxy_params: Optional[dict[str, tuple[float, float]]] = None

    def __init__(self, flame):
        super().__init__(flame)  # Connects this analyzer to the FLAME components

    def analysis_method(self, data, aggregator_results) -> Any:
        """
        Performs analysis on the retrieved data from data sources.

        :param data: A list of dictionaries containing the data from each data source.
                     - Each dictionary corresponds to a data source.
                     - Keys are the queries executed, and values are the results (dict for FHIR, str for S3).
        :param aggregator_results: Results from the aggregator in previous iterations.
                                   - None in the first iteration.
                                   - Contains the result from the aggregator's aggregation_method in subsequent iterations.
        :return: Any result of your analysis on one node (ex. patient count).
        """
        # TODO: Implement your analysis method
        #  in this example we first retrieve a fhir dataset, extract patient counts,
        #  and then take the total number of patients
        patient_count = float(data[0]['Patient?_summary=count']['total'])
        return patient_count


class MyProxy(Proxy):
    def __init__(self, flame):
        super().__init__(flame)  # Connects this analyzer to the FLAME components

    def proxy_aggregation_method(self, analysis_results: list[Any]) -> Any:
        """
        Aggregates the results received from all analyzer nodes assigned to this proxy.

        :param analysis_results: A list of analysis results from each analyzer node.
        :return: The aggregated result (e.g., total patient count across all analyzers).
        """
        # TODO: Implement your proxy_aggregation method
        #  in this example we sum up the total patient counts across the analysis nodes assigned to this proxy
        sub_total_patient_count = sum(analysis_results)
        return sub_total_patient_count


class MyAggregator(ProxyAggregator):
    def __init__(self, flame):
        super().__init__(flame)

    def aggregation_method(self, proxy_results):
        """
        Aggregates the results received from all proxy nodes (never has direct access to analyzer results).

        :param proxy_results: A list of pre-aggregated results from each proxy node.
        :return: The aggregated result (e.g., total patient count across all analyzers).
        """
        # TODO: Implement your aggregation method
        #  in this example we sum up the total patient counts across all proxy nodes
        total_patient_count = sum(proxy_results)
        return total_patient_count

    def has_converged(self, result, last_result):
        return True


def main():
    ProxyModel(
        analyzer=MyAnalyzer,             # Custom analyzer class
        proxy=MyProxy,                   # Custom proxy class
        aggregator=MyAggregator,         # Custom aggregator class
        data_type='fhir',                # Type of data source ('fhir' or 's3')
        query='Patient?_summary=count',  # Query or list of queries to retrieve data
        num_proxy_nodes=1,               # Number of proxy nodes partaking in this analysis
        simple_analysis=True,            # True for single-iteration; False for multi-iterative analysis
        output_type='str',               # Output format for the final result ('str', 'bytes', or 'pickle')
        multiple_results=False,          # Can be set to True to return highest iterable-level of results as separate files
        analyzer_kwargs=None,            # Additional keyword arguments for the custom analyzer constructor (i.e. MyAnalyzer)
        aggregator_kwargs=None           # Additional keyword arguments for the custom aggregator constructor (i.e. MyAggregator)
    )


if __name__ == "__main__":
    main()
