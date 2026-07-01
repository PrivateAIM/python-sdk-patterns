from flame.proxy import ProxyModelTester
from examples.run_basic_proxy import MyAnalyzer, MyProxy, MyAggregator


if __name__ == "__main__":
    data = [[{'Patient?_summary=count': {'total': 10}}],
            [{'Patient?_summary=count': {'total': 20}}],
            [{'Patient?_summary=count': {'total': 30}}],
            [{'Patient?_summary=count': {'total': 40}}]]

    ProxyModelTester(
        data_splits=data,
        analyzer=MyAnalyzer,
        proxy=MyProxy,
        aggregator=MyAggregator,
        data_type='fhir',
        query='Patient?_summary=count',
        num_proxy_nodes=2,
        output_type='str',
        filename='test/results/test_basic_proxy_result.txt'
    )
