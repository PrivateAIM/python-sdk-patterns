from flame.proxy import ProxyModelTester
from examples.run_basic_proxy import MyAnalyzer, MyProxy, MyAggregator, DATA_FILENAME


if __name__ == "__main__":
    data = [[{DATA_FILENAME: b'10'}],
            [{DATA_FILENAME: b'20'}],
            [{DATA_FILENAME: b'30'}],
            [{DATA_FILENAME: b'40'}]]

    ProxyModelTester(
        data_splits=data,
        analyzer=MyAnalyzer,
        proxy=MyProxy,
        aggregator=MyAggregator,
        data_type='s3',
        query=DATA_FILENAME,
        num_proxy_nodes=2,
        simple_analysis=False,
        output_type='str',
        filename='test/results/test_basic_proxy_result.txt'
    )
