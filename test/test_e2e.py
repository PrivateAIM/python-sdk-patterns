"""HALTA-FLAME: Federated Long COVID (PASC) analysis pipeline.
"""
from flame.star import  StarModelTester
from examples.run_e2e import HaltaAnalyzer, HaltaAggregator, RESULT_FILENAMES


if __name__ == "__main__":
    data = [[{'synthetic_eucare_1_labeled.csv': open('test/data/node1/synthetic_eucare_1_labeled.csv', 'rb').read()}],
            [{'synthetic_eucare_2_labeled.csv': open('test/data/node2/synthetic_eucare_2_labeled.csv', 'rb').read()}]]

    StarModelTester(
        data_splits=data,
        analyzer=HaltaAnalyzer,
        aggregator=HaltaAggregator,
        data_type='s3',
        query=[],
        multiple_results=True,
        simple_analysis=False,
        output_type=['bytes', 'str', 'bytes', 'bytes'],
        filename=['test/results/' + f for f in RESULT_FILENAMES]
    )
