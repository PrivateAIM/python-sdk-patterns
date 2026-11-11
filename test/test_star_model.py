from flame.star import StarModelTester
from examples.run_star_model import MyAnalyzer, MyAggregator


if __name__ == "__main__":
    data = [[{'Patient?_summary=count': {'total': 10}}],
            [{'Patient?_summary=count': {'total': 18}}]]
    StarModelTester(data_splits=data,                       # TODO: Insert your data fragments in a list
                    analyzer=MyAnalyzer,                    # TODO: Replace with your custom Analyzer class
                    aggregator=MyAggregator,                # TODO: Replace with your custom Aggregator class
                    data_type='s3',                         # TODO: Specify data type ('fhir' or 's3')
                    simple_analysis=False)
