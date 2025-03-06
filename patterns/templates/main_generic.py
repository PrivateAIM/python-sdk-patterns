from flame import FlameCoreSDK

from patterns.templates.aggregator_generic import Pattern_Aggregator
from patterns.templates.analyzer_generic import Pattern_Analyzer


def main():
    # start the communication with the flame message protocols, and enable access api
    flame = FlameCoreSDK()
    # await participating nodes to come online and test communication
    flame.ready_check()

    # start node in aggregator or analysis mode
    if flame.is_aggregator():
        aggregator = Pattern_Aggregator()

        aggr_result = []
        while not aggregator.has_converged(aggr_result):
            results = flame.await_intermediate_data(senders=flame.get_participant_ids())
            aggr_result = aggregator.aggregate(results)

        flame.submit_final_result(result=aggr_result, output_type='pickle')
        flame.analysis_finished()

    elif flame.is_analyzer():
        analyzer = Pattern_Analyzer()

        data = flame.get_fhir_data()
        aggr_result = None
        while True:
            analyzer.analyze_or_train(data, aggr_result)
            flame.send_intermediate_data(receivers=flame.get_aggregator_id(), data=data)
            aggr_result = flame.await_intermediate_data(senders=[flame.get_aggregator_id()])[flame.get_aggregator_id()]

    else:
        raise ValueError("Fatal: Found unknown value for node mode.")


if __name__ == "__main__":
    main()
