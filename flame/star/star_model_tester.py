import pickle
import threading
import uuid
from typing import Any, Type, Literal, Optional, Union
import traceback

from flame.star import StarModel, StarLocalDPModel, StarAnalyzer, StarAggregator
from flame.utils.mock_flame_core import MockFlameCoreSDK


class StarModelTester:
    def __init__(self,
                 data_splits: list[Any],
                 analyzer: Type[StarAnalyzer],
                 aggregator: Type[StarAggregator],
                 data_type: Literal['fhir', 's3'],
                 node_roles: Optional[list[str]] = None,
                 query: Optional[Union[str, list[str]]] = None,
                 simple_analysis: bool = True,
                 output_type: Union[Literal['str', 'bytes', 'pickle'], list] = 'str',
                 multiple_results: bool = False,
                 analyzer_kwargs: Optional[dict] = None,
                 aggregator_kwargs: Optional[dict] = None,
                 epsilon: Optional[float] = None,
                 sensitivity: Optional[float] = None,
                 result_filepath: Optional[Union[str, list[str]]] = None) -> None:
        num_splits = len(data_splits)
        self.test_input(data_splits[0])
        if node_roles is None:
            node_roles = ['default' for _ in range(len(data_splits))]
        participant_ids = [str(uuid.uuid4()) for _ in range(len(node_roles) + 1)]

        threads = []
        thread_errors = {}
        results_queue = []
        MockFlameCoreSDK.stop_event = []  # shared stop event for all threads in case of failure in any thread
        for i, participant_id in enumerate(participant_ids):
            test_kwargs = {
                'analyzer': analyzer,
                'aggregator': aggregator,
                'data_type': data_type,
                'query': query,
                'simple_analysis': simple_analysis,
                'output_type': output_type,
                'multiple_results': multiple_results,
                'analyzer_kwargs': analyzer_kwargs,
                'aggregator_kwargs': aggregator_kwargs,
                'test_mode': True,
                'test_kwargs': {f'{data_type}_data': data_splits[i] if i < num_splits else None,
                                'node_id': participant_id,
                                'aggregator_id': participant_ids[-1],
                                'participant_ids': [participant_ids[j] for j in range(num_splits + 1) if i != j],
                                'role': node_roles[i] if i < num_splits else 'aggregator',
                                'analysis_id': "analysis_id",
                                'project_id': "project_id"
                                }
            }
            use_local_dp = (epsilon is not None) and (sensitivity is not None)
            if use_local_dp:
                test_kwargs['epsilon'] = epsilon
                test_kwargs['sensitivity'] = sensitivity

            def run_node(kwargs=test_kwargs, use_dp=use_local_dp):
                try:
                    if not use_dp:
                        flame = StarModel(**kwargs).flame
                    else:
                        flame = StarLocalDPModel(**kwargs).flame
                    results_queue.append(flame.final_results_storage)
                except Exception:
                    stop_event = MockFlameCoreSDK.stop_event
                    if not stop_event:
                        stack_trace = traceback.format_exc()
                        thread_errors[(kwargs['test_kwargs']['role'],
                                       kwargs['test_kwargs']['node_id'])] = f"\033[31m{stack_trace}\033[0m"
                        stop_event.append(kwargs['test_kwargs']['node_id'])
                        mock = MockFlameCoreSDK(test_kwargs=kwargs['test_kwargs'])
                        mock.__pop_logs__(failure_message=True)
                    else:
                        thread_errors[(kwargs['test_kwargs']['role'],
                                       kwargs['test_kwargs']['node_id'])] = (Exception("Another thread already failed, "
                                                                                       "stopping this thread as well."))
                    return

            thread = threading.Thread(target=run_node)
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()


        # write final results
        if results_queue:
            self.write_result(results_queue[0], output_type, result_filepath, multiple_results)
        else:
            print("No results to write. All threads failed with errors:")
            for (role, node_id), error in thread_errors.items():
                print(f"\t{(role if role != 'default' else 'analyzer').capitalize()} {node_id}: {error}")

    @staticmethod
    def test_input(data: Any) -> None:
        is_list = isinstance(data, list)
        try:
            contains_ds = len(data) != 0
        except TypeError:
            contains_ds = False
        try:
            contains_data = isinstance(data[0], dict)
        except TypeError:
            contains_data = False
        if (not is_list) or (not contains_ds) or (not contains_data):
            print("\033[93mWarning! Data readied in FLAME's architecture will always be a list of dictionaries at "
                  "every node.\n\tHere, each dictionary corresponds to a datasource within the node (ex. if multiple "
                  "s3-buckets are connected to a single analysis).\n\tThe dictionary items, depending on whether you "
                  "are accessing s3 or fhir data, either each correspond to a dataset in the datasource for s3 or a "
                  "fhir bundle for fhir.\n\t\t* For s3, the items contain the dataset names as keys and the datasets "
                  "in bytes format as values.\n\t\t* For fhir, the items contain the queries used to retrieve the "
                  "bundles as keys and the bundles as dictionaries as values.\nTo summarize: You see this warning "
                  "because the data used for testing here is not in line with this format, which may result in your "
                  "analysis working locally during testing, but not in the actual architecture.\nIn order to get rid "
                  "of this warning make sure your data fulfills the following criteria, and your analysis accommodates "
                  "this input format:\033[0m")
            if not is_list:
                print("\033[93m\t* Format your splits as lists.\033[0m")
            if not contains_ds:
                print("\033[93m\t* Fill your datasource lists with data.\033[0m")
            if not contains_data:
                print("\033[93m\t* Ensure your data is set as dictionaries containing datasets (dataset names as keys, "
                      "and the datasets as values (bytes format for s3, fhir bundles for fhir)).\033[0m")
        else:
            pass

    @staticmethod
    def write_result(result: Any,
                     output_type: Union[Literal['str', 'bytes', 'pickle'], list],
                     result_filepath: Optional[Union[str, list[str]]] = None,
                     multiple_results: bool = False) -> None:
        if multiple_results:
            if isinstance(result, list) or isinstance(result, tuple):
                if isinstance(result_filepath, list) and (len(result_filepath) != len(result)):
                    print(f"Warning! Inconsistent number of result_filepaths (len={result_filepath}) "
                          f"and results (len={len(result)}) -> multiple_results will be ignored.")
                    multi_iterable_results = False
                else:
                    multi_iterable_results = True
            else:
                print(f"Warning! Given multiple_results={multiple_results}, but result is neither of type "
                      f"'list' nor 'tuple' (found {type(result)} instead) -> multiple_results will be ignored.")
                multi_iterable_results = False
        else:
            multi_iterable_results = False

        if result_filepath is not None:
            if not multi_iterable_results:
                result = [result]
                result_filepath = [result_filepath]

            for i, res in enumerate(result):
                if isinstance(result_filepath, list):
                    current_path = result_filepath[i]
                else:
                    if '.' in result_filepath:
                        result_filename, result_extension = result_filepath.rsplit('.', 1)
                        current_path = f"{result_filename}_{i + 1}.{result_extension}"
                    else:
                        current_path = f"{result_filepath}_{i + 1}"
                if isinstance(output_type, list) and (len(output_type) == len(result)):
                    out_type = output_type[i]
                else:
                    out_type = output_type
                if out_type == 'str':
                    with open(current_path, 'w') as f:
                        f.write(str(res))
                elif out_type == 'pickle':
                    with open(current_path, 'wb') as f:
                        f.write(pickle.dumps(res))
                else:
                    with open(current_path, 'wb') as f:
                        f.write(res)
                print(f"Final result{f'_{i + 1}' if multi_iterable_results else ''} written to {current_path}")
        else:
            if multi_iterable_results:
                for i, res in enumerate(result):
                    print(f"Final result_{i + 1}: {res}")
            else:
                print(f"Final result: {result}")
