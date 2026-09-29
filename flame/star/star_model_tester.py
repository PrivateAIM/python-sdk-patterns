"""Run a whole star analysis locally, one thread per simulated node.

This lets an analysis be developed and debugged on a laptop: every node runs
against :class:`~flame.utils.mock_flame_core.MockFlameCoreSDK`, which fakes the
message broker, the data sources and the result storage in process.
"""

import pickle
import random
import threading
import traceback
import uuid
from typing import Any, Literal

from flame.star import StarAggregator, StarAnalyzer, StarLocalDPModel, StarModel
from flame.utils.mock_flame_core import MockFlameCoreSDK

#: Fixed first node id, so that repeated local runs produce the same node ids.
_SEED_NODE_ID = "7b484c11-ef77-5789-a75f-cdedb8ef5963"

#: Seed for the node id generator, likewise for reproducibility across runs.
_NODE_ID_RANDOM_SEED = 42

#: Offset added to the first node id to derive the aggregator's id.
_AGGREGATOR_ID_OFFSET = 10

# ANSI escapes used to colour the console output of a local test run.
_YELLOW = "\033[93m"
_RED = "\033[31m"
_RESET = "\033[0m"

_INPUT_FORMAT_WARNING = (
    "Warning! Data readied in FLAME's architecture will always be a list of dictionaries at "
    "every node.\n\tHere, each dictionary corresponds to a datasource within the node (ex. if multiple "
    "s3-buckets are connected to a single analysis).\n\tThe dictionary items, depending on whether you "
    "are accessing s3 or fhir data, either each correspond to a dataset in the datasource for s3 or a "
    "fhir bundle for fhir.\n\t\t* For s3, the items contain the dataset names as keys and the datasets "
    "in bytes format as values.\n\t\t* For fhir, the items contain the queries used to retrieve the "
    "bundles as keys and the bundles as dictionaries as values.\nTo summarize: You see this warning "
    "because the data used for testing here is not in line with this format, which may result in your "
    "analysis working locally during testing, but not in the actual architecture.\nIn order to get rid "
    "of this warning make sure your data fulfills the following criteria, and your analysis accommodates "
    "this input format:"
)


class StarModelTester:
    """Simulates a full star analysis in threads and writes out its result.

    One thread is started per data split (the analyzers) plus one for the
    aggregator. All threads share the mock SDK's class-level message broker, so
    they communicate exactly as the real nodes would.

    As with :class:`~flame.star.star_model.StarModel`, constructing the class
    runs the analysis: once the constructor returns, the final result has been
    printed or written to disk.

    If any thread raises, the failure is recorded, the remaining threads are
    asked to stop through the mock SDK's shared stop event, and the collected
    stack traces are printed instead of a result.
    """

    def __init__(
        self,
        data_splits: list[Any],
        analyzer: type[StarAnalyzer],
        aggregator: type[StarAggregator],
        data_type: Literal["fhir", "s3"],
        node_roles: list[str] | None = None,
        query: str | list[str] | None = None,
        simple_analysis: bool = True,
        output_type: Literal["str", "bytes", "pickle"] | list = "str",
        multiple_results: bool = False,
        filename: str | list[str] | None = None,
        stream_log_level: int = 20,
        analyzer_kwargs: dict | None = None,
        aggregator_kwargs: dict | None = None,
        load_checkpoint: int | None = None,
        local_storage_dir: str | None = None,
        epsilon: float | None = None,
        sensitivity: float | None = None,
    ) -> None:
        """Set up the simulated nodes, run them, and report the final result.

        Supplying both ``epsilon`` and ``sensitivity`` switches the simulation
        from :class:`~flame.star.star_model.StarModel` to
        :class:`~flame.star.star_localdp.star_localdp_model.StarLocalDPModel`.

        :param data_splits: One data split per analyzer node. Each split should
            be a list of dicts, mirroring the real data layout; see
            :meth:`test_input`.
        :param analyzer: Subclass of
            :class:`~flame.star.analyzer_client.Analyzer` to run on each
            analyzer node.
        :param aggregator: Subclass of
            :class:`~flame.star.aggregator_client.Aggregator` to run on the
            aggregator node.
        :param data_type: Which data source the splits stand in for.
        :param node_roles: Explicit role per node; defaults to "default" for
            every split plus a trailing "aggregator".
        :param query: FHIR query/queries or S3 key(s) to fetch from the mock.
        :param simple_analysis: True for a one-shot analysis (a single round);
            False to iterate until the aggregator reports convergence.
        :param output_type: Serialization of the final result.
        :param multiple_results: Whether the final result is a collection to be
            written out as several separate results.
        :param filename: Where to write the final result(s). If None, the result
            is printed instead.
        :param stream_log_level: Log level forwarded to the model.
        :param analyzer_kwargs: Extra keyword arguments for the ``analyzer``.
        :param aggregator_kwargs: Extra keyword arguments for the ``aggregator``.
        :param load_checkpoint: Index of a checkpoint to resume from, if any.
        :param local_storage_dir: Directory the mock SDK uses to emulate local
            node storage; defaults to the working directory.
        :param epsilon: Privacy budget for local DP; see
            :class:`~flame.star.star_localdp.star_localdp_model.StarLocalDPModel`.
        :param sensitivity: Sensitivity for local DP.
        :raises ValueError: If ``node_roles`` does not match ``data_splits``.
        """
        num_splits = len(data_splits)
        self.test_input(data_splits[0])
        participants = []
        if node_roles is not None and len(node_roles) != len(data_splits):
            raise ValueError(
                f"Length of node_roles ({len(node_roles)}) must be equal to length of data_splits "
                f"({len(data_splits)}), if node_roles is provided."
            )

        # Derive one deterministic uuid per node: analyzers get increasing ids,
        # the aggregator a fixed offset from the first one.
        node_ids = [_SEED_NODE_ID]
        random.seed(_NODE_ID_RANDOM_SEED)
        for i in range(len(data_splits) + 1):
            participant_role = (
                ("default" if i < len(data_splits) else "aggregator")
                if node_roles is None
                else (node_roles[i] if i < len(node_roles) else "aggregator")
            )
            if participant_role == "aggregator":
                participant_id = str(uuid.UUID(int=uuid.UUID(node_ids[0]).int + _AGGREGATOR_ID_OFFSET))
            else:
                participant_id = node_ids[-1]
                node_ids.append(str(uuid.UUID(int=uuid.UUID(node_ids[-1]).int + random.randint(11, 2**100))))
            participants.append({"id": participant_id, "role": participant_role})

        threads = []
        thread_errors = {}
        results_queue = []
        MockFlameCoreSDK.stop_event = []  # shared stop event for all threads in case of failure in any thread
        for i, participant in enumerate(participants):
            participant_id = participant["id"]
            participant_role = participant["role"]
            test_kwargs = {
                "analyzer": analyzer,
                "aggregator": aggregator,
                "data_type": data_type,
                "query": query,
                "simple_analysis": simple_analysis,
                "output_type": output_type,
                "multiple_results": multiple_results,
                "stream_log_level": stream_log_level,
                "analyzer_kwargs": analyzer_kwargs,
                "aggregator_kwargs": aggregator_kwargs,
                "load_checkpoint": load_checkpoint,
                "test_mode": True,
                "test_kwargs": {
                    f"{data_type}_data": data_splits[i] if i < num_splits else None,
                    "node_id": participant_id,
                    "aggregator_id": participants[-1]["id"],
                    "participants": [part for j, part in enumerate(participants) if i != j],
                    "role": participant_role,
                    "analysis_id": "analysis_id",
                    "project_id": "project_id",
                    "local_storage_dir": local_storage_dir,
                },
            }
            use_local_dp = (epsilon is not None) and (sensitivity is not None)
            if use_local_dp:
                test_kwargs["epsilon"] = epsilon
                test_kwargs["sensitivity"] = sensitivity

            # kwargs/use_dp are bound as defaults so each thread captures its
            # own values rather than the last ones seen by the loop.
            def run_node(kwargs=test_kwargs, use_dp=use_local_dp):
                """Run one simulated node; record its result or its failure."""
                try:
                    if not use_dp:
                        flame = StarModel(**kwargs).flame
                    else:
                        flame = StarLocalDPModel(**kwargs).flame
                    if kwargs["test_kwargs"]["role"] == "aggregator":
                        results_queue.append(flame.final_results_storage)
                except Exception:
                    stop_event = MockFlameCoreSDK.stop_event
                    if not stop_event:
                        # First failure: record the trace, flush this node's
                        # logs, and signal every other thread to give up.
                        stack_trace = traceback.format_exc()
                        thread_errors[(kwargs["test_kwargs"]["role"], kwargs["test_kwargs"]["node_id"])] = (
                            f"{_RED}{stack_trace}{_RESET}"
                        )
                        stop_event.append(kwargs["test_kwargs"]["node_id"])
                        mock = MockFlameCoreSDK(test_kwargs=kwargs["test_kwargs"])
                        mock.__pop_logs__(failure_message=True)
                    else:
                        thread_errors[(kwargs["test_kwargs"]["role"], kwargs["test_kwargs"]["node_id"])] = Exception(
                            "Another thread already failed, stopping this thread as well."
                        )

            thread = threading.Thread(target=run_node)
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # write final results
        if results_queue:
            self.write_result(results_queue[0], output_type, filename, multiple_results)
        else:
            print("No results to write. All threads failed with errors:")
            for (role, node_id), error in thread_errors.items():
                print(f"\t{(role if role != 'default' else 'analyzer').capitalize()} {node_id}: {error}")

    @staticmethod
    def test_input(data: Any) -> None:
        """Warn if a data split does not look like real FLAME node data.

        Real nodes always receive a list of dicts (one dict per data source), so
        an analysis written against a differently shaped local split may pass
        locally and then fail in the actual architecture. This only prints a
        warning; it never raises.

        :param data: One data split, as handed to :meth:`__init__`.
        """
        is_list = isinstance(data, list)
        try:
            contains_ds = len(data) != 0
        except TypeError:
            contains_ds = False
        try:
            contains_data = isinstance(data[0], dict)
        except TypeError:
            contains_data = False
        except KeyError:
            contains_data = False
        if (not is_list) or (not contains_ds) or (not contains_data):
            print(f"{_YELLOW}{_INPUT_FORMAT_WARNING}{_RESET}")
            if not is_list:
                print(f"{_YELLOW}\t* Format your splits as lists.{_RESET}")
            if not contains_ds:
                print(f"{_YELLOW}\t* Fill your datasource lists with data.{_RESET}")
            if not contains_data:
                print(
                    f"{_YELLOW}\t* Ensure your data is set as dictionaries containing datasets (dataset names as keys, "
                    f"and the datasets as values (bytes format for s3, fhir bundles for fhir)).{_RESET}"
                )

    @staticmethod
    def write_result(
        result: Any,
        output_type: Literal["str", "bytes", "pickle"] | list,
        filename: str | list[str] | None = None,
        multiple_results: bool = False,
    ) -> None:
        """Write the aggregator's final result to disk, or print it.

        With ``multiple_results`` set and an iterable result, each element is
        written separately. A single ``filename`` is then numbered per element
        (``out.txt`` -> ``out_1.txt``, ``out_2.txt``, ...), while a list of
        filenames is used as given. ``multiple_results`` is ignored - with a
        warning - if the result is not a list/tuple, or if a list of filenames
        does not match the number of results.

        If ``filename`` is None the result is printed instead of written.

        :param result: The aggregator's final result.
        :param output_type: How to serialize each result: "str" writes
            ``str(res)`` as text, "pickle" writes a pickle, anything else writes
            the raw bytes. A list applies one entry per result.
        :param filename: Target path(s), or None to print.
        :param multiple_results: Whether to write the result element-wise.
        """
        if multiple_results:
            if isinstance(result, (list, tuple)):
                if isinstance(filename, list) and (len(filename) != len(result)):
                    print(
                        f"Warning! Inconsistent number of filenames (len={filename}) "
                        f"and results (len={len(result)}) -> multiple_results will be ignored."
                    )
                    multi_iterable_results = False
                else:
                    multi_iterable_results = True
            else:
                print(
                    f"Warning! Given multiple_results={multiple_results}, but result is neither of type "
                    f"'list' nor 'tuple' (found {type(result)} instead) -> multiple_results will be ignored."
                )
                multi_iterable_results = False
        else:
            multi_iterable_results = False

        if filename is not None:
            if not multi_iterable_results:
                result = [result]
                if not isinstance(filename, list):
                    filename = [filename]

            for i, res in enumerate(result):
                if isinstance(filename, list):
                    current_path = filename[i]
                else:
                    # Single filename for several results: number them, keeping
                    # any file extension last.
                    if "." in filename:
                        result_filename, result_extension = filename.rsplit(".", 1)
                        current_path = f"{result_filename}_{i + 1}.{result_extension}"
                    else:
                        current_path = f"{filename}_{i + 1}"
                if isinstance(output_type, list) and (len(output_type) == len(result)):
                    out_type = output_type[i]
                else:
                    out_type = output_type
                if out_type == "str":
                    with open(current_path, "w") as f:
                        f.write(str(res))
                elif out_type == "pickle":
                    with open(current_path, "wb") as f:
                        f.write(pickle.dumps(res))
                else:
                    with open(current_path, "wb") as f:
                        f.write(res)
                print(f"Final result{f'_{i + 1}' if multi_iterable_results else ''} written to {current_path}")
        else:
            if multi_iterable_results:
                for i, res in enumerate(result):
                    print(f"Final result_{i + 1}: {res}")
            else:
                print(f"Final result: {result}")
