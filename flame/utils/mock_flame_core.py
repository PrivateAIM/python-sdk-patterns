import pickle
import time
import os
import uuid

from httpx import AsyncClient
from io import StringIO
from typing import Any, Literal, Optional, Union

from opendp.mod import enable_features
from opendp.domains import atom_domain
from opendp.measurements import make_laplace
from opendp.metrics import absolute_distance

from flamesdk.resources.utils.constants import LogTypeLiteral


_REQUIRED_KWARGS = ['node_id', 'aggregator_id', 'role', 'participants']

CHECKPOINT_TAG_PREFIX = 'checkpoint-'

_LOG_TYPE_LITERALS_COLORS = {LogTypeLiteral.INFO.value: 36,
                             LogTypeLiteral.NOTICE.value: 32,
                             LogTypeLiteral.DEBUG.value: 90,
                             LogTypeLiteral.WARNING.value: 33,
                             LogTypeLiteral.ALERT.value: 91,
                             LogTypeLiteral.EMERGENCY.value: 35,
                             LogTypeLiteral.ERROR.value: 31,
                             LogTypeLiteral.CRITICAL.value: 41}


class MockConfig:
    def __init__(self, test_kwargs) -> None:
        self.node_id: str = test_kwargs["node_id"]
        self.aggregator_id: str = test_kwargs["aggregator_id"]
        self.participants: list[dict[str, str]] = test_kwargs['participants']
        self.node_role: str = test_kwargs["role"]
        self.finished: bool = False


class IterationTracker:
    def __init__(self):
        self.iter = 0

    def increment(self):
        self.iter += 1

    def get_iterations(self):
        return self.iter


class MockFlameCoreSDK:
    num_iterations: IterationTracker = IterationTracker()
    logger: dict[str, list[str]] = {}
    message_broker: dict[str, list[dict[str, Any]]] = {}
    final_results_storage: Optional[Any] = None
    stop_event: list[tuple[str]] = []

    def __init__(self, test_kwargs):
        self.__sanity_check__(test_kwargs)
        self.config = MockConfig(test_kwargs)
        self.data = test_kwargs.get('fhir_data') or test_kwargs.get('s3_data')

        self._test_kwargs = test_kwargs
        self.progress = 0
        self.incoming_message_queue = []
        self.outgoing_message_queue = []

        self.logger[self.get_id()] = [self.get_role(), self.progress, '']

        node_id = self.get_id()
        if node_id not in self.message_broker:
            self.message_broker[node_id] = []

    def __sanity_check__(self, test_kwargs) -> None:
        required_kwargs_check = all([k in test_kwargs.keys() for k in _REQUIRED_KWARGS])
        data_given = 'fhir_data' in test_kwargs.keys() or 's3_data' in test_kwargs.keys()
        if not required_kwargs_check:
            print('\n'.join([f"{k} in test_kwargs: {k in test_kwargs.keys()}" for k in _REQUIRED_KWARGS]))
            raise ValueError("test_kwargs must include 'node_id', 'aggregator_id', 'role', and 'participants' keys.")
        if not data_given:
            raise ValueError("test_kwargs must include either 'fhir_data' or 's3_data' key with corresponding data.")

    ########################################General##################################################
    def get_aggregator_id(self) -> Optional[str]:
        return self.config.aggregator_id

    def get_participants(self) -> list[dict[str, str]]:
        return self.config.participants

    def get_participant_ids(self) -> list[str]:
        return [v for participant in self.config.participants for k, v in participant.items() if k == 'id']

    def get_analysis_id(self) -> str:
        return self._test_kwargs.get('analysis_id', 'analysis_123')

    def get_project_id(self) -> str:
        return self._test_kwargs.get('project_id', 'project_123')

    def get_id(self) -> str:
        return self.config.node_id

    def get_role(self) -> str:
        return self.config.node_role

    def get_self_node_index(self) -> int:
        """
        Returns the index of the executing node id from list containing all analysis node ids sorted alphanumerically.
        :return: the node id index
        """
        return self.get_node_index(self.get_id())

    def get_node_index(self, node_id: str) -> Optional[int]:
        """
        Returns the index of the given node id from list containing all analysis node ids sorted alphanumerically.
        If the given id cannot be found returns None.
        :return: the node id index or None
        """
        id_list = self.get_participant_ids()
        id_list.append(self.get_id())
        if node_id in id_list:
            return sorted(id_list).index(node_id)
        else:
            self.flame_log(f"\tSearched node id '{node_id}' not found during indexing attempt",
                           log_type= LogTypeLiteral.WARNING.value)
            return None


    def node_has_data(self) -> bool:
        """
        Returns whether the node has access to data via DataAPI.
        Used for distinguishing between analyzer nodes (with data) and proxy nodes (without data).
        """
        return self._test_kwargs.get('has_data', True)

    def analysis_finished(self) -> bool:
        if self.get_participant_ids():
            self.send_message(self.get_participant_ids(),
                              "analysis_finished",
                              {},
                              max_attempts=5,
                              attempt_timeout=30)
            self.config.finished = True
        return True

    def ready_check(self,
                    nodes: list[str] = 'all',
                    attempt_interval: int = 30,
                    timeout: Optional[int] = None) -> dict[str, bool]:
        if nodes == 'all':
            nodes = self.get_participants
        return {node: True for node in nodes}

    def flame_log(self,
                  msg: Union[str, bytes],
                  sep: str = ' ',
                  end: str = '\n',
                  file: object = None,
                  log_type: str = LogTypeLiteral.INFO.value,
                  append: bool = False,
                  halt_submission: bool = False,
                  hidden_error_msg: Optional[str] = None) -> None:
        if log_type in _LOG_TYPE_LITERALS_COLORS.keys():
            color = str(_LOG_TYPE_LITERALS_COLORS[log_type])
        else:
            color = str(_LOG_TYPE_LITERALS_COLORS[LogTypeLiteral.INFO.value])
        self.logger[self.get_id()][2] += f"\033[{color}m{msg}\033[0m{end}"
        if hidden_error_msg is not None:
            self.logger[self.get_id()][2] += f"\033[{color}m_HIDDEN:{hidden_error_msg}\033[0m{end}"

    def declare_log_types(self, new_log_types: dict[str, str]) -> None:
        pass

    def get_progress(self) -> int:
        return self.progress

    def set_progress(self, progress: Union[int, float]) -> None:
        if isinstance(progress, float):
            progress = int(progress)
        if not (0 <= progress <= 100):
            self.flame_log(msg=f"Invalid progress: {progress} (should be a numeric value between 0 and 100).",
                           log_type=LogTypeLiteral.WARNING.value)
        elif self.progress >= progress:
            self.flame_log(msg=f"Progress value needs to be higher to current progress (i.e. only register progress, "
                               f"if actual progress has been made).",
                           log_type=LogTypeLiteral.WARNING.value)
        else:
            self.progress = progress
            self.logger[self.get_id()][1] = progress

    def set_checkpoint(self, kwargs: dict[str, Any]) -> None:
        """
        Saves given kwargs into local node storage for future analysis retrieval. raise Warning for incorrect format
        of kwargs

        :param kwargs:
        :return:
        """
        if not isinstance(kwargs, dict):
            self.flame_log(msg=f'Expected dictionary object for kwargs in checkpoint save but received {type(kwargs)}.'
                               f' Could not save checkpoint',
                           log_type=LogTypeLiteral.WARNING.value)
        elif not any(isinstance(key, str) for key in kwargs.keys()):
            self.flame_log(msg=f'Expected string object for kwargs keys in checkpoint save but received '
                               f'{[type(k) for k in kwargs.keys()]}. Could not save checkpoint',
                           log_type=LogTypeLiteral.WARNING.value)
        else:
            i = len(self.get_local_tags(CHECKPOINT_TAG_PREFIX)) + 1
            self.flame_log(msg=f'Saved checkpoint no.{i}', log_type=LogTypeLiteral.INFO.value)
            self.save_intermediate_data(data= kwargs,
                                        location='local',
                                        tag=f"{CHECKPOINT_TAG_PREFIX}{i}")

    def load_checkpoint(self, index: int) -> Optional[dict[str, Any]]:
        """
        Load saved kwargs from previous checkpoint with given index. return None if not found

        :param index:
        :return kwargs:
        """
        locally_tagged_saves = self.get_local_tags(f"{CHECKPOINT_TAG_PREFIX}{index}")
        if len(locally_tagged_saves) == 1:
            self.flame_log(msg=f'Loading checkpoint no.{index}', log_type=LogTypeLiteral.INFO.value)
            return self.get_intermediate_data(location='local', tag=f"{CHECKPOINT_TAG_PREFIX}{index}")
        elif len(locally_tagged_saves) > 1:
            self.flame_log(msg=f'Error: Loading checkpoint no.{index} failed. Multiple saves under same tag found',
                           log_type=LogTypeLiteral.ERROR.value)
            return None
        else:
            self.flame_log(msg=f'No checkpoint {index} was found. Returning None',
                           log_type=LogTypeLiteral.WARNING.value)
            return None


    def fhir_to_csv(self,
                    fhir_data: dict[str, Any],
                    col_key_seq: str,
                    value_key_seq: str,
                    input_resource: str,
                    row_key_seq: Optional[str] = None,
                    row_id_filters: Optional[list[str]] = None,
                    col_id_filters: Optional[list[str]] = None,
                    row_col_name: str = '',
                    separator: str = ',',
                    output_type: Literal["file", "dict"] = "file"
                    ) -> Optional[Union[StringIO, dict[Any, dict[Any, Any]]]]:
        return None


    ########################################Message Broker Client####################################
    def send_message(self,
                     receivers: list[str],
                     message_category: str,
                     message: dict,
                     max_attempts: int = 1,
                     timeout: Optional[int] = None,
                     attempt_timeout: int = 10) -> tuple[list[str], list[str]]:
        sender = self.get_id()
        for r in receivers:
            if r not in self.message_broker.keys():
                self.message_broker[r] = []
            inbox = self.message_broker[r]
            inbox.append({
                "category": message_category,
                "sender": sender,
                "data": message,
            })
            self.message_broker[r] = inbox
        return receivers, []

    def await_messages(self,
                       senders: list[str],
                       message_category: str,
                       message_id: Optional[str] = None,
                       timeout: Optional[int] = None) -> dict[str, Optional[list[str]]]:
        if not isinstance(senders, list):
            raise ValueError(f"Senders should be provided as a list of participant ids. Not {senders} of type {type(senders)}.")
        else:
            for sender in senders:
                if sender not in self.get_participant_ids():
                    raise ValueError(f"Sender {sender} is not a valid participant id for this analysis.")

        node_id = self.get_id()

        while True:
            try:
                inbox = self.message_broker.get(node_id, [])
                if inbox:
                    finished_messages = [msg for msg in inbox if msg["category"] == 'analysis_finished']
                    if finished_messages:
                        self._node_finished()
                        break

                    msg_senders = [msg["sender"] for msg in inbox if msg["category"] == message_category]
                    if all(sender in msg_senders for sender in senders):
                        break
                raise KeyError
            except KeyError:
                if self.stop_event:
                    raise Exception
                time.sleep(.01)
                pass

        if not self.config.finished:
            remaining_msgs = []
            latest_results = {}
            for msg in inbox:
                if (msg["category"] == message_category) and (msg["sender"] in senders):
                    latest_results[msg["sender"]] = msg["data"]
                else:
                    remaining_msgs.append(msg)

            # retain only unconsumed messages
            self.message_broker[node_id] = remaining_msgs
            return latest_results
        else:
            return {self.config.aggregator_id: None}

    def get_messages(self, status: Literal['unread', 'read'] = 'unread') -> list[str]:
        pass

    def delete_messages(self, message_ids: list[str]) -> int:
        pass

    def clear_messages(self, status: Literal["read", "unread", "all"] = "read",
                       min_age: Optional[int] = None) -> int:
        pass

    def send_message_and_wait_for_responses(self,
                                            receivers: list[str],
                                            message_category: str,
                                            message: dict,
                                            max_attempts: int = 1,
                                            timeout: Optional[int] = None,
                                            attempt_timeout: int = 10) -> dict[str, Optional[list[str]]]:
        pass

    ########################################Storage Client###########################################
    def submit_final_result(self,
                            result: Any,
                            output_type: Union[Literal['str', 'bytes', 'pickle'], list] = 'str',
                            multiple_results: bool = False,
                            local_dp: Optional[dict] = None,
                            filename: Optional[Union[str, list[str]]] = None) -> Union[dict[str, str], list[dict[str, str]]]:
        if self.get_id() == self.get_aggregator_id():
            if local_dp is not None:
                if type(result) in [int, float]:
                    enable_features("contrib")
                    scale = local_dp['sensitivity'] / local_dp['epsilon']  # Laplace scale parameter
                    laplace_mech = make_laplace(input_domain=atom_domain(T=float, nan=False),
                                                input_metric=absolute_distance(T=float),
                                                scale=scale)
                    result = laplace_mech(float(result))
                else:
                    self.flame_log("Given result type is not supported for local DP -> DP step will be skipped.",
                                   log_type=LogTypeLiteral.WARNING.value)
            self.final_results_storage = result
            self.set_progress(100)
            self.__pop_logs__()
            return {"result": "submitted"}
        else:
            raise RuntimeError(f"Final results may only be submitted by the aggregator {self.get_aggregator_id()} "
                               f"(given node with id={self.get_id()}).")

    def save_intermediate_data(self,
                               data: Any,
                               location: Literal["local", "global"],
                               remote_node_ids: Optional[list[str]] = None,
                               tag: Optional[str] = None) -> Union[dict[str, dict[str, str]], dict[str, str]]:
        filename = str(uuid.uuid4())
        save_location = ""
        if location == "local":
            storage_dir = self._test_kwargs.local_storage_dir()
            if not storage_dir:
                os.mkdir(storage_dir)
            node_store = f"{storage_dir}/{self.get_id()}"
            if not os.path.exists(node_store):
                os.mkdir(node_store)

            if tag is not None:
                tagged_node_store = f"{node_store}/{tag}"
                if not os.path.exists(tagged_node_store):
                    os.mkdir(tagged_node_store)
                save_location = f"{tagged_node_store}/{filename}"
            else:
                save_location = f"{node_store}/{filename}"

            with open(save_location, "wb") as f:
                f.write(pickle.dumps(data))
            return {"saved": save_location}
        else:
            self.send_intermediate_data(receivers=remote_node_ids, data=data)

    def get_intermediate_data(self,
                              location: Literal["local", "global"],
                              id: Optional[str] = None,
                              tag: Optional[str] = None,
                              tag_option: Optional[Literal["all", "last","first"]] = "all",
                              sender_node_id: Optional[str] = None) -> Any:
        pass #TODO

    def send_intermediate_data(self,
                               receivers: list[str],
                               data: Any,
                               message_category: str = "intermediate_data",
                               max_attempts: int = 1,
                               timeout: Optional[int] = None,
                               attempt_timeout: int = 10,
                               encrypted: bool = False) -> tuple[list[str], list[str]]:
        receivers, _ = self.send_message(receivers=receivers,
                                         message_category=message_category,
                                         message=data,
                                         max_attempts=max_attempts,
                                         timeout=timeout)
        if self.get_id() == self.get_aggregator_id():
            self.__pop_logs__()
        return receivers, []

    def await_intermediate_data(self,
                                senders: list[str],
                                message_category: str = "intermediate_data",
                                timeout: Optional[int] = None) -> dict[str, Any]:
        return self.await_messages(senders=senders, message_category=message_category, timeout=timeout)

    def get_local_tags(self, filter: Optional[str] = None) -> list[str]:
        pass

    ########################################Data Client#######################################
    def get_data_client(self, data_id: str) -> Optional[AsyncClient]:
        pass

    def get_data_sources(self) -> Optional[list[str]]:
        pass

    def get_fhir_data(self, fhir_queries: Optional[list[str]] = None) -> Optional[list[Union[dict[str, dict], dict]]]:
        if 'fhir_data' in self._test_kwargs.keys():
            if (fhir_queries is not None) and (len(fhir_queries) != 0):
                return [{k: v for k, v in ds.items() if k in fhir_queries}
                        for ds in self.data if any(q in ds.keys() for q in fhir_queries)]
            else:
                return None
        else:
            raise ValueError("No FHIR data provided in test_kwargs.")

    def get_s3_data(self, s3_keys: Optional[list[str]] = None) -> Optional[list[Union[dict[str, str], str]]]:
        if 's3_data' in self._test_kwargs.keys():
            if s3_keys == []:
                return self.data
            if s3_keys is not None:
                return [{k: v for k, v in ds if k in s3_keys} for ds in self.data if
                        any(q in ds.keys() for q in s3_keys)]
            else:
                return None
        else:
            raise ValueError("No S3 data provided in test_kwargs.")

    def _node_finished(self) -> bool:
        self.config.finished = True
        return self.config.finished

    def __pop_logs__(self, failure_message: bool = False) -> None:
        print(f"--- Starting Iteration {self.__get_iteration__()} ---")
        if failure_message:
            self.flame_log("Exception was raised (see Stacktrace)!", log_type=LogTypeLiteral.ERROR.value)
        for k, v in self.logger.items():
            role, progress, log = v
            print(f"Logs for {'Analyzer' if role == 'default' else role.capitalize()} {k} (Progress: {progress}%):")
            self.logger[k] = [role, progress, '']
            print(log, end='')
        print(f"--- Ending Iteration {self.__get_iteration__()} ---\n")
        self.num_iterations.increment()

    def __get_iteration__(self):
        return self.num_iterations.get_iterations()
