import time
from enum import Enum
from httpx import AsyncClient
from io import StringIO
from typing import Any, Literal, Optional, Union

from opendp.mod import enable_features
from opendp.domains import atom_domain
from opendp.measurements import make_laplace
from opendp.metrics import absolute_distance


_REQUIRED_KWARGS = ['node_id', 'aggregator_id', 'role', 'participants']


class HUB_LOG_LITERALS(Enum):
    info_log = 'info'
    notice_message = 'notice'
    debug_log = 'debug'
    warning_log = 'warn'
    alert_log = 'alert'
    emergency_log = 'emerg'
    error_code = 'error'
    critical_error_code = 'crit'


_LOG_TYPE_LITERALS = {'info': (HUB_LOG_LITERALS.info_log.value, 36),
                      'normal': (HUB_LOG_LITERALS.info_log.value, 39),
                      'notice': (HUB_LOG_LITERALS.notice_message.value, 32),
                      'debug': (HUB_LOG_LITERALS.debug_log.value, 90),
                      'warning': (HUB_LOG_LITERALS.warning_log.value, 33),
                      'alert': (HUB_LOG_LITERALS.alert_log.value, 91),
                      'emergency': (HUB_LOG_LITERALS.emergency_log.value, 35),
                      'error': (HUB_LOG_LITERALS.error_code.value, 31),
                      'critical-error': (HUB_LOG_LITERALS.critical_error_code.value, 41)}


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
        self.sanity_check(test_kwargs)
        self.config = MockConfig(test_kwargs)
        self.data = test_kwargs.get('fhir_data') or test_kwargs.get('s3_data')
        self.logger[self.get_id()] = [self.get_role(), '']

        self._test_kwargs = test_kwargs
        self.progress = 0
        self.incoming_message_queue = []
        self.outgoing_message_queue = []

        node_id = self.get_id()
        if node_id not in self.message_broker:
            self.message_broker[node_id] = []

    def sanity_check(self, test_kwargs) -> None:
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
                  log_type: str = 'normal',
                  suppress_head: bool = False,
                  halt_submission: bool = False) -> None:
        if log_type in _LOG_TYPE_LITERALS.keys():
            color = str(_LOG_TYPE_LITERALS[log_type][1])
        else:
            color = str(_LOG_TYPE_LITERALS['normal'][1])
        self.logger[self.get_id()][1] += f"\033[{color}m{msg}\033[0m{end}"

    def declare_log_types(self, new_log_types: dict[str, str]) -> None:
        pass

    def get_progress(self) -> int:
        return self.progress

    def set_progress(self, progress: Union[int, float]) -> None:
        if isinstance(progress, float):
            progress = int(progress)
        if not (0 <= progress <= 100):
            self.flame_log(msg=f"Invalid progress: {progress} (should be a numeric value between 0 and 100).")
        elif self.progress > progress:
            self.flame_log(msg=f"Progress value needs to be higher to current progress (i.e. only register progress, "
                               f"if actual progress has been made).")
        else:
            self.progress = progress

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
                            local_dp: Optional[dict] = None) -> Union[dict[str, str], list[dict[str, str]]]:
        if self.get_id() == self.get_aggregator_id():
            if local_dp is not None:
                if type(result) in [int, float]:
                    enable_features("contrib")
                    scale = local_dp['sensitivity'] / local_dp['epsilon']  # Laplace scale parameter
                    laplace_mech = make_laplace(input_domain=atom_domain(T=float),
                                                input_metric=absolute_distance(T=float),
                                                scale=scale)
                    result = laplace_mech(float(result))
                else:
                    self.flame_log("Given result type is not supported for local DP -> DP step will be skipped.",
                                   log_type='warning')
            self.final_results_storage = result
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
        pass

    def get_intermediate_data(self,
                              location: Literal["local", "global"],
                              id: Optional[str] = None,
                              tag: Optional[str] = None,
                              tag_option: Optional[Literal["all", "last","first"]] = "all",
                              sender_node_id: Optional[str] = None) -> Any:
        pass

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
                                         timeout=timeout,)
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
            return self.data
        else:
            raise ValueError("No FHIR data provided in test_kwargs.")

    def get_s3_data(self, s3_keys: Optional[list[str]] = None) -> Optional[list[Union[dict[str, str], str]]]:
        if 's3_data' in self._test_kwargs.keys():
            return self.data
        else:
            raise ValueError("No S3 data provided in test_kwargs.")

    def _node_finished(self) -> bool:
        self.config.finished = True
        return self.config.finished

    def __pop_logs__(self, failure_message: bool = False) -> None:
        print(f"--- Starting Iteration {self.__get_iteration__()} ---")
        if failure_message:
            self.flame_log("Exception was raised (see Stacktrace)!", log_type='error')
        for k, v in self.logger.items():
            role, log = self.logger[k]
            print(f"Logs for {'Analyzer' if role == 'default' else role.capitalize()} {k}:")
            self.logger[k] = [role, '']
            print(log, end='')
        print(f"--- Ending Iteration {self.__get_iteration__()} ---\n")
        self.num_iterations.increment()

    def __get_iteration__(self):
        return self.num_iterations.get_iterations()
