"""In-process stand-in for ``flamesdk.FlameCoreSDK``, used for local testing.

:class:`MockFlameCoreSDK` mirrors the public surface of the real SDK but keeps
everything in memory (or, for local storage, in a directory on disk), so that a
whole federated analysis can be exercised in threads on one machine. The message
broker, the log buffer and the final result storage are deliberately *class*
attributes: every simulated node shares them, which is what lets the nodes talk
to each other.

Anything that has no local equivalent - the data API client, remote tags, global
storage - is stubbed out or downgraded to its local counterpart with a warning.
"""

import os
import pickle
import time
import uuid
from io import StringIO
from typing import Any, ClassVar, Literal

from flamesdk.resources.utils.constants import LogTypeLiteral
from httpx import AsyncClient
from opendp.domains import atom_domain
from opendp.measurements import make_laplace
from opendp.metrics import absolute_distance
from opendp.mod import enable_features

#: Keys every caller must supply in ``test_kwargs``; see :meth:`MockFlameCoreSDK.__sanity_check__`.
_REQUIRED_KWARGS = ["node_id", "aggregator_id", "role", "participants"]

#: Prefix under which checkpoints are tagged in the emulated local storage.
CHECKPOINT_TAG_PREFIX = "checkpoint-"

#: Message category the aggregator broadcasts to tell every node to shut down.
_ANALYSIS_FINISHED_CATEGORY = "analysis_finished"

#: ANSI foreground colour per log type, so local runs are readable at a glance.
_LOG_TYPE_LITERALS_COLORS = {
    LogTypeLiteral.INFO.value: 36,
    LogTypeLiteral.NOTICE.value: 32,
    LogTypeLiteral.DEBUG.value: 90,
    LogTypeLiteral.WARNING.value: 33,
    LogTypeLiteral.ALERT.value: 91,
    LogTypeLiteral.EMERGENCY.value: 35,
    LogTypeLiteral.ERROR.value: 31,
    LogTypeLiteral.CRITICAL.value: 41,
}


class MockConfig:
    """Stands in for the real SDK's node configuration object.

    :ivar node_id: Id of the simulated node.
    :ivar aggregator_id: Id of the aggregator in this simulated analysis.
    :ivar participants: The other nodes, as ``{"id": ..., "role": ...}`` dicts.
    :ivar node_role: Role of this node ("default", "proxy" or "aggregator").
    :ivar finished: Whether this node has been told the analysis is over.
    """

    def __init__(self, test_kwargs) -> None:
        """Read the simulated node's configuration out of ``test_kwargs``.

        :param test_kwargs: Mock configuration; see
            :meth:`MockFlameCoreSDK.__sanity_check__` for the required keys.
        """
        self.node_id: str = test_kwargs["node_id"]
        self.aggregator_id: str = test_kwargs["aggregator_id"]
        self.participants: list[dict[str, str]] = test_kwargs["participants"]
        self.node_role: str = test_kwargs["role"]
        self.finished: bool = False


class IterationTracker:
    """A shared round counter, used to label the log output of a local run."""

    def __init__(self):
        """Start counting at zero."""
        self.iter = 0

    def increment(self):
        """Advance to the next round."""
        self.iter += 1

    def get_iterations(self):
        """:return: The number of rounds completed so far."""
        return self.iter


class MockMessage:
    """A single message in the emulated broker.

    :ivar body: The message payload.
    """

    def __init__(self, message: dict):
        """Wrap a payload, rejecting the field the broker reserves for itself.

        :param message: The message payload.
        :raises KeyError: If the payload uses the reserved key "meta".
        """
        if "meta" in message:
            raise KeyError(
                "Cannot use field 'meta' in message body. "
                "This field is reserved for meta data used by the message broker."
            )
        self.body = message


class MockFlameCoreSDK:
    """A local, in-memory replacement for the FLAME Core SDK.

    One instance stands for one simulated node. The class attributes below are
    shared by *all* instances in the process on purpose: they are what makes
    inter-node communication and the joint log output work when the pattern
    testers run every node in its own thread.

    :cvar num_iterations: Shared round counter used when printing logs.
    :cvar logger: Buffered output per node id, as ``[role, progress, text]``.
    :cvar message_broker: Inbox per node id - the emulated message broker.
    :cvar final_results_storage: What the aggregator submitted, read back by the
        pattern testers once all threads have joined.
    :cvar stop_event: Non-empty once some thread has failed, which tells the
        others to abandon their wait loops.
    """

    num_iterations: ClassVar[IterationTracker] = IterationTracker()
    logger: ClassVar[dict[str, list[str]]] = {}
    message_broker: ClassVar[dict[str, list[MockMessage]]] = {}
    final_results_storage: Any | None = None
    stop_event: ClassVar[list[tuple[str]]] = []

    def __init__(self, test_kwargs):
        """Register a simulated node with the shared broker and log buffer.

        :param test_kwargs: Mock configuration; see :meth:`__sanity_check__`.
        :raises ValueError: If required keys or the node's data are missing.
        """
        self.__sanity_check__(test_kwargs)
        self.config = MockConfig(test_kwargs)
        self.data = test_kwargs.get("fhir_data") or test_kwargs.get("s3_data")

        self._test_kwargs = test_kwargs
        self.progress = 0
        self.incoming_message_queue = []
        self.outgoing_message_queue = []

        self.logger[self.get_id()] = [self.get_role(), self.progress, ""]

        node_id = self.get_id()
        if node_id not in self.message_broker:
            self.message_broker[node_id] = []

    def __sanity_check__(self, test_kwargs) -> None:
        """Verify that ``test_kwargs`` describes a usable simulated node.

        :param test_kwargs: Mock configuration to validate. It must contain
            every key in :data:`_REQUIRED_KWARGS` plus either "fhir_data" or
            "s3_data".
        :raises ValueError: If a required key or the data is missing.
        """
        required_kwargs_check = all(k in test_kwargs for k in _REQUIRED_KWARGS)
        data_given = "fhir_data" in test_kwargs or "s3_data" in test_kwargs
        if not required_kwargs_check:
            print("\n".join([f"{k} in test_kwargs: {k in test_kwargs}" for k in _REQUIRED_KWARGS]))
            raise ValueError("test_kwargs must include 'node_id', 'aggregator_id', 'role', and 'participants' keys.")
        if not data_given:
            raise ValueError("test_kwargs must include either 'fhir_data' or 's3_data' key with corresponding data.")

    ########################################General##################################################
    def get_aggregator_id(self) -> str | None:
        """:return: Id of the aggregator node of this analysis."""
        return self.config.aggregator_id

    def get_participants(self) -> list[dict[str, str]]:
        """:return: The other nodes, as ``{"id": ..., "role": ...}`` dicts."""
        return self.config.participants

    def get_participant_ids(self) -> list[str]:
        """:return: Ids of all analysis nodes other than this one."""
        return [v for participant in self.config.participants for k, v in participant.items() if k == "id"]

    def get_analysis_id(self) -> str:
        """:return: Id of the simulated analysis."""
        return self._test_kwargs.get("analysis_id", "analysis_123")

    def get_project_id(self) -> str:
        """:return: Id of the simulated project."""
        return self._test_kwargs.get("project_id", "project_123")

    def get_id(self) -> str:
        """:return: Id of this node."""
        return self.config.node_id

    def get_role(self) -> str:
        """:return: Role of this node ("default", "proxy" or "aggregator")."""
        return self.config.node_role

    def get_self_node_index(self) -> int:
        """
        Returns the index of the executing node id from list containing all analysis node ids sorted alphanumerically.
        :return: the node id index
        """
        return self.get_node_index(self.get_id())

    def get_node_index(self, node_id: str) -> int | None:
        """
        Returns the index of the given node id from list containing all analysis node ids sorted alphanumerically.
        If the given id cannot be found returns None.

        :param node_id: The node id to look up.
        :return: the node id index or None
        """
        id_list = self.get_participant_ids()
        id_list.append(self.get_id())
        if node_id in id_list:
            return sorted(id_list).index(node_id)
        else:
            self.flame_log(
                f"\tSearched node id '{node_id}' not found during indexing attempt",
                log_type=LogTypeLiteral.WARNING.value,
            )
            return None

    def node_has_data(self) -> bool:
        """
        Returns whether the node has access to data via DataAPI.
        Used for distinguishing between analyzer nodes (with data) and proxy nodes (without data).
        """
        return self._test_kwargs.get("has_data", True)

    def analysis_finished(self) -> bool:
        """Tell every other node that the analysis is over, then mark this one.

        :return: Always True, mirroring the real SDK.
        """
        if self.get_participant_ids():
            self.send_message(
                self.get_participant_ids(),
                _ANALYSIS_FINISHED_CATEGORY,
                {},
                max_attempts=5,
                attempt_timeout=30,
            )
            self.config.finished = True
        return True

    def ready_check(
        self,
        nodes: list[str] = "all",
        attempt_interval: int = 30,
        timeout: int | None = None,
    ) -> dict[str, bool]:
        """Pretend every node is reachable - locally, they always are.

        :param nodes: Node ids to check, or "all" for every participant.
        :param attempt_interval: Ignored; kept for signature compatibility.
        :param timeout: Ignored; kept for signature compatibility.
        :return: ``{node id: True}`` for each checked node.
        """
        if nodes == "all":
            nodes = self.get_participants
        return dict.fromkeys(nodes, True)

    def flame_log(
        self,
        msg: str | bytes,
        sep: str = " ",
        end: str = "\n",
        file: object = None,
        log_type: str = LogTypeLiteral.INFO.value,
        append: bool = False,
        halt_submission: bool = False,
        hidden_error_msg: str | None = None,
    ) -> None:
        """Buffer a log line for this node, coloured by log type.

        Nothing is printed here: output is collected per node and flushed by
        :meth:`__pop_logs__`, so that the interleaved threads produce readable,
        per-round output.

        :param msg: The message to log.
        :param sep: Ignored; kept for signature compatibility.
        :param end: Line terminator appended after the message.
        :param file: Ignored; kept for signature compatibility.
        :param log_type: One of the ``LogTypeLiteral`` values; unknown types
            fall back to INFO.
        :param append: Ignored; kept for signature compatibility.
        :param halt_submission: Ignored; kept for signature compatibility.
        :param hidden_error_msg: Detail that stays on the node in production
            (e.g. a stack trace); buffered here behind a "_HIDDEN:" marker.
        """
        if log_type in _LOG_TYPE_LITERALS_COLORS:
            color = str(_LOG_TYPE_LITERALS_COLORS[log_type])
        else:
            color = str(_LOG_TYPE_LITERALS_COLORS[LogTypeLiteral.INFO.value])
        self.logger[self.get_id()][2] += f"\033[{color}m{msg}\033[0m{end}"
        if hidden_error_msg is not None:
            self.logger[self.get_id()][2] += f"\033[{color}m_HIDDEN:{hidden_error_msg}\033[0m{end}"

    def declare_log_types(self, new_log_types: dict[str, str]) -> None:
        """No-op stub: custom log types have no meaning in a local run.

        :param new_log_types: Ignored.
        """

    def get_progress(self) -> int:
        """:return: This node's reported progress, 0-100."""
        return self.progress

    def set_progress(self, progress: int | float) -> None:
        """Record analysis progress, if it actually moved forward.

        Out-of-range or non-increasing values are rejected with a warning
        rather than an exception, as in the real SDK.

        :param progress: Progress between 0 and 100; floats are truncated.
        """
        if isinstance(progress, float):
            progress = int(progress)
        if not (0 <= progress <= 100):
            self.flame_log(
                msg=f"Invalid progress: {progress} (should be a numeric value between 0 and 100).",
                log_type=LogTypeLiteral.WARNING.value,
            )
        elif self.progress >= progress:
            self.flame_log(
                msg="Progress value needs to be higher to current progress (i.e. only register progress, "
                "if actual progress has been made).",
                log_type=LogTypeLiteral.WARNING.value,
            )
        else:
            self.progress = progress
            self.logger[self.get_id()][1] = progress

    def set_checkpoint(self, kwargs: dict[str, Any]) -> None:
        """
        Saves given kwargs into local node storage for future analysis retrieval. raise Warning for incorrect format
        of kwargs

        Checkpoints are numbered by counting the ones already stored, so the
        first call produces ``checkpoint-1``.

        :param kwargs: The state to persist, keyed by attribute name.
        :return:
        """
        if not isinstance(kwargs, dict):
            self.flame_log(
                msg=f"Expected dictionary object for kwargs in checkpoint save but received {type(kwargs)}."
                f" Could not save checkpoint",
                log_type=LogTypeLiteral.WARNING.value,
            )
        elif not any(isinstance(key, str) for key in kwargs):
            self.flame_log(
                msg=f"Expected string object for kwargs keys in checkpoint save but received "
                f"{[type(k) for k in kwargs]}. Could not save checkpoint",
                log_type=LogTypeLiteral.WARNING.value,
            )
        else:
            i = len(self.get_local_tags(CHECKPOINT_TAG_PREFIX)) + 1
            self.flame_log(msg=f"Saved checkpoint no.{i}", log_type=LogTypeLiteral.INFO.value)
            self.save_intermediate_data(data=kwargs, location="local", tag=f"{CHECKPOINT_TAG_PREFIX}{i}")

    def load_checkpoint(self, index: int) -> dict[str, Any] | None:
        """
        Load saved kwargs from previous checkpoint with given index. return None if not found

        :param index: 1-based index of the checkpoint to load.
        :return kwargs: The stored state, or None if it is missing or ambiguous.
        """
        locally_tagged_saves = self.get_local_tags(f"{CHECKPOINT_TAG_PREFIX}{index}")
        if len(locally_tagged_saves) == 1:
            self.flame_log(msg=f"Loading checkpoint no.{index}", log_type=LogTypeLiteral.INFO.value)
            return self.get_intermediate_data(location="local", tag=f"{CHECKPOINT_TAG_PREFIX}{index}")
        elif len(locally_tagged_saves) > 1:
            self.flame_log(
                msg=f"Error: Loading checkpoint no.{index} failed. Multiple saves under same tag found",
                log_type=LogTypeLiteral.ERROR.value,
            )
            return None
        else:
            self.flame_log(
                msg=f"No checkpoint {index} was found. Returning None",
                log_type=LogTypeLiteral.WARNING.value,
            )
            return None

    def fhir_to_csv(
        self,
        fhir_data: dict[str, Any],
        col_key_seq: str,
        value_key_seq: str,
        input_resource: str,
        row_key_seq: str | None = None,
        row_id_filters: list[str] | None = None,
        col_id_filters: list[str] | None = None,
        row_col_name: str = "",
        separator: str = ",",
        output_type: Literal["file", "dict"] = "file",
    ) -> StringIO | dict[Any, dict[Any, Any]] | None:
        """Not emulated locally; always returns None.

        :return: None.
        """
        return None

    ########################################Message Broker Client####################################
    def send_message(
        self,
        receivers: list[str],
        message_category: str,
        message: dict,
        max_attempts: int = 1,
        timeout: int | None = None,
        attempt_timeout: int = 10,
    ) -> tuple[list[str], list[str]]:
        """Drop a message into each receiver's inbox in the shared broker.

        Delivery cannot fail locally, so the retry parameters are accepted but
        unused, and the "failed" half of the return value is always empty.

        :param receivers: Ids of the nodes to deliver to.
        :param message_category: Category the receiver will filter on.
        :param message: The payload.
        :param max_attempts: Ignored; kept for signature compatibility.
        :param timeout: Ignored; kept for signature compatibility.
        :param attempt_timeout: Ignored; kept for signature compatibility.
        :return: Tuple of (delivered to, failed for).
        """
        sender = self.get_id()
        for r in receivers:
            if r not in self.message_broker:
                self.message_broker[r] = []
            inbox = self.message_broker[r]
            inbox.append(MockMessage({"category": message_category, "sender": sender, "data": message}))
            self.message_broker[r] = inbox
        return receivers, []

    def await_messages(
        self,
        senders: list[str],
        message_category: str,
        message_id: str | None = None,
        timeout: int | None = None,
    ) -> dict[str, list[MockMessage] | None]:
        """Block until every sender has left a message of the given category.

        Polls this node's inbox in the shared broker. The wait ends early if the
        aggregator broadcast that the analysis is finished, in which case this
        node is marked finished and ``{aggregator id: None}`` is returned.
        Consumed messages are removed from the inbox; unrelated ones stay.

        :param senders: Ids whose messages are awaited; all must report in.
        :param message_category: Category to wait for.
        :param message_id: Ignored; kept for signature compatibility.
        :param timeout: Ignored; kept for signature compatibility.
        :return: Sender id -> its messages of that category, newest last.
        :raises ValueError: If ``senders`` is not a list of participant ids.
        :raises Exception: If another thread failed and set the stop event.
        """
        if not isinstance(senders, list):
            raise ValueError(
                f"Senders should be provided as a list of participant ids. Not {senders} of type {type(senders)}."
            )
        else:
            for sender in senders:
                if sender not in self.get_participant_ids():
                    raise ValueError(f"Sender {sender} is not a valid participant id for this analysis.")

        node_id = self.get_id()

        while True:
            try:
                inbox = self.message_broker.get(node_id, [])
                if inbox:
                    finished_messages = [msg for msg in inbox if msg.body["category"] == _ANALYSIS_FINISHED_CATEGORY]
                    if finished_messages:
                        self._node_finished()
                        break

                    msg_senders = [msg.body["sender"] for msg in inbox if msg.body["category"] == message_category]
                    if all(sender in msg_senders for sender in senders):
                        break
                # Not everyone has reported in yet -> fall through to the wait.
                raise KeyError
            except KeyError:
                if self.stop_event:
                    # Another node's thread failed: stop waiting for a message
                    # that will never arrive. `from None` keeps the synthetic
                    # KeyError above out of the reported traceback.
                    raise Exception from None
                time.sleep(0.01)

        if not self.config.finished:
            remaining_msgs = []
            latest_results = {}
            for msg in inbox:
                if (msg.body["category"] == message_category) and (msg.body["sender"] in senders):
                    if msg.body["sender"] not in latest_results:
                        latest_results[msg.body["sender"]] = []
                    latest_results[msg.body["sender"]].append(MockMessage(msg.body["data"]))
                else:
                    remaining_msgs.append(msg)

            # retain only unconsumed messages
            self.message_broker[node_id] = remaining_msgs
            return latest_results
        else:
            return {self.config.aggregator_id: None}

    def get_messages(self, status: Literal["unread", "read"] = "unread") -> list[MockMessage]:
        """Return this node's whole inbox without consuming it.

        :param status: Ignored; the local broker does not track read state.
        :return: Every message currently in this node's inbox.
        """
        inbox = self.message_broker.get(self.get_id(), [])
        finished_messages = [msg for msg in inbox if msg.body["category"] == _ANALYSIS_FINISHED_CATEGORY]
        if finished_messages:
            self._node_finished()
        return inbox

    def delete_messages(self, message_ids: list[str]) -> int:
        """No-op stub: the local broker has no per-message deletion.

        :param message_ids: Ignored.
        :return: None.
        """

    def clear_messages(
        self,
        status: Literal["read", "unread", "all"] = "read",
        min_age: int | None = None,
    ) -> int:
        """No-op stub: the local broker is cleared as messages are consumed.

        :param status: Ignored.
        :param min_age: Ignored.
        :return: None.
        """

    def send_message_and_wait_for_responses(
        self,
        receivers: list[str],
        message_category: str,
        message: dict,
        max_attempts: int = 1,
        timeout: int | None = None,
        attempt_timeout: int = 10,
    ) -> dict[str, list[MockMessage] | None]:
        """Send a message to every receiver and block for their replies.

        :param receivers: Ids to send to and then await.
        :param message_category: Category used in both directions.
        :param message: The payload to send.
        :param max_attempts: Ignored; kept for signature compatibility.
        :param timeout: Ignored; kept for signature compatibility.
        :param attempt_timeout: Ignored; kept for signature compatibility.
        :return: Receiver id -> its replies, as :meth:`await_messages` returns.
        """
        self.send_message(receivers, message_category, message, max_attempts, timeout, attempt_timeout)
        return self.await_messages(receivers, message_category, None, timeout)

    ########################################Storage Client###########################################
    def submit_final_result(
        self,
        result: Any,
        output_type: Literal["str", "bytes", "pickle"] | list = "str",
        multiple_results: bool = False,
        local_dp: dict | None = None,
        filename: str | list[str] | None = None,
    ) -> dict[str, str] | list[dict[str, str]]:
        """Store the final result, optionally perturbed by local DP.

        Local DP is only applied to numeric results; anything else is stored
        unchanged with a warning, since the Laplace mechanism has no meaning
        for it. Storing also flushes this node's buffered logs.

        :param result: The aggregator's final result.
        :param output_type: Ignored locally; the result is kept as an object.
        :param multiple_results: Ignored locally.
        :param local_dp: ``{"epsilon": ..., "sensitivity": ...}`` to apply the
            Laplace mechanism, or None to store the result as is.
        :param filename: Ignored locally; see the pattern testers, which write
            the stored result out afterwards.
        :return: ``{"result": "submitted"}``.
        :raises RuntimeError: If a node other than the aggregator calls this.
        """
        if self.get_id() == self.get_aggregator_id():
            if local_dp is not None:
                if type(result) in [int, float]:
                    enable_features("contrib")
                    scale = local_dp["sensitivity"] / local_dp["epsilon"]  # Laplace scale parameter
                    laplace_mech = make_laplace(
                        input_domain=atom_domain(T=float, nan=False),
                        input_metric=absolute_distance(T=float),
                        scale=scale,
                    )
                    result = laplace_mech(float(result))
                else:
                    self.flame_log(
                        "Given result type is not supported for local DP -> DP step will be skipped.",
                        log_type=LogTypeLiteral.WARNING.value,
                    )
            self.final_results_storage = result
            self.flame_log(
                f"Submitted final result: {str(result)[:100]}{'...' if len(str(result)) > 100 else ''}",
                log_type=LogTypeLiteral.INFO.value,
            )
            self.set_progress(100)
            self.__pop_logs__()
            return {"result": "submitted"}
        else:
            raise RuntimeError(
                f"Final results may only be submitted by the aggregator {self.get_aggregator_id()} "
                f"(given node with id={self.get_id()})."
            )

    def save_intermediate_data(
        self,
        data: Any,
        location: Literal["local", "global"],
        remote_node_ids: list[str] | None = None,
        tag: str | None = None,
    ) -> dict[str, dict[str, str]] | dict[str, str] | tuple[list[str], list[str]]:
        """Persist data locally as a pickle, or fall back to sending it.

        Local saves go to ``<local_storage_dir>/<node id>[/<tag>]/<uuid>``.
        Global storage has no local equivalent, so it is downgraded to
        :meth:`send_intermediate_data` with a warning.

        :param data: The object to store.
        :param location: "local" to write to disk, "global" to send instead.
        :param remote_node_ids: Receivers used by the "global" fallback.
        :param tag: Optional subdirectory grouping related saves.
        :return: ``{"status", "url", "id"}`` for a local save, or whatever
            :meth:`send_intermediate_data` returns for a global one.
        """
        filename = str(uuid.uuid4())
        if location == "local":
            storage_dir = self._test_kwargs["local_storage_dir"]
            if storage_dir is None:
                storage_dir = "."
            elif not os.path.exists(storage_dir):
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
            return {"status": "success", "url": save_location, "id": filename}
        else:
            self.flame_log(
                "Warning: Issuing a global save location is not supported during local testing.\n"
                "Opting for sending intermediate data instead.",
                log_type=LogTypeLiteral.WARNING.value,
            )
            return self.send_intermediate_data(receivers=remote_node_ids, data=data)

    def get_intermediate_data(
        self,
        location: Literal["local", "global"],
        id: str | None = None,
        tag: str | None = None,
        tag_option: Literal["all", "last", "first"] | None = "all",
        sender_node_id: str | None = None,
    ) -> Any:
        """Read back data written by :meth:`save_intermediate_data`.

        Within a tag, ``tag_option`` picks which of the stored files to load.
        Global storage has no local equivalent, so it is downgraded to
        :meth:`await_intermediate_data` with a warning, reading ``sender_node_id``
        as the sender and ``tag`` as the message category.

        :param location: "local" to read from disk, "global" to await instead.
        :param id: Specific save id; required when no ``tag`` is given.
        :param tag: Subdirectory the save was grouped under.
        :param tag_option: Which saves under the tag to load: "all", "last" or
            "first". Only used when ``id`` is None.
        :param sender_node_id: Sender used by the "global" fallback.
        :return: The unpickled object, or a list of them for ``tag_option="all"``.
        :raises ValueError: If the store, the tag or the id cannot be resolved.
        """
        if location == "local":
            storage_dir = self._test_kwargs["local_storage_dir"]
            if storage_dir is None:
                storage_dir = "."
            node_store = f"{storage_dir}/{self.get_id()}"
            if not os.path.exists(node_store):
                raise ValueError(
                    f"Could not find local store for current node at specified location "
                    f"(given node_id={self.get_id()}, storage_dir={storage_dir})."
                )
            if tag is not None:
                tagged_node_store = f"{node_store}/{tag}"
                if not os.path.exists(tagged_node_store):
                    raise ValueError(
                        f"Could not find tagged node store for current node at specified location "
                        f"(given tag={tag}, found tags={os.listdir(node_store)})."
                    )
                if id is None:
                    file_paths = os.listdir(tagged_node_store)
                    if len(file_paths) == 0:
                        raise ValueError(
                            f"Could not find any file in tagged node store for current node at {tagged_node_store}"
                        )
                    elif tag_option == "all":
                        save_location = file_paths
                    elif tag_option == "last":
                        save_location = file_paths[-1]
                    elif tag_option == "first":
                        save_location = file_paths[0]
                    else:
                        raise ValueError(
                            f"Invalid value given for tag_option. Must be 'all', 'last' or 'first' "
                            f"(given tag_option={tag_option})."
                        )
                else:
                    save_location = f"{tagged_node_store}/{id}"
            else:
                if id is None:
                    raise ValueError(
                        f"When attempting to retrieve from local store, either id or tag must be "
                        f"specified (given id={id} and tag={tag})."
                    )
                else:
                    save_location = f"{node_store}/{id}"
            if isinstance(save_location, list):
                return [pickle.loads(open(sl, "rb").read()) for sl in save_location]
            else:
                return pickle.loads(open(save_location, "rb").read())
        else:
            self.flame_log(
                "Warning: Issuing a global save location is not supported during local testing.\n"
                "Opting for awaiting intermediate data instead, using given sender_node_id as sender"
                "and given tag as message_category.",
                log_type=LogTypeLiteral.WARNING.value,
            )
            return self.await_intermediate_data(senders=[sender_node_id], message_category=tag)

    def send_intermediate_data(
        self,
        receivers: list[str],
        data: Any,
        message_category: str = "intermediate_data",
        max_attempts: int = 1,
        timeout: int | None = None,
        attempt_timeout: int = 10,
        encrypted: bool = False,
    ) -> tuple[list[str], list[str]]:
        """Send a round's result to the given nodes through the shared broker.

        When the aggregator sends - which marks the end of a round - the
        buffered logs of all nodes are flushed.

        :param receivers: Ids of the nodes to deliver to.
        :param data: The payload to send.
        :param message_category: Category the receiver will filter on.
        :param max_attempts: Ignored; kept for signature compatibility.
        :param timeout: Ignored; kept for signature compatibility.
        :param attempt_timeout: Ignored; kept for signature compatibility.
        :param encrypted: Ignored; nothing leaves the process locally.
        :return: Tuple of (delivered to, failed for).
        """
        receivers, _ = self.send_message(
            receivers=receivers,
            message_category=message_category,
            message={"data": data},
            max_attempts=max_attempts,
            timeout=timeout,
        )
        if self.get_id() == self.get_aggregator_id():
            self.__pop_logs__()
        return receivers, []

    def await_intermediate_data(
        self,
        senders: list[str],
        message_category: str = "intermediate_data",
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """Block until every sender has sent a result, then unwrap the payloads.

        Only the most recent message per sender is returned.

        :param senders: Ids whose results are awaited; all must report in.
        :param message_category: Category to wait for.
        :param timeout: Ignored; kept for signature compatibility.
        :return: Sender id -> that sender's latest result.
        """
        sent_data = {}
        response_dict = self.await_messages(senders=senders, message_category=message_category, timeout=timeout)
        if response_dict is not None:
            for sender, messages in response_dict.items():
                sent_data[sender] = messages[-1].body["data"]
        return sent_data

    def get_local_tags(self, filter: str | None = None) -> list[str]:
        """Not emulated locally; always returns None.

        :param filter: Ignored.
        :return: None.
        """

    ########################################Data Client#######################################
    def get_data_client(self, data_id: str) -> AsyncClient | None:
        """Not emulated locally; always returns None.

        :param data_id: Ignored.
        :return: None.
        """

    def get_data_sources(self) -> list[str] | None:
        """Not emulated locally; always returns None.

        :return: None.
        """

    def get_fhir_data(self, fhir_queries: list[str] | None = None) -> list[dict[str, dict] | dict] | None:
        """Return the FHIR data this node was configured with, filtered by query.

        Each data source is reduced to the entries whose key matches one of the
        queries; sources matching none are dropped entirely.

        :param fhir_queries: Queries to keep. None or an empty list yields None.
        :return: One dict per matching data source, or None.
        :raises ValueError: If this node was not configured with FHIR data.
        """
        if "fhir_data" in self._test_kwargs:
            if (fhir_queries is not None) and (len(fhir_queries) != 0):
                return [
                    {k: v for k, v in ds.items() if k in fhir_queries}
                    for ds in self.data
                    if any(q in ds for q in fhir_queries)
                ]
            else:
                return None
        else:
            raise ValueError("No FHIR data provided in test_kwargs.")

    def get_s3_data(self, s3_keys: list[str] | None = None) -> list[dict[str, str] | str] | None:
        """Return the S3 data this node was configured with, filtered by key.

        Note the asymmetry with :meth:`get_fhir_data`: an *empty* key list here
        means "everything", while None still yields None.

        :param s3_keys: Keys to keep; an empty list returns all data, None
            returns None.
        :return: One dict per matching data source, or None.
        :raises ValueError: If this node was not configured with S3 data.
        """
        if "s3_data" in self._test_kwargs:
            if s3_keys == []:
                return self.data
            if s3_keys is not None:
                return [
                    {k: v for k, v in ds.items() if k in s3_keys} for ds in self.data if any(q in ds for q in s3_keys)
                ]
            else:
                return None
        else:
            raise ValueError("No S3 data provided in test_kwargs.")

    def _node_finished(self) -> bool:
        """Mark this node as finished with the analysis.

        :return: Always True.
        """
        self.config.finished = True
        return self.config.finished

    def __pop_logs__(self, failure_message: bool = False) -> None:
        """Print and clear every node's buffered logs, grouped by round.

        Only the aggregator flushes, and only while the analysis is still
        running, so the output of the concurrently running threads is emitted
        once per round instead of interleaved.

        :param failure_message: Prepend a notice that an exception was raised.
        """
        if (not self.config.finished) and (self.config.node_role == "aggregator"):
            print(f"--- Starting Iteration {self.__get_iteration__()} ---")
            if failure_message:
                self.flame_log("Exception was raised (see Stacktrace)!", log_type=LogTypeLiteral.ERROR.value)
            for k, v in self.logger.items():
                role, progress, log = v
                print(f"Logs for {'Analyzer' if role == 'default' else role.capitalize()} {k} (Progress: {progress}%):")
                self.logger[k] = [role, progress, ""]
                print(log, end="")
            print(f"--- Ending Iteration {self.__get_iteration__()} ---\n")
            self.num_iterations.increment()

    def __get_iteration__(self):
        """:return: The current shared round number."""
        return self.num_iterations.get_iterations()
