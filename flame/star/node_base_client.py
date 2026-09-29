"""Common state shared by every node taking part in a star analysis."""

from typing import Any, Literal

from flamesdk import FlameCoreSDK

from flame.utils.mock_flame_core import MockFlameCoreSDK

#: Attributes that describe the node itself rather than the analysis, and that
#: must therefore never end up in - or be restored from - a checkpoint.
_PROTECTED_CHECKPOINT_FIELDS = ["id", "role", "finished", "partner_node_ids", "flame"]


class Node:
    """Base class for the analyzer and aggregator ends of a star analysis.

    It holds the bookkeeping both roles need (who am I, who are my partners, am
    I done, what did I produce last) and implements checkpointing on top of the
    SDK, so that a node can resume an interrupted multi-round analysis.

    :ivar id: Id of the node this code is executing on.
    :ivar role: Role assigned to this node by the FLAME hub.
    :ivar finished: Whether this node has left its analysis loop.
    :ivar partner_node_ids: Ids of all other nodes in the analysis.
    :ivar flame: The FLAME Core SDK (or its mock, during local testing).
    :ivar latest_result: Result of the most recent analysis/aggregation round.
    :ivar num_iterations: Number of rounds completed so far.
    """

    id: str
    role: Literal["default", "aggregator"]
    finished: bool
    partner_node_ids: list[str]
    flame: FlameCoreSDK | MockFlameCoreSDK

    latest_result: Any | None
    num_iterations: int

    def __init__(self, flame: FlameCoreSDK | MockFlameCoreSDK):
        """Read the node's own identity and partners off the SDK.

        :param flame: An initialized FLAME Core SDK, or its testing mock.
        """
        self.flame = flame

        self.id = self.flame.get_id()
        self.role = self.flame.get_role()
        self.finished = False
        self.latest_result = None
        self.partner_node_ids = self.flame.get_participant_ids()
        self.num_iterations: int = 0

    def node_finished(self):
        """Mark this node as done, which breaks the enclosing analysis loop."""
        self.finished = True

    def load_checkpoint(self, checkpoint_index: int) -> None:
        """Restore this node's attributes from a previously saved checkpoint.

        Every key/value pair of the checkpoint is written back onto the node, so
        the node continues where the checkpointed run left off.

        :param checkpoint_index: 1-based index of the checkpoint to restore.
        """
        checkpoint_dict = self.flame.load_checkpoint(checkpoint_index)
        for k, v in checkpoint_dict.items():
            setattr(self, k, v)

    def should_checkpoint(self) -> bool:
        """Decide whether the current round should be checkpointed.

        Checkpointing is off by default; override this in a subclass to enable
        it, for instance every n-th round via ``self.num_iterations``.

        :return: True if :meth:`set_checkpoint` should run after this round.
        """
        return False

    def set_checkpoint(self, checkpoint_filter: list[str] | None = None) -> None:
        """Persist this node's analysis state to local node storage.

        Everything that is neither a dunder, nor callable, nor node identity
        (see :data:`_PROTECTED_CHECKPOINT_FIELDS`) is saved.

        :param checkpoint_filter: If given, restrict the checkpoint to these
            attribute names; if None, save every eligible attribute.
        """
        self.flame.set_checkpoint(
            {
                attr: self.__getattribute__(attr)
                for attr in dir(self)
                if (not attr.startswith("__"))
                and (not callable(self.__getattribute__(attr)))
                and (attr not in _PROTECTED_CHECKPOINT_FIELDS)
                and (True if checkpoint_filter is None else (attr in checkpoint_filter))
            }
        )
