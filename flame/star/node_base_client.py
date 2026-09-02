from typing import Any, Literal, Optional, Union

from flamesdk import FlameCoreSDK
from flame.utils.mock_flame_core import MockFlameCoreSDK


class Node:
    id: str
    role: Literal["default", "aggregator"]
    finished: bool
    partner_node_ids: list[str]
    flame: Union[FlameCoreSDK, MockFlameCoreSDK]

    latest_result: Optional[Any]
    num_iterations: int

    def __init__(self, flame: Union[FlameCoreSDK, MockFlameCoreSDK]):
        self.flame = flame

        self.id = self.flame.get_id()
        self.role = self.flame.get_role()
        self.finished = False
        self.latest_result = None
        self.partner_node_ids = self.flame.get_participant_ids()
        self.num_iterations: int = 0

    def node_finished(self):
        self.finished = True

    def load_checkpoint(self, checkpoint_index: int) -> None:
        checkpoint_dict = self.flame.load_checkpoint(checkpoint_index)
        for k, v in checkpoint_dict.items():
            setattr(self, k, v)

    def should_checkpoint(self) -> bool:
        return False

    def set_checkpoint(self, checkpoint_filter: Optional[list[str]] = None) -> None:
        protected_fields = ['id', 'role', 'finished', 'partner_node_ids', 'flame']
        self.flame.set_checkpoint({attr: self.__getattribute__(attr) for attr in dir(self)
                                   if (not attr.startswith('__')) and
                                   (not callable(self.__getattribute__(attr))) and
                                   (attr not in protected_fields) and
                                   (True if checkpoint_filter is None else (attr in checkpoint_filter))})
