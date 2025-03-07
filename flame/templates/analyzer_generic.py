from typing import Any, Callable, Optional, Union
from flamesdk.resources.node_config import NodeConfig


class Pattern_Analyzer:
    def __init__(self,
                 node_config: Optional[NodeConfig] = None,
                 base_model: Optional[Any] = None,
                 model_params: Optional[dict[str: Union[str, float, int, bool]]] = None,
                 weights: Optional[list[Any]] = None,
                 gradients: Optional[list[list[float]]] = None,
                 analz_method: Optional[Callable] = None) -> None:
        pass

    def analyze_or_train(self,
                         data: Any,
                         aggr_result: Optional[Any] = None) -> Any:
        return sum(data) + aggr_result #here: summation example
