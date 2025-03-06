from typing import Any, Callable, Optional, Union
from flame.resources.node_config import NodeConfig


class Pattern_Aggregator:

    last_result: Any = None

    def __init__(self,
                 node_config: Optional[NodeConfig] = None,
                 base_model: Optional[Any] = None,
                 model_params: Optional[dict[str: Union[str, float, int, bool]]] = None,
                 weights: Optional[list[Any]] = None,
                 gradients: Optional[list[list[float]]] = None,
                 aggr_method: Optional[Callable] = None) -> None:
        pass

    def aggregate(self,
                  results_or_modelparams = list[Any]) -> Any:
        return sum(results_or_modelparams) #here: summation example

    def has_converged(self, result: Any) -> bool:
        are_identical = result == self.last_result
        self.last_result = result
        return are_identical #here: identity test with earlier result as convergence criteria