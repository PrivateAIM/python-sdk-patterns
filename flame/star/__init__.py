"""Star topology: N analyzer nodes reporting to a single aggregator node.

Typical use is to subclass :class:`StarAnalyzer` and :class:`StarAggregator`
with the analysis at hand and hand both classes to :class:`StarModel`, which
figures out from the node's own role which of the two to run.

:class:`StarModelTester` runs the same pair locally across threads, and
:class:`StarLocalDPModel` is a drop-in :class:`StarModel` that perturbs the
final result with local differential privacy before submission.
"""

from flame.star.aggregator_client import Aggregator as StarAggregator
from flame.star.analyzer_client import Analyzer as StarAnalyzer
from flame.star.star_localdp.star_localdp_model import StarLocalDPModel
from flame.star.star_model import StarModel
from flame.star.star_model_tester import StarModelTester

__all__ = [
    "StarAggregator",
    "StarAnalyzer",
    "StarLocalDPModel",
    "StarModel",
    "StarModelTester",
]
