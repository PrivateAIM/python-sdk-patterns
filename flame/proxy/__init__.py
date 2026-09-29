"""Proxy topology: analyzer nodes -> proxy nodes -> a single aggregator node.

Analyzer nodes never talk to the aggregator directly. Each analyzer is assigned
to exactly one proxy node, the proxy pre-aggregates the results of the analyzers
assigned to it, and only those pre-aggregates reach the aggregator. This hides
individual node contributions from the aggregator.

Subclass :class:`ProxyAnalyzer`, :class:`Proxy` and :class:`ProxyAggregator`,
then hand all three to :class:`ProxyModel`, which runs whichever role the
executing node has been assigned. :class:`ProxyModelTester` runs the whole
topology locally across threads.
"""

from flame.proxy.aggregator_client import Aggregator as ProxyAggregator
from flame.proxy.analyzer_client import Analyzer as ProxyAnalyzer
from flame.proxy.proxy_client import Proxy
from flame.proxy.proxy_model import ProxyModel
from flame.proxy.proxy_model_tester import ProxyModelTester

__all__ = [
    "Proxy",
    "ProxyAggregator",
    "ProxyAnalyzer",
    "ProxyModel",
    "ProxyModelTester",
]
