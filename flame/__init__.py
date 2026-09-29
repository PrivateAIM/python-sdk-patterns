"""Reusable federated-analysis patterns built on top of the FLAME Python SDK.

The package bundles ready-made communication topologies ("patterns") so that an
analysis author only has to supply the statistics, and not the orchestration:

* :mod:`flame.star` -- star topology: many analyzer nodes report to a single
  aggregator node. Also provides a local-differential-privacy variant.
* :mod:`flame.proxy` -- proxy topology: analyzer nodes report to intermediate
  proxy nodes, which pre-aggregate before forwarding to the aggregator.
* :mod:`flame.templates` -- minimal skeletons to copy when writing a new
  analysis from scratch.
* :mod:`flame.utils` -- helpers, most notably a mock of the FLAME Core SDK that
  allows a whole federated analysis to be exercised locally in threads.

Sub-packages are intentionally *not* imported here: importing ``flame`` on its
own therefore stays free of any FLAME SDK side effects.
"""
