# python-sdk-patterns

Abstractions on top of the Python SDK for Flame.

`flame` packages the orchestration of a federated analysis — role detection,
ready checks, message passing, convergence loops, result submission — so that an
analysis author only writes the statistics. Two topologies are provided, and
both can be run locally in threads before they are ever deployed.

## Installation

```bash
poetry install
```

## The star pattern

Every analyzer node reports directly to a single aggregator node.

```
analyzer ─┐
analyzer ─┼─> aggregator ─> final result
analyzer ─┘
```

Implement two classes and hand them to `StarModel`. The same image runs on every
node; `StarModel` inspects the role the FLAME hub assigned and runs the matching
half.

```python
from flame.star import StarModel, StarAnalyzer, StarAggregator


class MyAnalyzer(StarAnalyzer):
    def analysis_method(self, data, aggregator_results):
        # `data` is one dict per registered data source on this node
        return sum(len(ds) for ds in data)


class MyAggregator(StarAggregator):
    def aggregation_method(self, analysis_results):
        return sum(analysis_results)

    def has_converged(self, result, last_result):
        return result == last_result


if __name__ == "__main__":
    StarModel(
        analyzer=MyAnalyzer,
        aggregator=MyAggregator,
        data_type="s3",
        query=["my-dataset.csv"],
        simple_analysis=True,  # one round; set False to iterate to convergence
    )
```

`simple_analysis=True` runs a single round. With `False`, the aggregator
broadcasts its result back to the analyzers, which receive it as
`aggregator_results` in the next round, until `has_converged` returns `True`.

### Local differential privacy

`StarLocalDPModel` is a drop-in replacement that perturbs the final result with
a Laplace mechanism before submitting it:

```python
from flame.star import StarLocalDPModel

StarLocalDPModel(..., epsilon=1.0, sensitivity=1.0)
```

The mechanism applies only when both parameters are given, the aggregator's
`has_converged` returned `True` for the final round, and the result is numeric.

### Checkpointing

Override `should_checkpoint()` on your analyzer or aggregator to persist the
node's state each round, and pass `load_checkpoint=<index>` to resume:

```python
class MyAnalyzer(StarAnalyzer):
    def should_checkpoint(self):
        return self.num_iterations % 10 == 0
```

Node identity (`id`, `role`, `finished`, `partner_node_ids`, `flame`) is never
checkpointed. Pass `checkpoint_filter=[...]` to narrow what is saved further.

> **Note:** checkpointing is not currently exercisable under the local testers —
> `MockFlameCoreSDK.get_local_tags` is an unimplemented stub returning `None`,
> so a node that actually checkpoints raises `TypeError` in test mode.

## The proxy pattern

Analyzers never talk to the aggregator directly. Each is assigned to one proxy
node, which pre-aggregates before forwarding, so the aggregator only ever sees
group results rather than individual node contributions.

```
analyzer ─┐
          ├─> proxy ─┐
analyzer ─┘          ├─> aggregator ─> final result
analyzer ───> proxy ─┘
```

```python
from flame.proxy import ProxyModel, ProxyAnalyzer, Proxy, ProxyAggregator


class MyAnalyzer(ProxyAnalyzer):
    def analysis_method(self, data, aggregator_results):
        return sum(len(ds) for ds in data)


class MyProxy(Proxy):
    def proxy_aggregation_method(self, analysis_results):
        return sum(analysis_results)


class MyAggregator(ProxyAggregator):
    def aggregation_method(self, proxy_results):
        return sum(proxy_results)

    def has_converged(self, result, last_result):
        return result == last_result


if __name__ == "__main__":
    ProxyModel(
        analyzer=MyAnalyzer,
        proxy=MyProxy,
        aggregator=MyAggregator,
        data_type="s3",
        num_proxy_nodes=2,
    )
```

Roles are resolved at runtime: the hub only distinguishes `default` from
`aggregator`, and a `default` node is a proxy if it has no data access. Nodes
report their own role to the aggregator during the ready check, which then
computes the analyzer-to-proxy assignment and informs everyone. The default
assignment is round-robin; pass `mapping_method=` to change it (see
`flame.proxy.mapping_methods`).

There must be at least as many analyzers as proxies, and exactly
`num_proxy_nodes` proxies must report in, or the aggregator refuses to start.

## Testing locally

`StarModelTester` and `ProxyModelTester` run a full analysis in threads against
`MockFlameCoreSDK`, which emulates the message broker, the data sources and the
result storage in process:

```python
from flame.star import StarModelTester

StarModelTester(
    data_splits=[split_a, split_b, split_c],  # one per analyzer node
    analyzer=MyAnalyzer,
    aggregator=MyAggregator,
    data_type="s3",
    query=[],
    filename="result.txt",  # omit to print the result instead
)
```

### Data shape

Nodes always receive **a list of dicts** — one dict per data source connected to
that node:

* **s3** — dataset name → dataset bytes
* **fhir** — the query used to retrieve the bundle → the bundle as a dict

Local splits should mirror this, or an analysis may pass locally and fail once
deployed. The testers print a warning when a split does not match.

Note the asymmetry when querying the mock: for s3, an empty `query` list returns
everything and `None` returns nothing; for fhir, both return nothing.

## Layout

| Module | Contents |
| --- | --- |
| `flame.star` | Star topology: `StarModel`, `StarLocalDPModel`, `StarAnalyzer`, `StarAggregator`, `StarModelTester` |
| `flame.proxy` | Proxy topology: `ProxyModel`, `ProxyAnalyzer`, `Proxy`, `ProxyAggregator`, `ProxyModelTester`, `mapping_methods` |
| `flame.templates` | Copy-and-adapt skeletons for writing an analysis against the raw SDK |
| `flame.utils` | `MockFlameCoreSDK`, the in-process stand-in used by the testers |

## Development

```bash
ruff check flame       # lint
ruff format flame      # format
pytest test            # tests
pre-commit install     # run both on commit
```

Lint and format settings live under `[tool.ruff]` in `pyproject.toml`.
