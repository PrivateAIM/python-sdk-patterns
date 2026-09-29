"""Strategies for assigning analyzer nodes to proxy nodes.

A mapping method is any callable ``(proxies, analyzers) -> {analyzer: proxy}``.
The aggregator calls it once during the ready check and then tells every node
about its counterpart(s). Pass a custom one via ``mapping_method`` on
:class:`flame.proxy.proxy_model.ProxyModel` to change how the load is spread.
"""


def round_robin_analyzer_to_proxy_mapping(proxies: list[str], analyzers: list[str]) -> dict[str, str]:
    """Map each analyzer to a proxy using even round-robin distribution.

    Both id lists are sorted first, so that every node derives the same mapping
    regardless of the order in which the ready-check responses arrived.

    Note that both input lists are sorted in place.

    :param proxies: List of proxy node IDs.
    :param analyzers: List of analyzer node IDs.
    :return: Dictionary mapping each analyzer ID (keys) to its assigned proxy ID
        (values)::

            {analyzer ID 1: proxy ID 1,
             analyzer ID 2: proxy ID 2,
             ...,
             analyzer ID M: proxy ID M,
             analyzer ID M+1: proxy ID 1,   # wraps around
             ...,
             analyzer ID N: proxy ID (N mod M)}
    """
    proxies.sort()
    analyzers.sort()
    mapping = {}
    for idx, analyzer_id in enumerate(analyzers):
        # Round-robin distribution
        proxy_idx = idx % len(proxies)
        mapping[analyzer_id] = proxies[proxy_idx]
    return mapping
