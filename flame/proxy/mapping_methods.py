
def round_robin_analyzer_to_proxy_mapping(proxies: list[str], analyzers: list[str]) -> dict[str, str]:
    """Map each analyzer to a proxy using even round-robin distribution.

     Args:
         proxies: List of proxy node IDs.
         analyzers: List of analyzer node IDs.

     Returns:
         Dictionary mapping each analyzer ID (keys) to its assigned proxy ID (values).
            {analyzer ID 1: proxy ID 1,
             analyzer ID 2: proxy ID 1,
             analyzer ID 3: proxy ID 2,
             ...,
             analyzer ID N: proxy ID M}
    """
    proxies.sort()
    analyzers.sort()
    mapping = {}
    for idx, analyzer_id in enumerate(analyzers):
        # Round-robin distribution
        proxy_idx = idx % len(proxies)
        mapping[analyzer_id] = proxies[proxy_idx]
    return mapping
