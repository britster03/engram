// Vector search with optional URI prefix scope and tenant isolation.
CALL db.index.vector.queryNodes('l0_idx', $over_k, $vec) YIELD node, score
WHERE node.tenant_id = $tenant_id
  AND node.status = 'ACTIVE'
  AND NOT EXISTS {
    MATCH (:Node {tenant_id: $tenant_id})-[:SUPERSEDES]->(node)
  }
  AND coalesce(node.retrieval_weight, 1.0) >= $dormant_floor
  AND ($prefix IS NULL OR node.source_uri STARTS WITH $prefix)
RETURN node.source_uri AS source_uri,
       node.l0_abstract AS l0_abstract,
       node.node_type   AS node_type,
       score
ORDER BY score DESC
LIMIT $k
